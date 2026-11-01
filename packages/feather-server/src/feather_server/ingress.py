"""Webhook ingress: verify, store in the inbox, answer (design 6.3 and 13.5). It never waits on a destination (I5).

`POST /ingress/{tenant}/{source}` is outside the dashboard: it asks for neither a session nor a CSRF token, because the
source's signature is what vouches for a request. That follows from this router not asking for them, since each dashboard
route asks for itself, and no list of exemptions exists for another route to end up on.

The checks go from cheap to dear, so that a caller who can't sign costs little: the tenant and source, the rate, the
declared size, the size as read, the signature, and only then the JSON. A refusal says what the caller may know and nothing
else. Above all, a failed verification answers the same whichever check failed. What stays server-side is logged, and
neither a secret nor a body ever is: a connector's or a database's message can quote the body, so of those only the class
of the error is logged.
"""

import json
import logging
import math
import threading
from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from feather_connect.clock import Clock
from feather_connect.core.connector import (
    InboundRequest,
    PushSource,
    event_id_of,
    is_unsigned,
)
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import PermanentError
from feather_connect.core.webhooks import SignatureInvalid
from feather_connect.logs import redact

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

_TENANT = "default"  # the only tenant until M3
_PUSH = "push"
_ACCEPTED = {"status": "accepted"}
_TITLES = {
    400: "Bad Request",
    401: "Unauthorized",
    404: "Not Found",
    413: "Content Too Large",
    429: "Too Many Requests",
    503: "Service Unavailable",
}
# What a request carries that vouches for its sender. `verify` may need it, and the inbox has no use for it.
_NOT_KEPT = frozenset({"authorization", "cookie", "proxy-authorization"})
_MAX_EVENT_ID = 255

router = APIRouter()


def _problem(status: int, headers: Mapping[str, str] | None = None) -> Response:
    return JSONResponse(
        {"title": _TITLES[status], "status": status},
        status,
        dict(headers or {}),
        media_type="application/problem+json",
    )


class RateLimiter:
    """A token bucket per source, holding a second's worth of requests.

    It lives in this process, as `LoginThrottle` does: with several replicas behind a load balancer each counts for
    itself, so a source can get up to the limit times the number of replicas. It protects a replica, and is not a promise
    to anyone about what a source may send.
    """

    def __init__(self, rate_per_s: float, clock: Clock) -> None:
        self._rate = rate_per_s
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets: dict[str, tuple[float, float]] = {}  # tokens, and the time they were counted

    def wait_for(self, source: str) -> float:
        """Take a token and return 0, or return how many seconds until there is one."""
        now = self._clock.now().timestamp()
        with self._lock:
            tokens, at = self._buckets.get(source, (self._rate, now))
            tokens = min(self._rate, tokens + (now - at) * self._rate)
            if tokens >= 1:
                self._buckets[source] = (tokens - 1, now)
                return 0.0
            self._buckets[source] = (tokens, now)
            return (1 - tokens) / self._rate


def _push_dataset(state: "AppState", tenant: str, source: str) -> Dataset | None:
    """Return the dataset that takes events at this address, or None if there is none."""
    if tenant != _TENANT:
        return None
    try:
        dataset = state.runtime.dataset(source)
    except KeyError:
        return None
    if dataset.strategy.name != _PUSH:
        return None
    flows = [flow for flow in state.runtime.flows() if dataset in flow.datasets]
    return dataset if any(flow.enabled for flow in flows) else None


class _TooLarge(Exception):
    pass


async def _read_body(request: Request, limit: int) -> bytes:
    """Read the body, counting as it comes: a `Content-Length` was only checked, and a chunked body has none."""
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise _TooLarge
        chunks.append(chunk)
    return b"".join(chunks)


def _is_json(body: bytes) -> bool:
    """Whether the body parses as JSON. Sources take JSON until one needs otherwise. Parsing also meets deep nesting."""
    try:
        json.loads(body)
    except (ValueError, RecursionError):
        return False
    return True


def _verified(
    state: "AppState", dataset: Dataset, source: PushSource, request: InboundRequest
) -> Response | None:
    """Run the source's check. Return the refusal, or None if the request is good. Anything unclear is a refusal."""
    name = dataset.name
    if is_unsigned(source):
        return None
    try:
        source.verify(state.runtime.make_context(source.connection), request)
    except SignatureInvalid as error:
        log.warning("ingress %s: refused: %s", name, redact(str(error)))
        return _problem(401)
    except (
        PermanentError
    ) as error:  # the secret isn't there: the sender did nothing wrong, and should try again
        log.error("ingress %s: can't verify: %s", name, redact(str(error)))
        return _problem(503)
    except Exception as error:  # noqa: BLE001 - a verify that fails another way proves nothing: refuse
        log.warning("ingress %s: refused: verify raised %s", name, type(error).__name__)
        return _problem(401)
    return None


def _event_id(dataset: Dataset, source: PushSource, request: InboundRequest) -> str | None:
    """The event's id, if it can be kept. Without one it may be stored twice, which beats not at all."""
    try:
        found = event_id_of(source, request)
    except Exception as error:  # noqa: BLE001 - the message of a connector's error may quote the body
        log.warning("ingress %s: no event id: %s", dataset.name, type(error).__name__)
        return None
    if isinstance(found, str) and found and "\x00" not in found and len(found) <= _MAX_EVENT_ID:  # pyright: ignore[reportUnnecessaryIsInstance]
        return found
    return None


def _accept(state: "AppState", dataset: Dataset, request: InboundRequest) -> Response:
    """Verify and store. Off the event loop, since the check and the store block."""
    source = cast(PushSource, dataset.source)
    refused = _verified(state, dataset, source, request)
    if refused is not None:
        return refused
    if not _is_json(request.body):
        log.warning("ingress %s: refused: the body is not JSON", dataset.name)
        return _problem(400)
    kept = {
        name: value
        for name, value in request.headers.items()
        if name not in _NOT_KEPT and "\x00" not in value
    }
    try:
        state.store.inbox.put(
            dataset.name, replace(request, headers=kept), _event_id(dataset, source, request)
        )
    except Exception as error:  # noqa: BLE001 - the message of a database's error may quote the row
        log.error("ingress %s: could not store an event: %s", dataset.name, type(error).__name__)
        return _problem(503)
    return JSONResponse(_ACCEPTED, 202)


@router.post("/ingress/{tenant}/{source}")
async def ingress(request: Request, tenant: str, source: str) -> Response:
    state: AppState = request.app.state.feather
    dataset = _push_dataset(state, tenant, source)
    if dataset is None:
        return _problem(404)
    wait = cast(RateLimiter, request.app.state.ingress_rate).wait_for(source)
    if wait:
        log.warning("ingress %s: refused: over the rate limit", source)
        return _problem(429, {"Retry-After": str(max(1, math.ceil(wait)))})
    limit = state.settings.ingress_max_bytes
    declared = request.headers.get("content-length")
    if declared is not None:
        if not (declared.isascii() and declared.isdigit()):
            return _problem(400)
        if int(declared) > limit:
            log.warning("ingress %s: refused: %s bytes is over the limit", source, declared)
            return _problem(413)
    try:
        body = await _read_body(request, limit)
    except _TooLarge:
        log.warning("ingress %s: refused: the body went over the limit", source)
        return _problem(413)
    except ClientDisconnect:
        return _problem(400)
    inbound = InboundRequest(
        request.method,
        request.url.path,
        {name.lower(): value for name, value in request.headers.items()},
        body,
        state.clock.now(),
    )
    return await run_in_threadpool(_accept, state, dataset, inbound)


def install(app: FastAPI, state: "AppState") -> None:
    """Add the ingress route to `app`."""
    app.state.ingress_rate = RateLimiter(state.settings.ingress_rate_per_s, state.clock)
    app.include_router(router)

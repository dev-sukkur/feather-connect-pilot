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

import asyncio
import json
import logging
import math
import queue
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, FastAPI, Request, Response
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
_MAX_BATCH = 200
_JOIN_S = 10.0

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


def _refusal(state: "AppState", dataset: Dataset, request: InboundRequest) -> Response | None:
    """Return why this request is refused, or None if it is to be stored."""
    source = cast(PushSource, dataset.source)
    refused = _verified(state, dataset, source, request)
    if refused is not None:
        return refused
    if not _is_json(request.body):
        log.warning("ingress %s: refused: the body is not JSON", dataset.name)
        return _problem(400)
    return None


def _as_stored(dataset: Dataset, request: InboundRequest) -> tuple[str, InboundRequest, str | None]:
    """Return the event as the inbox takes it, without what a request carries to vouch for its sender."""
    kept = {
        name: value
        for name, value in request.headers.items()
        if name not in _NOT_KEPT and "\x00" not in value
    }
    source = cast(PushSource, dataset.source)
    return dataset.name, replace(request, headers=kept), _event_id(dataset, source, request)


@dataclass
class _Arrival:
    """A request waiting for its answer, on the event loop that will give it."""

    dataset: Dataset
    request: InboundRequest
    loop: asyncio.AbstractEventLoop
    answer: asyncio.Future[Response]


def _give_answers(answers: Sequence[tuple[_Arrival, Response]]) -> None:
    for arrival, response in answers:
        if not arrival.answer.done():  # the sender may have gone
            arrival.answer.set_result(response)


class IngressWorker:
    """Verifies and stores requests on one thread, off the event loop since those block, in shared transactions.

    A transaction and two thread switches per event would leave a server at hundreds a second doing little else, so
    this takes what has come in while it was busy, stores that in one transaction and wakes the event loop once for all of
    it. Nothing waits to fill a batch: a lone event goes straight away. If a batch can't be stored, its events are tried one
    at a time, so that one bad event costs only its own sender an error.
    """

    def __init__(self, state: "AppState") -> None:
        self._state = state
        self._queue: queue.SimpleQueue[_Arrival | None] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    async def handle(self, dataset: Dataset, request: InboundRequest) -> Response:
        """Verify and store the request, and return what to answer."""
        with self._lock:  # started by the first event, as most apps get none
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, name="ingress", daemon=True)
                self._thread.start()
        loop = asyncio.get_running_loop()
        arrival = _Arrival(dataset, request, loop, loop.create_future())
        self._queue.put(arrival)
        return await arrival.answer

    def close(self) -> None:
        with self._lock:
            if self._thread is not None:
                self._queue.put(None)
                self._thread.join(_JOIN_S)
                self._thread = None

    def _run(self) -> None:
        closing = False
        while not closing:
            first = self._queue.get()
            if first is None:
                return
            batch = [first]
            while len(batch) < _MAX_BATCH:
                try:
                    more = self._queue.get_nowait()
                except queue.Empty:
                    break
                if more is None:
                    closing = True
                    break
                batch.append(more)
            answers = self._answers(batch)
            for loop in {arrival.loop for arrival in batch}:
                ours = [(arrival, response) for arrival, response in answers if arrival.loop is loop]
                try:
                    loop.call_soon_threadsafe(_give_answers, ours)
                except RuntimeError:  # the loop has closed, and nobody is waiting
                    log.info("ingress: an event loop closed with requests on it")

    def _answers(self, batch: Sequence[_Arrival]) -> list[tuple[_Arrival, Response]]:
        answers: list[tuple[_Arrival, Response]] = []
        to_store: list[_Arrival] = []
        for arrival in batch:
            try:
                refused = _refusal(self._state, arrival.dataset, arrival.request)
            except Exception as error:  # noqa: BLE001 - the message may quote the body
                log.error(
                    "ingress %s: could not check an event: %s", arrival.dataset.name, type(error).__name__
                )
                refused = _problem(503)
            if refused is None:
                to_store.append(arrival)
            else:
                answers.append((arrival, refused))
        if to_store:
            answers += self._stored(to_store)
        return answers

    def _stored(self, arrivals: Sequence[_Arrival]) -> list[tuple[_Arrival, Response]]:
        inbox = self._state.store.inbox
        events = [_as_stored(arrival.dataset, arrival.request) for arrival in arrivals]
        try:
            inbox.put_many(events)
        except Exception:  # noqa: BLE001 - whose fault it is shows below, one at a time
            return [
                self._stored_alone(arrival, event) for arrival, event in zip(arrivals, events, strict=True)
            ]
        return [(arrival, JSONResponse(_ACCEPTED, 202)) for arrival in arrivals]

    def _stored_alone(
        self, arrival: _Arrival, event: tuple[str, InboundRequest, str | None]
    ) -> tuple[_Arrival, Response]:
        try:
            self._state.store.inbox.put(*event)
        except Exception as error:  # noqa: BLE001 - the message of a database's error may quote the row
            log.error("ingress %s: could not store an event: %s", event[0], type(error).__name__)
            return arrival, _problem(503)
        return arrival, JSONResponse(_ACCEPTED, 202)


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
    return await cast(IngressWorker, request.app.state.ingress_worker).handle(dataset, inbound)


def install(app: FastAPI, state: "AppState") -> None:
    """Add the ingress route to `app`."""
    app.state.ingress_rate = RateLimiter(state.settings.ingress_rate_per_s, state.clock)
    app.state.ingress_worker = IngressWorker(state)
    state.closers.append(app.state.ingress_worker.close)
    app.include_router(router)

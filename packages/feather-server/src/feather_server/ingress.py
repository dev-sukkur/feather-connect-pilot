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
import hashlib
import json
import logging
import math
import queue
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
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
from feather_connect.packages.webhooks import Rejected, Webhook, parse_webhooks, pick, verify
from feather_connect.project import ProjectError, resolve_secret
from feather_connect.storage.port import WebhookEvent

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
_RETRY_AFTER = {
    "Retry-After": "1"
}  # the queue drains in seconds, and a database back from a blip is back at once

router = APIRouter()


def _problem(status: int, headers: Mapping[str, str] | None = None) -> Response:
    return JSONResponse(
        {"title": _TITLES[status], "status": status},
        status,
        dict(headers or {}),
        media_type="application/problem+json",
    )


_MAX_BUCKETS = 10_000  # callers a limiter remembers: one bucket per source and address


class RateLimiter:
    """A token bucket per key, holding a second's worth of requests. Ingress keeps two: one per source and caller
    address, taken before verifying, and one per source, taken only by requests that verified (S5). So a caller who
    can't sign uses up its own bucket and never the source's.

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
            if source not in self._buckets and len(self._buckets) >= _MAX_BUCKETS:
                self._forget_full(now)
            tokens, at = self._buckets.get(source, (self._rate, now))
            tokens = min(self._rate, tokens + (now - at) * self._rate)
            if tokens >= 1:
                self._buckets[source] = (tokens - 1, now)
                return 0.0
            self._buckets[source] = (tokens, now)
            return (1 - tokens) / self._rate

    def _forget_full(self, now: float) -> None:
        """Drop the buckets that have refilled, which a caller would get back anyway; if that leaves too many, as a
        flood of new addresses would, drop the oldest half. Called with the lock held."""
        self._buckets = {
            key: (tokens, at)
            for key, (tokens, at) in self._buckets.items()
            if tokens + (now - at) * self._rate < self._rate
        }
        if len(self._buckets) >= _MAX_BUCKETS:
            newest = sorted(self._buckets.items(), key=lambda item: item[1][1])[_MAX_BUCKETS // 2 :]
            self._buckets = dict(newest)


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
        ctx = replace(state.runtime.make_context(source.connection), now=request.received_at)
        source.verify(ctx, request)
    except SignatureInvalid as error:
        log.warning("ingress %s: refused: %s", name, redact(str(error)))
        return _problem(401)
    except (
        PermanentError
    ) as error:  # the secret isn't there: the sender did nothing wrong, and should try again
        log.error("ingress %s: can't verify: %s", name, redact(str(error)))
        return _problem(503, _RETRY_AFTER)
    except Exception as error:  # noqa: BLE001 - a verify that fails another way proves nothing: refuse
        log.warning("ingress %s: refused: verify raised %s", name, type(error).__name__)
        return _problem(401)
    return None


def _replay_id(source: PushSource, request: InboundRequest) -> str | None:
    """An id for a signed request that has no event id: a hash of its signature and timestamp headers and its body
    (S10). A sender that resends the same signed request, having lost the 202, sends the same signature and timestamp,
    so the inbox keeps it once; a new event, or the same one signed anew, gets another id. An unsigned request has
    nothing to tell a replay by."""
    if is_unsigned(source):
        return None
    signing = sorted(
        (name, value)
        for name, value in request.headers.items()
        if "signature" in name or "timestamp" in name or name == "webhook-id"
    )
    if not signing:
        return None
    digest = hashlib.sha256()
    for name, value in signing:
        digest.update(f"{name}:{value}\n".encode())
    digest.update(request.body)
    return "replay:" + digest.hexdigest()


def _event_id(dataset: Dataset, source: PushSource, request: InboundRequest) -> str | None:
    """The event's id, if it can be kept, or else the signed request's own (S10). Without either it may be stored
    twice, which beats not at all."""
    try:
        found = event_id_of(source, request)
    except Exception as error:  # noqa: BLE001 - the message of a connector's error may quote the body
        log.warning("ingress %s: no event id: %s", dataset.name, type(error).__name__)
        found = None
    if isinstance(found, str) and found and "\x00" not in found and len(found) <= _MAX_EVENT_ID:  # pyright: ignore[reportUnnecessaryIsInstance]
        return found
    return _replay_id(source, request)


class _SampledLog:
    """Logs the first refusal of a kind, then one in `every`, so a flood of refusals can't flood the log too."""

    def __init__(self, every: int = 100) -> None:
        self._every = every
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def warning(self, key: str, message: str, *args: object) -> None:
        with self._lock:
            n = self._counts.get(key, 0)
            self._counts[key] = n + 1 if len(self._counts) < 10_000 else 1
        if n % self._every == 0:
            log.warning(message + (f" ({n + 1} so far)" if n else ""), *args)


_REFUSALS = _SampledLog()


def _over_rate(limiter: RateLimiter | None, key: str, name: str) -> Response | None:
    if limiter is None:
        return None
    wait = limiter.wait_for(key)
    if not wait:
        return None
    _REFUSALS.warning(f"rate:{name}", "ingress %s: refused: over the rate limit", name)
    return _problem(429, {"Retry-After": str(max(1, math.ceil(wait)))})


def _refusal(
    state: "AppState", dataset: Dataset, request: InboundRequest, limiter: RateLimiter | None = None
) -> Response | None:
    """Return why this request is refused, or None if it is to be stored. The source's rate counts only requests that
    verified."""
    source = cast(PushSource, dataset.source)
    refused = _verified(state, dataset, source, request)
    if refused is not None:
        return refused
    refused = _over_rate(limiter, f"source:{dataset.name}", dataset.name)
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
    it. Nothing waits to fill a batch: a lone event goes straight away. If a batch can't be stored because of what is in
    it, its events are tried one at a time, so that one bad event costs only its own sender an error.

    Nothing waits without a limit. A request that finds `ingress_queue_max` waiting, or isn't stored within
    `ingress_wait_s`, is answered 503 with Retry-After, and one that is answered so before its turn comes is not stored.
    One being stored as its time runs out may still be, which a 503 allows: a 202 still means stored, and only that.
    When the database is away, a batch is answered 503 whole, without trying each event against it.
    """

    def __init__(self, state: "AppState", limiter: RateLimiter | None = None) -> None:
        self._state = state
        self._limiter = limiter
        self._queue: queue.Queue[_Arrival | None] = queue.Queue(state.settings.ingress_queue_max)
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
        try:
            self._queue.put_nowait(arrival)
        except queue.Full:
            log.warning("ingress %s: refused: too many requests wait to be stored", dataset.name)
            return _problem(503, _RETRY_AFTER)
        try:
            return await asyncio.wait_for(arrival.answer, self._state.settings.ingress_wait_s)
        except TimeoutError:  # the answer is cancelled, which tells the worker to pass it by
            log.warning("ingress %s: refused: not stored in time", dataset.name)
            return _problem(503, _RETRY_AFTER)

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
            batch = [
                arrival for arrival in batch if not arrival.answer.done()
            ]  # what timed out goes unstored
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
                refused = _refusal(self._state, arrival.dataset, arrival.request, self._limiter)
            except Exception as error:  # noqa: BLE001 - the message may quote the body
                log.error(
                    "ingress %s: could not check an event: %s", arrival.dataset.name, type(error).__name__
                )
                refused = _problem(503, _RETRY_AFTER)
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
        except Exception as error:  # noqa: BLE001 - whose fault it is shows below, one at a time
            log.error("ingress: could not store %s events: %s", len(events), type(error).__name__)
            if self._state.store.is_unavailable(error):
                return [(arrival, _problem(503, _RETRY_AFTER)) for arrival in arrivals]
            return self._stored_alone(arrivals, events)
        return [(arrival, JSONResponse(_ACCEPTED, 202)) for arrival in arrivals]

    def _stored_alone(
        self, arrivals: Sequence[_Arrival], events: Sequence[tuple[str, InboundRequest, str | None]]
    ) -> list[tuple[_Arrival, Response]]:
        """Store each event in a transaction of its own, until the database is found to be away."""
        answers: list[tuple[_Arrival, Response]] = []
        away = False
        for arrival, event in zip(arrivals, events, strict=True):
            if away:
                answers.append((arrival, _problem(503, _RETRY_AFTER)))
                continue
            try:
                self._state.store.inbox.put(*event)
            except Exception as error:  # noqa: BLE001 - the message of a database's error may quote the row
                log.error("ingress %s: could not store an event: %s", event[0], type(error).__name__)
                away = self._state.store.is_unavailable(error)
                answers.append((arrival, _problem(503, _RETRY_AFTER)))
            else:
                answers.append((arrival, JSONResponse(_ACCEPTED, 202)))
        return answers


@router.post("/ingress/{tenant}/{source}")
async def ingress(request: Request, tenant: str, source: str) -> Response:
    state: AppState = request.app.state.feather
    dataset = _push_dataset(state, tenant, source)
    if dataset is None:
        return _problem(404)
    address = request.client.host if request.client else "unknown"
    refused = _over_rate(request.app.state.ingress_rate, f"address:{source}:{address}", source)
    if refused is not None:
        return refused
    limit = state.settings.ingress_max_bytes
    declared = request.headers.get("content-length")
    if declared is not None:
        if not (declared.isascii() and declared.isdigit()):
            return _problem(400)
        if (
            len(declared.lstrip("0")) > len(str(limit)) or int(declared) > limit
        ):  # too long for `int` is too big
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


# Webhooks a package declares (flow packages §7.2): verified, deduplicated and kept raw here, parsed on a worker.


def _declared(state: "AppState", package: str, name: str) -> Webhook | None:
    """The webhook `name` of `package`, when a version of it declares it and the flow reading its dataset is active."""
    active = [a for a in state.store.packages.active() if a.package == package]
    for activation in active:
        version = state.store.packages.version(package, activation.version)
        if version is None:
            continue
        try:
            hooks = parse_webhooks(cast(dict[str, Any], version.load_check.get("webhooks") or {}))
        except ValueError:
            continue
        hook = hooks.get(name)
        specs = cast(dict[str, Any], version.load_check.get("specs") or {})
        spec = cast(dict[str, Any], specs.get(activation.flow) or {})
        datasets = {str(d.get("name")) for d in cast(list[dict[str, Any]], spec.get("datasets") or [])}
        if hook is not None and f"{package}/{hook.dataset}" in datasets:
            return hook
    return None


def _keep_declared(
    state: "AppState", package: str, hook: Webhook, headers: dict[str, str], body: bytes
) -> Response:
    now = state.clock.now()
    try:
        secret = resolve_secret(hook.secret)
    except ProjectError as error:
        log.error("webhook %s/%s: its secret can't be had: %s", package, hook.name, error)
        return _problem(503, _RETRY_AFTER)
    try:
        verify(hook, secret, headers, body, now)
        key = pick(body, hook.key)
        stamp = pick(body, hook.stamp) if hook.stamp else None
    except Rejected as error:
        log.warning("webhook %s/%s: refused: %s", package, hook.name, error)
        return _problem(
            401 if "signature" in str(error) or "header" in str(error) or "timestamp" in str(error) else 400
        )
    kept = {k: v for k, v in headers.items() if k not in _NOT_KEPT}
    new = state.store.webhooks.add(
        WebhookEvent(package, hook.name, key[:_MAX_EVENT_ID], stamp, body, kept, now)
    )
    return JSONResponse({"status": "accepted" if new else "duplicate"}, hook.ack)


@router.post("/ingress/{tenant}/{package}/{webhook}")
async def declared_webhook(request: Request, tenant: str, package: str, webhook: str) -> Response:
    state: AppState = request.app.state.feather
    if tenant != _TENANT:
        return _problem(404)
    hook = await run_in_threadpool(_declared, state, package, webhook)
    if hook is None:
        return _problem(404)
    address = request.client.host if request.client else "unknown"
    refused = _over_rate(request.app.state.ingress_rate, f"address:{package}/{webhook}:{address}", webhook)
    if refused is not None:
        return refused
    try:
        body = await _read_body(request, state.settings.ingress_max_bytes)
    except _TooLarge:
        return _problem(413)
    except ClientDisconnect:
        return _problem(400)
    headers = {name.lower(): value for name, value in request.headers.items()}
    return await run_in_threadpool(_keep_declared, state, package, hook, headers, body)


class _NoLineForAnAcceptedEvent(logging.Filter):
    """Leaves out uvicorn's access line for an event that was accepted, which a burst has hundreds of a second, at a cost
    the server feels. A refusal keeps its line. What was accepted can be seen in the inbox."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple) and len(args) == 5:
            return not (args[4] == 202 and str(args[2]).startswith("/ingress/"))
        return True


_ACCESS_FILTER = _NoLineForAnAcceptedEvent()


def install(app: FastAPI, state: "AppState") -> None:
    """Add the ingress route to `app`."""
    app.state.ingress_rate = RateLimiter(state.settings.ingress_rate_per_s, state.clock)
    app.state.ingress_worker = IngressWorker(state, app.state.ingress_rate)
    state.closers.append(app.state.ingress_worker.close)
    logging.getLogger("uvicorn.access").addFilter(_ACCESS_FILTER)
    app.include_router(router)

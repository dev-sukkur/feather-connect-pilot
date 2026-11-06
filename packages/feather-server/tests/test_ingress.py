"""Webhook ingress: what it stores, what it refuses and what stays off it."""

import asyncio
import json
import logging
import threading
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory, simulation
from feather_connect.core.connector import InboundRequest
from feather_connect.core.webhooks import sign
from feather_connect.engine.local import LocalEngine
from feather_connect.project import Project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool
from feather_server.app import AppState, create_app
from feather_server.auth import require_csrf
from feather_server.ingress import _Arrival
from feather_server.settings import Settings

SECRET = "ingress-secret-value"
BODY_MARK = "body-marker-do-not-log"
URL = "/ingress/default/events"


class Harness:
    def __init__(
        self, clock: FrozenClock, *, max_bytes: int = 1024, rate: float = 1000.0, **more: Any
    ) -> None:
        self.clock = clock
        self.store = MemoryStore(clock)
        self.sink = memory.Sink("dst")
        self.source = simulation.Source("hooks", generator={"id": simulation.seq()})
        events = fc.Dataset("events", source=self.source, key="id", strategy=fc.strategy.push())
        pulled = fc.Dataset("pulled", source=memory.Source("pull"), key="id", strategy=fc.strategy.replace())
        unsigned = fc.Dataset(
            "open",
            source=simulation.Source("hooks", generator={"id": simulation.seq()}, unsigned=True),
            key="id",
            strategy=fc.strategy.push(),
        )
        flow = fc.Flow("f", reads=[events, pulled, unsigned], deliver=[fc.to(self.sink)])
        self.runtime = Runtime(
            [flow],
            self.store,
            MemorySpool(clock),
            LocalEngine(clock),
            code_sha="sha",
            clock=clock,
            config_of=lambda connection: {"signature_secret": SECRET} if connection == "hooks" else {},
        )
        project = Project("demo", Path("."), "dev", (flow,), {}, {}, {"timezone": "UTC"}, "sha")
        settings = Settings(
            secret_key="test-secret-key", ingress_max_bytes=max_bytes, ingress_rate_per_s=rate, **more
        )
        self.state = AppState(
            settings, project, self.store, MemorySpool(clock), self.runtime._engine, self.runtime, clock=clock
        )
        self.app = create_app(state=self.state, loops=lambda s: [])
        self.client = TestClient(self.app, base_url="http://localhost:8000", follow_redirects=False)

    def signed(self, body: bytes = b'{"id": 1}', secret: str = SECRET, **extra: str) -> dict[str, str]:
        stamp = str(int(self.clock.now().timestamp()))
        return {"X-Signature": sign(secret, body, stamp), "X-Timestamp": stamp, **extra}

    def post(self, body: bytes = b'{"id": 1}', url: str = URL, **extra: str) -> Any:
        return self.client.post(url, content=body, headers=self.signed(body, **extra))

    def stored(self, dataset: str = "events") -> list[InboundRequest]:
        return [event.request for event in self.store.inbox.take(dataset, 100)]


@pytest.fixture
def h(clock: FrozenClock) -> Harness:
    return Harness(clock)


def _arrival(h: Harness, body: bytes) -> Any:
    headers = {name.lower(): value for name, value in h.signed(body).items()}
    request = InboundRequest("POST", URL, headers, body, h.clock.now())
    return _Arrival(h.runtime.dataset("events"), request, None, None)  # type: ignore[arg-type]


UNAUTHORIZED = {"title": "Unauthorized", "status": 401}


def test_valid_webhook_is_stored_and_returns_202(h: Harness):
    r = h.post(b'{"id": 1}', **{"X-Other": "kept"})
    assert r.status_code == 202
    [request] = h.stored()
    assert request.body == b'{"id": 1}' and request.method == "POST" and request.path == URL
    assert request.headers["x-other"] == "kept" and request.received_at == h.clock.now()


def test_ingress_never_touches_a_sink(h: Harness):  # I5
    assert h.post().status_code == 202
    assert h.sink.calls == [] and h.store.ledger.list_deliveries() == []


def test_bad_signature_is_401_and_stores_nothing(h: Harness):  # Review Focus 2
    stamp = str(int(h.clock.now().timestamp()))
    r = h.client.post(URL, content=b"{}", headers={"X-Signature": "wrong", "X-Timestamp": stamp})
    assert r.status_code == 401 and r.json() == UNAUTHORIZED
    assert r.headers["content-type"] == "application/problem+json"
    assert h.stored() == []


def test_stale_timestamp_is_401_with_the_same_body(h: Harness):  # Review Focus 2
    stamp = str(int((h.clock.now() - timedelta(minutes=6)).timestamp()))
    headers = {"X-Signature": sign(SECRET, b"{}", stamp), "X-Timestamp": stamp}
    r = h.client.post(URL, content=b"{}", headers=headers)
    assert r.status_code == 401 and r.json() == UNAUTHORIZED and h.stored() == []


@pytest.mark.parametrize(
    "headers",
    [{}, {"X-Signature": "abc"}, {"X-Timestamp": "1"}, {"X-Signature": b"\xe9", "X-Timestamp": b"x"}],
)
def test_every_failed_check_answers_alike(h: Harness, headers: dict[str, Any]):
    r = h.client.post(URL, content=b"{}", headers=headers)
    assert r.status_code == 401 and r.json() == UNAUTHORIZED and h.stored() == []


def test_a_verify_that_raises_something_else_refuses_too(h: Harness, monkeypatch: pytest.MonkeyPatch):
    def broken(ctx: Any, request: Any) -> None:
        raise ValueError("a connector's own bug")

    monkeypatch.setattr(h.source, "verify", broken)
    r = h.post()
    assert r.status_code == 401 and r.json() == UNAUTHORIZED and h.stored() == []


def _drive(app: Any, headers: list[tuple[bytes, bytes]], chunks: list[bytes]) -> tuple[int, int]:
    """Call the app as a server does. Returns the status and how many chunks it asked for."""
    asked = 0
    sent: list[int] = []

    async def receive() -> dict[str, Any]:
        nonlocal asked
        asked += 1
        if asked > len(chunks):
            return {"type": "http.disconnect"}
        return {"type": "http.request", "body": chunks[asked - 1], "more_body": asked < len(chunks)}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            sent.append(message["status"])

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": URL,
        "raw_path": URL.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("1.2.3.4", 1000),
        "server": ("localhost", 8000),
    }
    asyncio.run(app(scope, receive, send))
    return sent[0], asked


def test_oversized_body_is_413_without_reading_it(h: Harness):  # Review Focus 3
    status, asked = _drive(h.app, [(b"content-length", b"1025")], [b"x" * 1025])
    assert status == 413 and asked == 0 and h.stored() == []


def test_chunked_body_over_the_limit_is_413(h: Harness):
    chunks = [b"x" * 600] * 5
    status, asked = _drive(h.app, [], chunks)
    assert status == 413 and asked == 2  # it stops at the chunk that goes over
    assert h.stored() == []


def test_a_lying_content_length_is_still_counted(h: Harness):
    status, _ = _drive(h.app, [(b"content-length", b"5")], [b"x" * 2000])
    assert status == 413 and h.stored() == []


def test_a_body_at_the_limit_is_taken(h: Harness):
    body = b'{"p": "' + b"x" * (1024 - 9) + b'"}'
    assert len(body) == 1024
    assert h.post(body).status_code == 202


@pytest.mark.parametrize("value", [b"lots", b"-1"])
def test_a_bad_content_length_is_400(h: Harness, value: bytes):
    status, asked = _drive(h.app, [(b"content-length", value)], [b"{}"])
    assert status == 400 and asked == 0 and h.stored() == []


def test_repeated_event_id_is_stored_once_and_returns_202(h: Harness):  # Review Focus 5
    body = b'{"id": 1, "event_id": "e1"}'
    assert h.post(body).status_code == 202
    assert h.post(body).status_code == 202
    assert len(h.stored()) == 1


@pytest.mark.parametrize("event_id", ["", "with\\u0000nul", 7, None, "x" * 300])
def test_an_event_id_that_cannot_be_kept_counts_as_none(h: Harness, event_id: Any):
    raw = (
        "null" if event_id is None else json.dumps(event_id) if isinstance(event_id, int) else f'"{event_id}"'
    )
    body = ('{"id": 1, "event_id": ' + raw + "}").encode()
    assert h.post(body).status_code == 202
    assert h.post(body).status_code == 202
    assert len(h.stored()) == 2


@pytest.mark.parametrize("body", [b"", b"not json", b'{"id": ', b"\xff\xfe\x00", b"[" * 100_000])
def test_bad_json_is_400_and_stores_nothing(clock: FrozenClock, body: bytes):
    h = Harness(clock, max_bytes=1024 * 1024)
    r = h.post(body)
    assert r.status_code == 400 and r.json() == {"title": "Bad Request", "status": 400} and h.stored() == []


def test_bad_json_with_a_bad_signature_says_only_unauthorized(h: Harness):
    r = h.client.post(URL, content=b"not json", headers={"X-Signature": "wrong", "X-Timestamp": "1"})
    assert r.status_code == 401


def test_unknown_source_is_404(h: Harness):
    r = h.post(url="/ingress/default/nope")
    assert r.status_code == 404 and r.json() == {"title": "Not Found", "status": 404}


def test_non_push_dataset_is_404(h: Harness):
    assert h.post(url="/ingress/default/pulled").status_code == 404
    assert h.stored("pulled") == []


@pytest.mark.parametrize("tenant", ["other", "DEFAULT", "default%00", "..%2fdefault"])
def test_a_malformed_tenant_is_404_and_stores_nothing(h: Harness, tenant: str):
    r = h.post(url=f"/ingress/{tenant}/events")
    assert r.status_code == 404 and h.stored() == []


def test_a_disabled_flow_takes_no_events(h: Harness):
    h.runtime.flow("f").enabled = False
    assert h.post().status_code == 404 and h.stored() == []


def test_rate_limit_returns_429_with_retry_after(clock: FrozenClock):
    h = Harness(clock, rate=2.0)
    assert [h.post().status_code for _ in range(3)] == [202, 202, 429]
    r = h.post()
    assert r.status_code == 429 and r.headers["Retry-After"] == "1"
    assert r.json() == {"title": "Too Many Requests", "status": 429}
    assert len(h.stored()) == 2
    clock.advance(timedelta(seconds=1))
    assert h.post().status_code == 202


def test_the_rate_limit_is_per_source(clock: FrozenClock):
    h = Harness(clock, rate=1.0)
    assert h.post().status_code == 202
    assert h.post().status_code == 429
    assert h.post(url="/ingress/default/open").status_code == 202


def test_an_unsigned_source_needs_no_signature(h: Harness):
    url = "/ingress/default/open"
    assert h.client.post(url, content=b'{"id": 1}').status_code == 202
    assert len(h.stored("open")) == 1


def test_a_signature_secret_that_is_not_set_is_503_and_stores_nothing(h: Harness):
    h.runtime = Runtime(
        h.runtime.flows(), h.store, h.state.spool, h.state.engine, clock=h.clock, config_of=lambda c: {}
    )
    h.state.runtime = h.runtime
    r = h.post()
    assert r.status_code == 503 and r.json() == {"title": "Service Unavailable", "status": 503}
    assert h.stored() == []


def test_a_store_that_fails_is_503_and_logs_no_body(
    h: Harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    def down(*args: Any) -> bool:
        raise RuntimeError(f"failing row {BODY_MARK}")

    monkeypatch.setattr(h.store.inbox, "put", down)
    monkeypatch.setattr(h.store.inbox, "put_many", down)
    with caplog.at_level(logging.DEBUG):
        r = h.post(json.dumps({"id": BODY_MARK}).encode())
    assert r.status_code == 503 and BODY_MARK not in r.text and BODY_MARK not in caplog.text


def test_a_batch_that_fails_costs_only_the_event_that_fails_it(h: Harness, monkeypatch: pytest.MonkeyPatch):
    put = h.store.inbox.put

    def batch_down(events: Any) -> list[bool]:
        raise RuntimeError("the batch")

    def poisoned(dataset: str, request: Any, event_id: Any = None) -> bool:
        if b"poison" in request.body:
            raise RuntimeError("the row")
        return put(dataset, request, event_id)

    monkeypatch.setattr(h.store.inbox, "put_many", batch_down)
    monkeypatch.setattr(h.store.inbox, "put", poisoned)
    worker = h.app.state.ingress_worker
    answers = worker._answers(  # in one batch, which requests sent one by one would not make
        [
            _arrival(h, json.dumps({"id": 1}).encode()),
            _arrival(h, json.dumps({"id": "poison"}).encode()),
            _arrival(h, json.dumps({"id": 3}).encode()),
        ]
    )
    assert [response.status_code for _, response in answers] == [202, 503, 202]


@pytest.mark.parametrize(
    ("path", "status", "kept"),
    [
        ("/ingress/default/events", 202, False),
        ("/ingress/default/events", 401, True),
        ("/runs", 200, True),
    ],
)
def test_the_access_log_leaves_out_only_accepted_events(h: Harness, path: str, status: int, kept: bool):
    record = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        "",
        0,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:1", "POST", path, "1.1", status),
        None,
    )
    assert bool(logging.getLogger("uvicorn.access").filter(record)) is kept


def test_no_secret_or_body_is_logged_or_answered(h: Harness, caplog: pytest.LogCaptureFixture):
    body = json.dumps({"id": BODY_MARK}).encode()
    with caplog.at_level(logging.DEBUG):
        responses = [
            h.post(body),
            h.client.post(URL, content=body, headers={"X-Signature": "wrong", "X-Timestamp": "1"}),
            h.client.post(URL, content=BODY_MARK.encode(), headers=h.signed(BODY_MARK.encode())),
        ]
    assert [r.status_code for r in responses] == [202, 401, 400]
    assert "refused" in caplog.text  # the reason is logged, server-side
    for text in (caplog.text, *(r.text for r in responses)):
        assert SECRET not in text and BODY_MARK not in text


def test_credentials_of_the_request_are_not_kept(h: Harness):
    extra = {"Authorization": "Bearer abc", "Cookie": "feather_session=abc", "X-Kept": "1"}
    r = h.client.post(URL, content=b"{}", headers=h.signed(b"{}", **extra))
    assert r.status_code == 202
    [request] = h.stored()
    assert request.headers["x-kept"] == "1"
    assert "authorization" not in request.headers and "cookie" not in request.headers


# What stays off ingress, and what ingress leaves as it was.


def test_ingress_needs_neither_a_session_nor_a_csrf_token(h: Harness):
    fresh = TestClient(h.app, base_url="http://localhost:8000", follow_redirects=False)
    body = b'{"id": 1}'
    assert fresh.post(URL, content=body, headers=h.signed(body)).status_code == 202
    assert "set-cookie" not in {
        key.lower() for key in fresh.post(URL, content=body, headers=h.signed(body)).headers
    }


def test_a_session_cookie_gives_ingress_nothing(h: Harness):
    h.client.cookies.set("feather_session", "whatever")
    r = h.client.post(URL, content=b"{}", headers={"X-Signature": "wrong", "X-Timestamp": "1"})
    assert r.status_code == 401 and h.stored() == []


def test_every_other_state_changing_route_still_demands_csrf(h: Harness):
    """Ingress is exempt because it never asks for CSRF, not because a list says so: no other route is."""

    def walk(routes: list[Any]) -> Iterator[Any]:
        for route in routes:
            if hasattr(route, "original_router"):  # how FastAPI holds an included router
                yield from walk(route.original_router.routes)
            else:
                yield route

    changing = [
        route
        for route in walk(h.app.routes)
        if isinstance(route, APIRoute) and set(route.methods or ()) & {"POST", "PUT", "PATCH", "DELETE"}
    ]
    assert len(changing) > 10  # the walk finds the dashboard's routes
    bare = [
        route.path
        for route in changing
        if require_csrf not in {dependency.call for dependency in route.dependant.dependencies}
    ]
    assert bare == ["/ingress/{tenant}/{source}"]


def test_a_dashboard_post_without_a_session_is_still_refused(h: Harness):
    fresh = TestClient(h.app, base_url="http://localhost:8000", follow_redirects=False)
    assert fresh.post("/flows/f/run").status_code == 403
    assert fresh.get("/").status_code == 303
    assert fresh.get(URL).status_code == 405


# Ingress under a database that is slow or away: a bound, a timeout, and 503 with Retry-After (S4).


class Stalled:
    """Makes `put_many` wait until let go, and says when one has started waiting."""

    def __init__(self, h: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
        self.entered, self.released = threading.Event(), threading.Event()
        real = h.store.inbox.put_many

        def stalled(events: Any) -> list[bool]:
            self.entered.set()
            self.released.wait(10)
            return real(events)

        monkeypatch.setattr(h.store.inbox, "put_many", stalled)


def handle_all(h: Harness, bodies: list[bytes], after_first: Any = None) -> list[Any]:
    """Hand the requests to the worker as concurrent requests would, the first alone until it is being stored."""
    worker = h.app.state.ingress_worker
    dataset = h.runtime.dataset("events")

    async def main() -> list[Any]:
        requests = [_arrival(h, body).request for body in bodies]
        tasks = [asyncio.create_task(worker.handle(dataset, requests[0]))]
        if after_first is not None:
            await asyncio.to_thread(after_first)
        tasks += [asyncio.create_task(worker.handle(dataset, r)) for r in requests[1:]]
        return list(await asyncio.gather(*tasks))

    return asyncio.run(main())


def test_a_full_ingress_queue_answers_503_at_once(clock: FrozenClock, monkeypatch: pytest.MonkeyPatch):
    h = Harness(clock, ingress_queue_max=2)
    stalled = Stalled(h, monkeypatch)

    def then_let_go() -> None:
        stalled.entered.wait(10)
        threading.Timer(0.5, stalled.released.set).start()

    answers = handle_all(h, [json.dumps({"id": n}).encode() for n in range(4)], then_let_go)
    h.app.state.ingress_worker.close()
    assert [a.status_code for a in answers] == [202, 202, 202, 503]
    assert answers[3].headers["Retry-After"] == "1"
    assert len(h.stored()) == 3


def test_a_store_that_hangs_is_503_by_the_timeout_and_what_waited_is_not_stored(
    clock: FrozenClock, monkeypatch: pytest.MonkeyPatch
):
    h = Harness(clock, ingress_wait_s=0.3)
    stalled = Stalled(h, monkeypatch)
    started = time.monotonic()
    answers = handle_all(h, [b'{"id": 1}', b'{"id": 2}'], lambda: stalled.entered.wait(10))
    assert time.monotonic() - started < 5
    assert [a.status_code for a in answers] == [503, 503]
    assert all(a.headers["Retry-After"] for a in answers)
    stalled.released.set()
    worker = h.app.state.ingress_worker
    worker.close()
    assert [r.body for r in h.stored()] == [b'{"id": 1}']  # it was being stored; the second had waited


def test_a_store_that_is_away_answers_the_batch_503_without_trying_each(
    h: Harness, monkeypatch: pytest.MonkeyPatch
):
    tried: list[Any] = []

    def away(*args: Any) -> Any:
        tried.append(args)
        raise ConnectionError("the database went away")

    monkeypatch.setattr(h.store.inbox, "put_many", away)
    monkeypatch.setattr(h.store.inbox, "put", away)
    answers = h.app.state.ingress_worker._answers([_arrival(h, b'{"id": 1}'), _arrival(h, b'{"id": 2}')])
    assert [response.status_code for _, response in answers] == [503, 503]
    assert all(response.headers["Retry-After"] for _, response in answers)
    assert len(tried) == 1  # the batch, and not each event again


def test_verify_checks_the_timestamp_against_when_the_request_arrived(h: Harness):
    arrival = _arrival(h, b'{"id": 1}')  # signed and received now
    h.clock.advance(timedelta(minutes=6))  # and verified after a wait longer than the tolerance
    [(_, response)] = h.app.state.ingress_worker._answers([arrival])
    assert response.status_code == 202


def test_a_content_length_too_long_to_be_a_number_is_413(h: Harness):
    status, _ = _drive(h.app, [(b"content-length", b"9" * 5000)], [b"{}"])
    assert status == 413

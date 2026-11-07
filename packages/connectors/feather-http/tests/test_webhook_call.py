"""`http.Webhook`: signatures, a custom scheme, records, deletes and event ids, and a webhook delivered to `http.Call`
end to end on the runtime, the local engine, the memory store and the sealer."""

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import (
    Deleted,
    InboundRequest,
    event_id_of,
    is_unsigned,
    signature_secret_of,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.webhooks import SignatureInvalid, sign
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.sealer import seal_once
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool
from feather_connect.testing.readme import run_readme_examples

SECRET = "whsec-0d1e2f3a4b5c6d7e"
ARRIVED = datetime(2026, 11, 7, 9, 0, tzinfo=UTC)
ERP = "https://erp.example.test"


def context(**config: Any) -> Context:
    # A clock far from `ARRIVED`: a signature's timestamp is checked against when the request arrived (S4).
    late = ARRIVED + timedelta(hours=3)
    return Context({"signature_secret": SECRET, **config}, logging.getLogger("webhook-test"), late, "UTC")


def signed(
    body: Any, *, at: datetime = ARRIVED, secret: str = SECRET, headers: dict[str, str] | None = None
) -> InboundRequest:
    raw = json.dumps(body).encode()
    stamp = str(int(at.timestamp()))
    sent = {"x-signature": "sha256=" + sign(secret, raw, stamp), "x-timestamp": stamp, **(headers or {})}
    return InboundRequest("POST", "/hooks/orders", sent, raw, ARRIVED)


def hook(**options: Any) -> Any:
    return http.Webhook("partner", signature_secret="signature_secret", **options)


def test_valid_signature_passes_and_bad_raises():
    source = hook()
    source.verify(context(), signed({"id": 1}))  # verified hours later, against when it arrived
    with pytest.raises(SignatureInvalid, match="does not match"):
        source.verify(context(), signed({"id": 1}, secret="another-secret-value"))
    with pytest.raises(SignatureInvalid, match="tolerance"):
        source.verify(context(), signed({"id": 1}, at=ARRIVED - timedelta(minutes=10)))
    bare = signed({"id": 1})
    with pytest.raises(SignatureInvalid, match="timestamp is missing"):
        source.verify(
            context(),
            InboundRequest("POST", "/", {"x-signature": bare.headers["x-signature"]}, bare.body, ARRIVED),
        )
    with pytest.raises(PermanentError, match="no secret"):  # the ingress answers 503: the sender tries again
        source.verify(Context({}, logging.getLogger("t"), ARRIVED, "UTC"), signed({"id": 1}))


def test_a_scheme_without_a_timestamp():
    source = hook(timestamp_header=None, signature_header="X-Hub-Signature-256")
    raw = b'{"id": 1}'
    request = InboundRequest(
        "POST", "/", {"x-hub-signature-256": "sha256=" + sign(SECRET, raw, None)}, raw, ARRIVED
    )
    source.verify(context(), request)


def test_custom_verify_scheme_is_used():
    seen: list[tuple[str, datetime]] = []

    def token_scheme(secret: str, request: InboundRequest, arrived: datetime) -> None:
        seen.append((secret, arrived))
        if request.headers.get("x-token") != secret:
            raise SignatureInvalid("the token does not match")

    source = hook(verify=token_scheme)
    source.verify(context(), InboundRequest("POST", "/", {"x-token": SECRET}, b"{}", ARRIVED))
    with pytest.raises(SignatureInvalid):
        source.verify(context(), InboundRequest("POST", "/", {"x-token": "nope"}, b"{}", ARRIVED))
    assert seen == [(SECRET, ARRIVED), (SECRET, ARRIVED)]


def test_signed_or_unsigned_is_what_the_loader_reads():
    assert signature_secret_of(hook()) == "signature_secret" and not is_unsigned(hook())
    anyone = http.Webhook("partner", unsigned=True)
    assert is_unsigned(anyone) and signature_secret_of(anyone) is None
    anyone.verify(context(), InboundRequest("POST", "/", {}, b"{}", ARRIVED))
    with pytest.raises(ValueError, match="not both"):
        http.Webhook("partner", signature_secret="signature_secret", unsigned=True)


def test_receive_splits_a_batch_payload_with_records_path():
    source = hook(records_path="data.orders", deleted_field="deleted")
    body = {"data": {"orders": [{"order_id": 1}, {"order_id": 2, "deleted": True}]}}
    assert source.receive(context(), signed(body)) == [
        {"order_id": 1},
        Deleted({"order_id": 2, "deleted": True}),
    ]
    assert hook().receive(context(), signed({"order_id": 3})) == [{"order_id": 3}]
    with pytest.raises(PermanentError, match="not a JSON object") as raised:
        hook().receive(context(), signed([{"order_id": 1}, "secret-looking text"]))
    assert "secret-looking" not in str(raised.value)  # the body stays out of the message
    with pytest.raises(PermanentError, match="has no data.orders"):
        source.receive(context(), signed({"other": 1}))


def test_event_id_comes_from_the_body_or_header():
    by_body = hook(event_id_path="event.id")
    assert event_id_of(by_body, signed({"event": {"id": "evt_1"}})) == "evt_1"
    assert event_id_of(by_body, signed({"event": {"id": 42}})) == "42"
    assert event_id_of(by_body, signed({"event": {}})) is None
    by_header = hook(event_id_header="X-Event-Id", event_id_path="event.id")
    assert (
        event_id_of(by_header, signed({"event": {"id": "body"}}, headers={"x-event-id": "header"}))
        == "header"
    )
    assert event_id_of(by_header, signed({"event": {"id": "body"}})) == "body"  # no header: the body's
    assert event_id_of(hook(), signed({"id": 1})) is None


@respx.mock
def test_webhook_to_call_end_to_end_under_the_harness():
    put = respx.put(url__startswith=f"{ERP}/orders/").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    clock = FrozenClock(ARRIVED)
    store, spool = MemoryStore(clock), MemorySpool(clock)
    source = hook(records_path="orders", event_id_header="x-event-id")
    orders = fc.Dataset("orders", source=source, key="order_id", strategy=fc.strategy.push())
    call = http.Call("erp", method="PUT", url="/orders/{order_id}")
    flow = fc.Flow("orders-to-erp", reads=orders, deliver=[fc.to(call)], triggers=[fc.on_event(orders)])
    configs = {"partner": {"signature_secret": SECRET}, "erp": {"base_url": ERP}}

    def make_context(connection: str) -> Context:
        return Context(configs[connection], logging.getLogger("harness"), clock.now(), "UTC")

    engine = LocalEngine(clock)
    runtime = Runtime([flow], store, spool, engine, clock=clock, make_context=make_context)
    engine.register(runtime.tasks())
    request = signed(
        {"orders": [{"order_id": "A-1", "total": 5}, {"order_id": "A/2", "total": 7}]},
        headers={"x-event-id": "e1"},
    )
    source.verify(runtime.make_context("partner"), request)  # what the ingress does before it stores
    assert store.inbox.put("orders", request, event_id_of(source, request))
    assert not store.inbox.put("orders", request, event_id_of(source, request))  # a repeat is stored once
    clock.advance(timedelta(seconds=2))
    assert len(seal_once(runtime, now=clock.now())) == 1
    engine.run_until_idle()
    sent = sorted((str(c.request.url), json.loads(c.request.content)["total"]) for c in put.calls)
    assert sent == [(f"{ERP}/orders/A%2F2", 7), (f"{ERP}/orders/A-1", 5)]


def test_readme_example_runs(tmp_path: Path):
    names = run_readme_examples(Path(__file__).resolve().parents[1] / "README.md", tmp_path)
    assert isinstance(names["partner_orders"].source, http.Webhook)

"""`http.Call`: templates of the URL and the body, the idempotency key, what each answer means, and `response_map`."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ApplyBatch, ApplyRecord
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.testing.readme import run_readme_examples

BASE = "https://api.example.test"
CHUNK_KEY = "orders-to-erp:v1:0:0"


def batch(*records: ApplyRecord) -> ApplyBatch:
    return ApplyBatch("orders-to-erp", "v1", 0, "*", "upsert", records, CHUNK_KEY, True, True)


def upsert(key: str, **payload: Any) -> ApplyRecord:
    return ApplyRecord(key, "upsert", payload)


def no_retry(seconds: float) -> None:
    pass


@respx.mock
def test_call_renders_url_and_body_from_the_record(make_ctx):
    route = respx.put(url__regex=rf"{BASE}/customers/\w+/orders/\d+").mock(return_value=httpx.Response(200))
    sink = http.Call(
        "erp",
        method="put",
        url="/customers/{customer}/orders/{order_id}",
        body={"ref": "order {order_id}", "total": "{total}", "lines": ["{sku}"], "source": "shop"},
        headers={"X-Source": "feather"},
    )
    by_function = http.Call(
        "erp", method="POST", url=lambda r: f"/orders/{r['order_id']}", body=lambda r: {"n": 1}
    )
    respx.post(f"{BASE}/orders/8").mock(return_value=httpx.Response(201))
    result = sink.apply(
        make_ctx(), batch(upsert("7", customer="c1", order_id=7, total=Decimal("9.50"), sku="A"))
    )
    assert result.applied == 1 and not result.rejected
    sent = route.calls.last.request
    assert sent.url.path == "/customers/c1/orders/7" and sent.headers["x-source"] == "feather"
    assert json.loads(sent.content) == {"ref": "order 7", "total": "9.50", "lines": ["A"], "source": "shop"}
    assert (
        sink.apply(make_ctx(), batch(upsert("8", order_id=8))).applied == 0
    )  # no customer: the record is rejected
    assert by_function.apply(make_ctx(), batch(upsert("8", order_id=8))).applied == 1
    plain = http.Call("erp", method="POST", url="/orders/{order_id}")  # no body: the record goes as it is
    plain.apply(make_ctx(), batch(upsert("8", order_id=8, note="x")))
    assert json.loads(respx.calls.last.request.content) == {"order_id": 8, "note": "x"}


@respx.mock
def test_call_sends_the_idempotency_key(make_ctx):
    route = respx.post(f"{BASE}/orders").mock(return_value=httpx.Response(201))
    sink = http.Call("erp", method="POST", url="/orders")
    sink.apply(make_ctx(), batch(upsert("1", n=1), upsert("2", n=2)))
    sink.apply(make_ctx(), batch(upsert("1", n=1)))  # the same chunk sent again
    keys = [call.request.headers["idempotency-key"] for call in route.calls]
    assert keys[0] == hashlib.sha256(f"{CHUNK_KEY}:1".encode()).hexdigest()
    assert keys[0] != keys[1] and keys[2] == keys[0]
    other = http.Call("erp", method="POST", url="/orders", key_header="X-Request-Id")
    other.apply(make_ctx(), batch(upsert("1", n=1)))
    assert route.calls.last.request.headers["x-request-id"] == keys[0]


@respx.mock
def test_call_names_the_version_it_sends(make_ctx):
    """`X-Feather-Version` goes beside the idempotency key, so that the destination's logs lead back to a trace."""
    route = respx.post(f"{BASE}/orders").mock(return_value=httpx.Response(201))
    http.Call("erp", method="POST", url="/orders").apply(make_ctx(), batch(upsert("1", n=1)))
    assert route.calls.last.request.headers["x-feather-version"] == "v1"


@respx.mock
def test_call_classifies_responses(make_ctx):
    answers = {"ok": 201, "again": 409, "bad": 422, "gone": 400}
    respx.post(url__regex=rf"{BASE}/orders/\w+").mock(
        side_effect=lambda request: httpx.Response(
            answers[request.url.path.rsplit("/", 1)[1]], json={"e": "x"}
        )
    )
    sink = http.Call("erp", method="POST", url="/orders/{name}")
    result = sink.apply(make_ctx(), batch(*(upsert(name, name=name) for name in answers)))
    assert result.applied == 2  # 2xx, and 409 as applied already
    assert [(r.key, r.error_class) for r in result.rejected] == [
        ("bad", "HttpStatusError"),
        ("gone", "HttpStatusError"),
    ]
    assert "HTTP 422" in result.rejected[0].message
    kept = {r.key: (r.status, r.sent) for r in result.responses}
    assert kept == {
        "ok": (201, None),
        "again": (409, None),
        "bad": (422, {"name": "bad"}),
        "gone": (400, {"name": "gone"}),
    }

    for status in (500, 503, 429):
        respx.post(f"{BASE}/flaky").mock(return_value=httpx.Response(status))
        flaky = http.Call("erp", method="POST", url="/flaky")
        flaky.sleep = no_retry
        with pytest.raises(TransientError, match=f"HTTP {status}"):
            flaky.apply(make_ctx(), batch(upsert("1", n=1)))
    respx.post(f"{BASE}/slow").mock(side_effect=httpx.ReadTimeout("slow"))
    slow = http.Call("erp", method="POST", url="/slow")
    slow.sleep = no_retry
    with pytest.raises(TransientError, match="timed out"):
        slow.apply(make_ctx(), batch(upsert("1", n=1)))


@respx.mock
def test_response_map_is_recorded(make_ctx):
    respx.post(f"{BASE}/orders").mock(
        return_value=httpx.Response(201, json={"data": {"id": "SO-1", "status": "open"}, "raw": "not kept"})
    )
    sink = http.Call(
        "erp", method="POST", url="/orders", response_map={"erp_id": "data.id", "missing": "data.nope"}
    )
    (response,) = sink.apply(make_ctx(), batch(upsert("1", n=1))).responses
    assert (response.key, response.status, dict(response.fields)) == (
        "1",
        201,
        {"erp_id": "SO-1", "missing": None},
    )
    respx.post(f"{BASE}/orders").mock(return_value=httpx.Response(201, text="created"))
    (plain,) = sink.apply(make_ctx(), batch(upsert("1", n=1))).responses
    assert plain.fields == {}  # an answer that is not JSON gives no fields


@respx.mock
def test_a_delete_is_rejected_by_default(make_ctx):
    route = respx.post(f"{BASE}/orders").mock(return_value=httpx.Response(201))
    gone = ApplyRecord("1", "delete", None)
    result = http.Call("erp", method="POST", url="/orders").apply(make_ctx(), batch(gone))
    assert result.applied == 0 and [r.error_class for r in result.rejected] == ["DeleteNotSupported"]
    skipped = http.Call("erp", method="POST", url="/orders", on_delete="skip").apply(make_ctx(), batch(gone))
    assert skipped.applied == 1 and not skipped.rejected
    assert not route.called


def test_readme_example_runs(tmp_path: Path):
    flow = run_readme_examples(Path(__file__).resolve().parents[1] / "README.md", tmp_path)["flow"]
    (route,) = flow.routes
    assert isinstance(route.sink, http.Call) and route.sink.response_map == {"erp_id": "id"}


@respx.mock
def test_a_409_answer_gives_its_fields_too(make_ctx):  # I3
    respx.post(f"{BASE}/orders").mock(return_value=httpx.Response(409, json={"id": "SO-1", "e": "duplicate"}))
    sink = http.Call("erp", method="POST", url="/orders", response_map={"erp_id": "id"})
    ctx = make_ctx()
    result = sink.apply(ctx, batch(upsert("1", n=1)))
    assert result.applied == 1 and [(r.status, dict(r.fields)) for r in result.responses] == [
        (409, {"erp_id": "SO-1"})
    ]
    assert ctx.answers() == list(result.responses)  # each answer is noted as it arrives


@respx.mock
def test_answers_are_noted_before_a_chunk_fails_part_way(make_ctx):  # I3
    respx.post(f"{BASE}/orders/1").mock(return_value=httpx.Response(201, json={"id": "SO-1"}))
    respx.post(f"{BASE}/orders/2").mock(return_value=httpx.Response(503))
    sink = http.Call("erp", method="POST", url="/orders/{n}", response_map={"erp_id": "id"})
    sink.sleep = no_retry
    ctx = make_ctx()
    with pytest.raises(TransientError):
        sink.apply(ctx, batch(upsert("1", n=1), upsert("2", n=2)))
    assert [(r.key, dict(r.fields)) for r in ctx.answers()] == [("1", {"erp_id": "SO-1"})]


@respx.mock
def test_a_configuration_error_fails_the_chunk(make_ctx):  # I4, I2
    for status in (401, 403, 404, 405, 407, 301, 302):
        respx.post(f"{BASE}/orders").mock(return_value=httpx.Response(status, text="no"))
        sink = http.Call("erp", method="POST", url="/orders")
        with pytest.raises(PermanentError, match=f"HTTP {status}"):
            sink.apply(make_ctx(), batch(upsert("1", n=1)))
    respx.post(f"{BASE}/orders").mock(return_value=httpx.Response(404, text="no such order"))
    lenient = http.Call("erp", method="POST", url="/orders", fail_statuses=(401,))
    result = lenient.apply(make_ctx(), batch(upsert("1", n=1)))
    assert result.applied == 0 and [r.error_class for r in result.rejected] == ["HttpStatusError"]


@respx.mock
def test_url_values_are_percent_encoded(make_ctx):  # M6
    route = respx.put(url__startswith=f"{BASE}/orders/").mock(return_value=httpx.Response(200))
    sink = http.Call("erp", method="PUT", url="/orders/{order_id}")
    sink.apply(make_ctx(), batch(upsert("1", order_id="A/B?x=..")))
    assert route.calls.last.request.url.raw_path == b"/orders/A%2FB%3Fx%3D.."


def test_a_replaced_client_is_closed_on_the_next_swap(make_ctx):  # M11
    sink = http.Call("erp", method="POST", url="/orders")
    first = sink.client(make_ctx(timeout=10))
    second = sink.client(make_ctx(timeout=11))
    assert not first.closed  # a call may still be using it
    sink.client(make_ctx(timeout=12))
    assert first.closed and not second.closed

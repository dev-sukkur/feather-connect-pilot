"""`sap_cpi`: OAuth2 client credentials against the BTP token URL, the CSRF fetch, an idempotent POST per record,
the reversal that `compensate` posts, and the source reading an iFlow's JSON (M2 plan 2 stage C1)."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from feather_connect.connectors import sap_cpi  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.partitions import none

BASE = "https://acme.it-cpi018-rt.cfapps.eu10.hana.ondemand.com"
TOKEN_URL = "https://acme.authentication.eu10.hana.ondemand.com/oauth/token"
NOW = datetime(2026, 11, 12, 9, tzinfo=UTC)


def ctx(**config: Any) -> Context:
    settings = {
        "base_url": BASE,
        "token_url": TOKEN_URL,
        "client_id": "cid",
        "client_secret": "csecret",
        **config,
    }
    return Context(settings, logging.getLogger("cpi-test"), NOW, "UTC")


def batch(*records: ApplyRecord, key: str = "sap:v1:0:0") -> ApplyBatch:
    return ApplyBatch("sap", "v1", 0, "_", "upsert", records, key, True, True)


def token() -> respx.Route:
    return respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(
            200, json={"access_token": "tok", "expires_in": 3600, "token_type": "bearer"}
        )
    )


@respx.mock
def test_each_record_is_posted_once_with_the_token_and_an_idempotency_key():
    token()
    posted = respx.post(f"{BASE}/http/invoices").mock(
        return_value=httpx.Response(201, json={"d": {"BillingDocument": "90000001"}})
    )
    sink = sap_cpi.Post("sap", path="/http/invoices", response_map={"sap_document": "d.BillingDocument"})
    result = sink.apply(ctx(), batch(ApplyRecord("INV-1", "upsert", {"invoice": "INV-1", "total": 10})))
    assert result.applied == 1 and result.responses[0].fields == {"sap_document": "90000001"}
    sent = posted.calls.last.request
    assert sent.headers["authorization"] == "Bearer tok" and sent.headers["idempotency-key"]
    again = sink.apply(ctx(), batch(ApplyRecord("INV-1", "upsert", {"invoice": "INV-1", "total": 10})))
    assert again.applied == 1
    assert (
        posted.calls[0].request.headers["idempotency-key"]
        == posted.calls[1].request.headers["idempotency-key"]
    )


@respx.mock
def test_a_csrf_protected_iflow_gets_a_fetched_token_and_its_cookie():
    token()
    fetch = respx.get(f"{BASE}/http/invoices").mock(
        return_value=httpx.Response(
            200, headers={"X-CSRF-Token": "csrf-1", "Set-Cookie": "JSESSIONID=s1; Path=/"}
        )
    )
    posted = respx.post(f"{BASE}/http/invoices").mock(return_value=httpx.Response(200, json={}))
    sink = sap_cpi.Post("sap", path="/http/invoices")
    sink.apply(
        ctx(csrf=True), batch(ApplyRecord("1", "upsert", {"a": 1}), ApplyRecord("2", "upsert", {"a": 2}))
    )
    assert (
        fetch.calls.last.request.headers["x-csrf-token"] == "Fetch" and fetch.call_count == 1
    )  # once a chunk
    sent = posted.calls.last.request
    assert sent.headers["x-csrf-token"] == "csrf-1" and "JSESSIONID=s1" in sent.headers["cookie"]


@respx.mock
def test_no_csrf_token_fails_the_chunk_naming_why():
    token()
    respx.get(f"{BASE}/http/invoices").mock(return_value=httpx.Response(200))
    with pytest.raises(PermanentError, match="CSRF"):
        sap_cpi.Post("sap", path="/http/invoices").apply(
            ctx(csrf=True), batch(ApplyRecord("1", "upsert", {"a": 1}))
        )


@respx.mock
def test_compensate_posts_the_records_to_the_reversal_path_under_their_own_keys():
    token()
    posted = respx.post(f"{BASE}/http/invoices").mock(return_value=httpx.Response(201, json={}))
    reversed_ = respx.post(f"{BASE}/http/invoices/reverse").mock(return_value=httpx.Response(200, json={}))
    sink = sap_cpi.Post("sap", path="/http/invoices", reversal_path="/http/invoices/reverse")
    record = ApplyRecord("INV-1", "upsert", {"invoice": "INV-1"})
    sink.apply(ctx(), batch(record))
    assert sink.compensate(ctx(), batch(record)).applied == 1
    assert json.loads(reversed_.calls.last.request.content) == {"invoice": "INV-1"}
    keys = (
        posted.calls.last.request.headers["idempotency-key"],
        reversed_.calls.last.request.headers["idempotency-key"],
    )
    assert keys[0] != keys[1]  # a reversal is not taken for the posting again
    assert sap_cpi.Post.spec.side_effect


def test_without_a_reversal_path_compensate_says_so():
    with pytest.raises(PermanentError, match="reversal_path"):
        sap_cpi.Post("sap", path="/http/x").compensate(ctx(), batch(ApplyRecord("1", "upsert", {"a": 1})))


@respx.mock
def test_the_source_reads_an_iflows_json_records():
    token()
    respx.get(f"{BASE}/http/prices").mock(
        return_value=httpx.Response(200, json={"d": {"results": [{"Material": "M1", "Price": "9.5"}]}})
    )
    source = sap_cpi.Source("sap", path="/http/prices", records_path="d.results")
    rows = [r for page in source.read(ctx(), ReadRequest("*", none())) for r in page]
    assert rows == [{"Material": "M1", "Price": "9.5"}]


def test_a_connection_with_an_unknown_setting_is_refused():
    with pytest.raises(PermanentError, match="not set up right"):
        sap_cpi.Post("sap", path="/http/x").apply(
            ctx(colour="red"), batch(ApplyRecord("1", "upsert", {"a": 1}))
        )

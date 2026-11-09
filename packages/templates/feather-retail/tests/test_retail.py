"""`zakya_invoices_to_erp`: Zakya's API as the template reads it, and the ERP call it makes of each invoice."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import httpx
import respx

import feather_connect as fc
from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.connectors.memory import Sink
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ReadRequest
from feather_connect.core.context import Context
from feather_connect.templates.retail import (  # pyright: ignore[reportMissingImports]
    erp_invoice,
    zakya_invoices_to_erp,
)

ZAKYA = "https://zakya.example.test"
ERP = "https://erp.example.test"
TOKEN_URL = "https://accounts.example.test/oauth/v2/token"
NOW = datetime(2026, 11, 7, 9, tzinfo=UTC)
ZAKYA_CONFIG = {
    "base_url": ZAKYA,
    "accounts_url": "https://accounts.example.test",
    "organization_id": "60001",
    "client_id": "fake-client",
    "client_secret": "fake-secret",
    "refresh_token": "fake-refresh",
}


def invoice(n: int, modified: str) -> dict[str, Any]:
    return {
        "invoice_id": f"inv-{n}",
        "invoice_number": f"INV-{n:05d}",
        "customer_id": "c-1",
        "customer_name": "Asha Traders",
        "date": "2026-11-07",
        "due_date": "2026-11-21",
        "status": "sent",
        "currency_code": "INR",
        "total": 1180.0,
        "balance": 1180.0,
        "last_modified_time": modified,
        "line_items": [{"sku": "TEA-1", "name": "Tea", "quantity": 2, "rate": 500.0, "item_total": 1000.0}],
    }


INVOICES = [invoice(n, f"2026-11-07T10:0{n}:00+0530") for n in range(1, 4)]


def zakya_api(request: httpx.Request) -> httpx.Response:
    assert request.headers["authorization"] == "Zoho-oauthtoken access-1"
    assert request.url.params["organization_id"] == "60001"
    if request.url.path.startswith("/inventory/v1/invoices/"):
        found = next(i for i in INVOICES if i["invoice_id"] == request.url.path.rsplit("/", 1)[1])
        return httpx.Response(200, json={"code": 0, "invoice": found})
    since = request.url.params.get("last_modified_time")
    page, size = int(request.url.params["page"]), int(request.url.params["per_page"])
    listed = [
        {k: v for k, v in i.items() if k != "line_items"}
        for i in INVOICES
        if since is None or datetime.fromisoformat(i["last_modified_time"]) >= datetime.fromisoformat(since)
    ]
    chunk = listed[(page - 1) * size : page * size]
    more = page * size < len(listed)
    return httpx.Response(200, json={"code": 0, "invoices": chunk, "page_context": {"has_more_page": more}})


def ctx(**config: Any) -> Context:
    return Context(config, logging.getLogger("retail-test"), NOW, "UTC")


@respx.mock
def test_the_flow_reads_zakya_invoices_page_by_page_with_details():
    respx.post(TOKEN_URL).mock(
        return_value=httpx.Response(200, json={"access_token": "access-1", "expires_in": 3600})
    )
    respx.get(url__startswith=f"{ZAKYA}/inventory/v1/invoices").mock(side_effect=zakya_api)
    flow = zakya_invoices_to_erp(page_size=2)
    (dataset,) = flow.datasets
    assert (dataset.name, dataset.key, dataset.strategy.name) == ("invoices", "invoice_id", "by_stamp")
    assert dataset.source.spec.name == "zakya.source"
    request = ReadRequest("*", fc.partition.none())
    rows = [row for batch in dataset.source.read(ctx(**ZAKYA_CONFIG), request) for row in batch]
    assert [r["invoice_id"] for r in rows] == ["inv-1", "inv-2", "inv-3"]
    assert rows[0]["line_items"][0]["sku"] == "TEA-1"  # from the detail endpoint
    since = ReadRequest("*", fc.partition.none(), since=datetime(2026, 11, 7, 4, 32, tzinfo=UTC))
    later = [row for batch in dataset.source.read(ctx(**ZAKYA_CONFIG), since) for row in batch]
    assert [r["invoice_id"] for r in later] == ["inv-2", "inv-3"]
    sent = respx.calls[-3].request.url.params["last_modified_time"]
    assert sent == "2026-11-07T04:32:00+0000"


@respx.mock
def test_each_invoice_is_put_to_the_erp_by_its_id_in_the_default_shape():  # I5: an upsert, not a new POST
    route = respx.put(f"{ERP}/invoices/inv-1").mock(return_value=httpx.Response(201, json={"id": "ERP-9"}))
    flow = zakya_invoices_to_erp()
    (erp,) = flow.routes
    assert erp.name == "erp" and tuple(flow.triggers) == (fc.cron("* * * * *", tz="UTC"),)
    batch = ApplyBatch(
        "erp", "v1", 0, "*", "upsert", [ApplyRecord("inv-1", "upsert", INVOICES[0])], "k", True, True
    )
    result = erp.sink.apply(ctx(base_url=ERP), batch)
    body = json.loads(route.calls.last.request.content)
    assert body == json.loads(json.dumps(erp_invoice(INVOICES[0])))
    assert body["external_id"] == "inv-1" and body["lines"] == [
        {"sku": "TEA-1", "name": "Tea", "quantity": 2, "rate": 500.0, "amount": 1000.0}
    ]
    assert [(r.status, dict(r.fields)) for r in result.responses] == [(201, {"erp_id": "ERP-9"})]
    custom = respx.post(f"{ERP}/api/v2/invoices/inv-1").mock(return_value=httpx.Response(200, json={}))
    (other,) = zakya_invoices_to_erp(erp_path="/api/v2/invoices", method="POST").routes
    other.sink.apply(ctx(base_url=ERP), batch)
    assert custom.called


def test_what_a_customer_changes_is_taken():
    extra = fc.to(Sink("archive"))
    flow = zakya_invoices_to_erp(
        source="zakya-in",
        destination="erp-eu",
        map=lambda i: {"ref": i["invoice_number"]},
        extra_routes=[extra],
        schedule=[fc.every("5m")],
        name="invoices",
    )
    assert flow.name == "invoices" and tuple(flow.triggers) == (fc.every("5m"),)
    assert flow.datasets[0].source.connection == "zakya-in"
    erp, archive = flow.routes
    assert isinstance(erp.sink, http.Call) and erp.sink.connection == "erp-eu" and archive is extra
    assert erp.sink.body(INVOICES[0]) == {"ref": "INV-00001"}
    assert not [p for p in flow.validate() if p.severity == "error"]

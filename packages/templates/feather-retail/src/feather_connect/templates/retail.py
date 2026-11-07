"""Retail flow templates, as library functions (ruling 12): a project calls one with what it changes, and gets the
template's fixes when it upgrades. `feather new flow --template` copies one into a project for full control.

`zakya_invoices_to_erp` reads new and changed invoices from Zakya (Zoho Inventory's API) and posts each to an ERP:

    connections:
      zakya:
        connector: http.rest
        budget: zoho-api
        base_url: https://api.zakya.in
        auth_scheme: oauth2_refresh_token
        token_url: https://accounts.zoho.in/oauth/v2/token
        token_prefix: Zoho-oauthtoken
        params: {organization_id: "60001"}
        auth:
          client_id: ${secret:env:ZAKYA_CLIENT_ID}
          client_secret: ${secret:env:ZAKYA_CLIENT_SECRET}
          refresh_token: ${secret:env:ZAKYA_REFRESH_TOKEN}
      erp:
        connector: http.call
        base_url: https://erp.example.com
        auth_scheme: bearer
        auth: {token: "${secret:env:ERP_TOKEN}"}
"""

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import feather_connect as fc
from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.flow import RouteSpec
from feather_connect.core.triggers import Trigger

__all__ = ["EVERY_MINUTE", "ZAKYA_STAMP_FORMAT", "erp_invoice", "zakya_invoices", "zakya_invoices_to_erp"]

# How Zakya writes `last_modified_time`, and takes it back in a filter: 2026-11-07T10:15:00+0530.
ZAKYA_STAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
# The default schedule: a frozen trigger, so one value can be shared by every call.
EVERY_MINUTE = fc.cron("* * * * *", tz="UTC")
InvoiceMap = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def erp_invoice(invoice: Mapping[str, Any]) -> dict[str, Any]:
    """The default body for the ERP: a Zakya invoice, as its detail endpoint serves it, in plain names."""
    lines: list[Mapping[str, Any]] = invoice.get("line_items") or []
    return {
        "external_id": invoice["invoice_id"],
        "number": invoice.get("invoice_number"),
        "date": invoice.get("date"),
        "due_date": invoice.get("due_date"),
        "status": invoice.get("status"),
        "customer": {"id": invoice.get("customer_id"), "name": invoice.get("customer_name")},
        "currency": invoice.get("currency_code"),
        "total": invoice.get("total"),
        "balance": invoice.get("balance"),
        "lines": [
            {
                "sku": line.get("sku"),
                "name": line.get("name"),
                "quantity": line.get("quantity"),
                "rate": line.get("rate"),
                "amount": line.get("item_total"),
            }
            for line in lines
        ],
        "modified_at": invoice.get("last_modified_time"),
    }


def zakya_invoices(source: str = "zakya", *, page_size: int = 200, lookback: str = "0s") -> fc.Dataset:
    """The invoices of a Zakya organisation, read by `last_modified_time`: each listed page, then each invoice on its
    own for its line items. The connection's `params` carry the `organization_id`."""
    return fc.Dataset(
        "invoices",
        source=http.Rest(
            source,
            path="/inventory/v1/invoices",
            records_path="invoices",
            pagination=http.Page(
                "page", "per_page", page_size=page_size, has_more_path="page_context.has_more_page"
            ),
            params={"sort_column": "last_modified_time", "sort_order": "A"},
            stamp_param="last_modified_time",
            stamp_format=ZAKYA_STAMP_FORMAT,
            detail_path="/inventory/v1/invoices/{invoice_id}",
            detail_records_path="invoice",
        ),
        key="invoice_id",
        strategy=fc.strategy.by_stamp("last_modified_time", lookback=lookback),
    )


def zakya_invoices_to_erp(
    *,
    source: str = "zakya",
    destination: str = "erp",
    map: InvoiceMap | None = None,
    extra_routes: Sequence[RouteSpec] = (),
    schedule: Trigger | Sequence[Trigger] = EVERY_MINUTE,
    name: str = "zakya-invoices-to-erp",
    erp_path: str = "/invoices",
    response_map: Mapping[str, str] | None = None,
    page_size: int = 200,
    lookback: str = "0s",
) -> fc.Flow:
    """A flow that posts each new or changed Zakya invoice to an ERP, one request each.

    `source` and `destination` name the connections. `map` turns an invoice into the ERP's body (`erp_invoice`
    when None), and `erp_path` is where it goes. The ERP's answer gives `response_map`'s fields, by default its `id`
    as `erp_id`. `extra_routes` deliver the same invoices elsewhere too, and `schedule` says when the flow runs.
    """
    triggers = list(schedule) if isinstance(schedule, Sequence) else [schedule]
    erp = http.Call(
        destination,
        method="POST",
        url=erp_path,
        body=map or erp_invoice,
        response_map={"erp_id": "id"} if response_map is None else response_map,
    )
    return fc.Flow(
        name,
        reads=zakya_invoices(source, page_size=page_size, lookback=lookback),
        deliver=[fc.to(erp), *extra_routes],
        triggers=triggers,
    )

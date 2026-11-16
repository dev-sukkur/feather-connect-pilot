"""The reference chain: Zakya → Postgres staging → SAP through CPI, for B2B and B2C sales and their returns.

Staging, in the Postgres of `feather dev` (schema `zakya_demo`):

- `zakya-invoices` (flows/invoices.py) upserts each invoice into `invoices`, with its channel (B2B for a customer
  registered for GST, B2C for a consumer) and its lines, each with both barcodes, as JSON.
- `zakya-credit-notes` does the same for the goods that come back, into `credit_notes`: a credit note names the
  invoice it is against.
- `sap-materials` reads SAP's material master every 15 minutes and keeps its state, so that the flows to SAP can
  find a material by the EAN on Zakya's line (`ctx.lookup("materials", ean)`, or `{{ dataset('materials') }}` in SQL).

To SAP, once a run's documents are in staging (`fc.on_ready`, at most every 5 minutes):

- B2B, one document per document: `invoices-to-sap` posts a billing document per invoice, and `b2b-returns-to-sap`
  a credit memo per credit note, against the invoice it names; while SAP doesn't have that invoice yet (it is held
  for approval, say), the credit memo's delivery fails, and a Retry on the Failures page posts it once it has. Either one over 500,000 holds its delivery for an
  approver (Holds page); an applied delivery can be reversed (Compensate, on the delivery's page).
- Returns are posted as the difference (a credit memo, a returns summary), not by reversing and posting again: a
  proposal for finance to confirm (discovery Q8, pilot D8); the README compares the two.
- B2C, summarised: `b2c-to-sap` (flows/b2c.py) sums the new B2C lines per day and material and posts one summary per
  day, material and direction (sales, returns).
"""

import json
from typing import Any

import feather_connect as fc
from feather_connect.connectors import http, postgres, sap_cpi, zakya  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.templates.retail import EVERY_MINUTE  # pyright: ignore[reportMissingImports]

APPROVAL_ABOVE = 500_000
NOT_YET = frozenset({424})  # SAP's answer to a credit memo whose invoice it doesn't have yet
EVERY_FIVE_MINUTES = "5m"


def channel_of(document: fc.Record | dict[str, Any]) -> str:
    """B2B for a customer registered for GST (`gst_treatment` business_*), B2C for a consumer."""
    return "B2B" if str(document.get("gst_treatment") or "").startswith("business") else "B2C"


def _listed(value: Any) -> list[dict[str, Any]]:
    """Lines as a list, whether they come as one or as JSON text (a column of mixed types is kept as JSON)."""
    if isinstance(value, str):
        value = json.loads(value)
    return list(value or [])


def staged_lines(document: fc.Record) -> list[dict[str, Any]]:
    """A Zakya document's lines as staging keeps them: both barcodes, the quantity, the rate and the net amount."""
    return [
        {
            "line_no": i,
            "ean": line.get("ean"),
            "pos_code": line.get("sku"),
            "name": line.get("name"),
            "quantity": float(line.get("quantity") or 0),
            "rate": float(line.get("rate") or 0),
            "amount": float(line.get("item_total") or 0),
        }
        for i, line in enumerate(_listed(document.get("line_items")), start=1)
    ]


def to_staging_row(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {
        "invoice_id": record["invoice_id"],
        "invoice_number": record.get("invoice_number"),
        "customer_id": record.get("customer_id"),
        "customer_name": record.get("customer_name"),
        "channel": channel_of(record),
        "gst_no": record.get("gst_no") or None,
        "invoice_date": record.get("date"),
        "total": record.get("total"),
        "status": record.get("status"),
        "last_modified_time": record.get("last_modified_time"),
        "lines": staged_lines(record),
    }


# The route `zakya-invoices` adds, to the staging table.
staging_route = fc.to(
    postgres.Table("staging", table="invoices", key_columns=["invoice_id"]),
    name="staging",
    map=to_staging_row,
)


def to_staging_return(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {
        "creditnote_id": record["creditnote_id"],
        "creditnote_number": record.get("creditnote_number"),
        "invoice_id": record.get("invoice_id"),
        "invoice_number": record.get("invoice_number"),
        "customer_id": record.get("customer_id"),
        "customer_name": record.get("customer_name"),
        "channel": channel_of(record),
        "gst_no": record.get("gst_no") or None,
        "note_date": record.get("date"),
        "total": record.get("total"),
        "status": record.get("status"),
        "reason": record.get("reason"),
        "last_modified_time": record.get("last_modified_time"),
        "lines": staged_lines(record),
    }


# Goods that come back: Zakya's credit notes, read by `last_modified_time` as the invoices are.
credit_notes = zakya.dataset("credit_notes", "zakya", page_size=25)
zakya_credit_notes = fc.Flow(
    "zakya-credit-notes",
    reads=credit_notes,
    deliver=[
        fc.to(
            postgres.Table("staging", table="credit_notes", key_columns=["creditnote_id"]),
            name="staging-returns",
            map=to_staging_return,
        )
    ],
    triggers=[EVERY_MINUTE],
)

# SAP's material master, kept as state for lookups by EAN, and copied to staging for anyone to query.
materials = fc.Dataset(
    "materials",
    source=sap_cpi.Source("sap_master", path="/http/materials", records_path="d.results"),
    key="ean",
    strategy=fc.strategy.snapshot_diff(),
    keep_state=True,
)
sap_materials = fc.Flow(
    "sap-materials",
    reads=materials,
    deliver=[
        fc.to(
            postgres.Table("staging", table="materials", key_columns=["ean"]),
            name="staging-materials",
        )
    ],
    triggers=[fc.cron("*/15 * * * *", tz="UTC")],
)


def sap_items(lines: Any, ctx: fc.Context) -> list[dict[str, Any]]:
    """Staged lines as SAP items: the material found by the line's EAN in SAP's master. A barcode SAP doesn't know
    turns the document down, to the quarantine, rather than posting it without a material."""
    items: list[dict[str, Any]] = []
    for i, line in enumerate(_listed(lines), start=1):
        material = ctx.lookup("materials", str(line.get("ean")))
        if material is None:
            raise ValueError(
                f"line {i} ({line.get('name')}): SAP has no material with EAN {line.get('ean')} "
                f"(POS code {line.get('pos_code')})"
            )
        items.append(
            {
                "ItemNo": i * 10,
                "Material": material["material"],
                "EAN": line.get("ean"),
                "Description": material["description"],
                "Quantity": line.get("quantity"),
                "NetAmount": line.get("amount"),
            }
        )
    return items


def _over_the_limit(record: fc.Record) -> bool:
    return float(record.get("total") or 0) > APPROVAL_ABOVE


approval = fc.approval.when(_over_the_limit, reason=f"over {APPROVAL_ABOVE:,}")


# B2B sales: one billing document per invoice.
# Feather's own `_fc_*` columns can't be read back as fields, so the read names the ones it wants, and the time each
# row was staged as `staged_at`, the change stamp.
STAGED = (
    "select invoice_id, invoice_number, customer_id, customer_name, gst_no, invoice_date, total, status, lines, "
    "_fc_applied_at as staged_at from zakya_demo.invoices where channel = 'B2B'"
)

staged = fc.Dataset(
    "staged-invoices",
    source=postgres.Query("staging_read", sql=STAGED),
    key="invoice_id",
    strategy=fc.strategy.by_stamp("staged_at", lookback="1m"),
)


def to_billing_document(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    items = sap_items(record.get("lines"), ctx)
    return {
        "invoice_id": record["invoice_id"],
        "BillingDocumentType": "F2",
        "Channel": "B2B",
        "SoldToParty": record.get("customer_id"),
        "CustomerName": record.get("customer_name"),
        "CustomerGSTIN": record.get("gst_no"),
        "PurchaseOrderByCustomer": record.get("invoice_number"),
        "BillingDocumentDate": record.get("invoice_date"),
        "TotalNetAmount": round(sum(float(i["NetAmount"] or 0) for i in items), 2),
        "TotalGrossAmount": float(record.get("total") or 0),
        "Currency": "INR",
        "Items": items,
    }


invoices_to_sap = fc.Flow(
    "invoices-to-sap",
    reads=staged,
    deliver=[
        fc.to(
            sap_cpi.Post(
                "sap",
                path="/http/invoices",
                reversal_path="/http/invoices/reverse",
                response_map={"sap_document": "d.BillingDocument"},
            ),
            name="sap",
            map=to_billing_document,
            approval=approval,
        )
    ],
    triggers=[
        fc.on_ready(
            fc.ready("invoices", delivered_to="staging"), each="run", at_most_every=EVERY_FIVE_MINUTES
        )
    ],
)


# B2B returns: one credit memo per credit note, against the billing document of the invoice it names.
STAGED_RETURNS = (
    "select creditnote_id, creditnote_number, invoice_id, invoice_number, customer_id, customer_name, gst_no, "
    "note_date, total, reason, lines, _fc_applied_at as staged_at from zakya_demo.credit_notes where channel = 'B2B'"
)

staged_returns = fc.Dataset(
    "staged-b2b-returns",
    source=postgres.Query("staging_read", sql=STAGED_RETURNS),
    key="creditnote_id",
    strategy=fc.strategy.by_stamp("staged_at", lookback="1m"),
)


def to_credit_memo(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    items = sap_items(record.get("lines"), ctx)
    return {
        "creditnote_id": record["creditnote_id"],
        "BillingDocumentType": "G2",
        "Channel": "B2B",
        "ReferenceInvoice": record.get("invoice_id"),
        "ReferenceInvoiceNumber": record.get("invoice_number"),
        "SoldToParty": record.get("customer_id"),
        "CustomerName": record.get("customer_name"),
        "CustomerGSTIN": record.get("gst_no"),
        "PurchaseOrderByCustomer": record.get("creditnote_number"),
        "BillingDocumentDate": record.get("note_date"),
        "Reason": record.get("reason"),
        "TotalNetAmount": round(sum(float(i["NetAmount"] or 0) for i in items), 2),
        "TotalGrossAmount": float(record.get("total") or 0),
        "Currency": "INR",
        "Items": items,
    }


b2b_returns_to_sap = fc.Flow(
    "b2b-returns-to-sap",
    reads=staged_returns,
    deliver=[
        fc.to(
            sap_cpi.Post(
                "sap",
                path="/http/creditmemos",
                reversal_path="/http/creditmemos/reverse",
                response_map={"sap_document": "d.BillingDocument"},
                # SAP answers 424 while the invoice isn't there: the delivery fails, to be retried once it is,
                # rather than the credit memo being turned down for good.
                fail_statuses=NOT_YET | http.DEFAULT_FAIL_STATUSES,
            ),
            name="sap-credit-memos",
            map=to_credit_memo,
            approval=approval,
        )
    ],
    triggers=[
        fc.on_ready(
            fc.ready("credit_notes", delivered_to="staging-returns"),
            each="run",
            at_most_every=EVERY_FIVE_MINUTES,
        )
    ],
)

"""The reference chain (M2 exit criterion): Zakya invoices → Postgres staging, then staging → SAP through CPI.

`zakya-invoices` (flows/invoices.py) upserts each invoice into the staging table too. Once a run's invoices are
applied there, `invoices-to-sap` starts, at most every 5 minutes (`fc.on_ready(..., delivered_to="staging")`), reads
the staging rows changed since its last run and posts each to SAP through a CPI iFlow. An invoice over 500,000 holds
the delivery for an approver (Holds page); an applied posting can be reversed (Compensate, on the delivery's page).
"""

from typing import Any

import feather_connect as fc
from feather_connect.connectors import postgres, sap_cpi  # pyright: ignore[reportAttributeAccessIssue]

STAGING_FIELDS = ("invoice_id", "invoice_number", "customer_name", "total", "status", "last_modified_time")
APPROVAL_ABOVE = 500_000


def to_staging_row(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {name: record.get(name) for name in STAGING_FIELDS}


# The route `zakya-invoices` adds, to the staging table.
staging_route = fc.to(
    postgres.Table("staging", table="invoices", key_columns=["invoice_id"]),
    name="staging",
    map=to_staging_row,
)

# Feather's own `_fc_*` columns can't be read back as fields, so the read names the ones it wants, and the time each
# row was staged as `staged_at`, the change stamp.
STAGED = f"select {', '.join(STAGING_FIELDS)}, _fc_applied_at as staged_at from zakya_demo.invoices"

staged = fc.Dataset(
    "staged-invoices",
    source=postgres.Query("staging_read", sql=STAGED),
    key="invoice_id",
    strategy=fc.strategy.by_stamp("staged_at", lookback="1m"),
)


def to_billing_document(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {
        "invoice_id": record["invoice_id"],
        "BillingDocumentType": "F2",
        "SoldToParty": record["customer_name"],
        "PurchaseOrderByCustomer": record["invoice_number"],
        "TotalNetAmount": str(record["total"]),
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
            approval=fc.approval.when(
                lambda r: float(r.get("total") or 0) > APPROVAL_ABOVE, reason=f"over {APPROVAL_ABOVE:,}"
            ),
        )
    ],
    triggers=[fc.on_ready(fc.ready("invoices", delivered_to="staging"), each="run", at_most_every="5m")],
)

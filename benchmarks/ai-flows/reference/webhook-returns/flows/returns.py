"""Partner returns to the ERP: the `webhook_to_api` template with a returns map."""

from typing import Any

import feather_connect as fc
from feather_connect.templates.generic import webhook_to_api  # pyright: ignore[reportMissingImports]


def erp_return(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    return {
        "external_id": record["return_id"],
        "order": record.get("order_id"),
        "reason": record.get("reason"),
        "amount": float(record["amount"]),
    }


returns = webhook_to_api(
    source="partner",
    destination="erp",
    dataset="returns",
    key="return_id",
    url="/returns/{return_id}",
    map=erp_return,
    name="partner-returns-to-erp",
)

"""Partner orders to the ERP's v2 API: the `webhook_to_api` template, its map extended."""

from typing import Any

import feather_connect as fc
from feather_connect.templates.generic import partner_order, webhook_to_api  # pyright: ignore[reportMissingImports]


def v2_order(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    order = partner_order(record, ctx)
    return {**order, "channel": "partner", "total": float(order["total"])}


orders = webhook_to_api(source="partner", destination="erp", method="POST", url="/v2/orders", map=v2_order)

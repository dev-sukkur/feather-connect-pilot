"""Items to the warehouse, cleaned: the `db_to_warehouse` template with a map."""

from typing import Any

import feather_connect as fc
from feather_connect.templates.generic import db_to_warehouse  # pyright: ignore[reportMissingImports]


def clean(record: fc.Record, ctx: fc.Context) -> dict[str, Any]:
    """The SKU upper case and trimmed, no unit of measure, and where the row came from."""
    row = {k: v for k, v in record.items() if k != "uom"}
    row["sku"] = str(record["sku"]).strip().upper()
    row["source_system"] = "app_db"
    return row


items = db_to_warehouse(source="app_db", destination="warehouse", table="items", key="item_id", map=clean)

"""Stock levels to the warehouse: the `api_to_warehouse` template over `/v2/stock`, by `changed_since`."""

from feather_connect.templates.generic import api_to_warehouse  # pyright: ignore[reportMissingImports]

stock = api_to_warehouse(
    source="inventory_api",
    destination="warehouse",
    path="/v2/stock",
    records_path="levels",
    dataset="stock",
    key="stock_id",
    stamp="changed_at",
    stamp_param="changed_since",
    name="stock-to-warehouse",
)

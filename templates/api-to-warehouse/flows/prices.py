"""Prices to the warehouse: the `api_to_warehouse` template, paging `/v1/prices` by `updated_since`."""

from feather_connect.templates.generic import api_to_warehouse  # pyright: ignore[reportMissingImports]

prices = api_to_warehouse(
    source="pricing_api", destination="warehouse", path="/v1/prices", records_path="prices"
)

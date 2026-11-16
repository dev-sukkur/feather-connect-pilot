"""Categories to the warehouse, hourly: the `db_to_warehouse` template over the `categories` table."""

import feather_connect as fc
from feather_connect.templates.generic import db_to_warehouse  # pyright: ignore[reportMissingImports]

categories = db_to_warehouse(
    source="app_db",
    destination="warehouse",
    table="categories",
    key="category_id",
    schedule=fc.every("1h"),
    name="categories-to-warehouse",
)

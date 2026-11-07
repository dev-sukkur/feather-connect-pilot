"""Items to the warehouse: the `db_to_warehouse` template with its defaults (the `items` table, by `updated_at`)."""

from feather_connect.templates.generic import db_to_warehouse  # pyright: ignore[reportMissingImports]

items = db_to_warehouse(source="app_db", destination="warehouse", table="items", key="item_id")

"""Invoice files to the database and the warehouse: the `file_drop_to_db` template with a second route."""

import feather_connect as fc
from feather_connect.connectors import duckdb  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.templates.generic import file_drop_to_db  # pyright: ignore[reportMissingImports]

invoices = file_drop_to_db(
    source="landing",
    destination="app_db",
    path="s3://landing/invoices/*.csv",
    extra_routes=[
        fc.to(
            duckdb.Table("warehouse", table="bronze_invoices", key_columns=["invoice_id"]), name="warehouse"
        )
    ],
)

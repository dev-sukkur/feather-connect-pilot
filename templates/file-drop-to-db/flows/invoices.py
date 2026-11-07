"""Invoice files to the database: the `file_drop_to_db` template over `s3://landing/invoices/*.csv`."""

from feather_connect.templates.generic import file_drop_to_db  # pyright: ignore[reportMissingImports]

invoices = file_drop_to_db(source="landing", destination="app_db", path="s3://landing/invoices/*.csv")

"""Return files to the database: the `file_drop_to_db` template over `s3://landing/returns/*.jsonl`."""

from feather_connect.templates.generic import file_drop_to_db  # pyright: ignore[reportMissingImports]

returns = file_drop_to_db(
    source="landing",
    destination="app_db",
    path="s3://landing/returns/*.jsonl",
    format="jsonl",
    dataset="returns",
    key="return_id",
    table="store_returns",
    name="return-files-to-db",
)

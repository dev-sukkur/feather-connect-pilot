# file-drop-to-db

Files in S3-compatible storage → Postgres.

Every 15 minutes it reads the CSV files under `s3://landing/invoices/` modified since the last run (by
`_object_mtime`), skipping any modified in the last two minutes as still being written, and upserts each invoice
by `invoice_id` into the `invoices` table. A row with the wrong number of fields goes to quarantine with its
text, and the rest of the file loads.

## Run it

`feather run --local --sim` runs the flow on this machine with the `dev` overlay: every connection the
overlay names as `simulation.source` or `simulation.sink` runs on a stand-in of the real connector, with
`fixtures/<dataset>.json` as its rows, so nothing outside is touched. `qa` does the same; `uat` and `prod` use
the real systems. `uv run pytest tests` runs the template's own tests.

## What to change for real systems

Set the bucket's `endpoint_url` and `region` (MinIO and other S3-compatible stores work), `LANDING_KEY_ID`,
`LANDING_SECRET` and `APP_DB_DSN`. Another layout is `file_drop_to_db(path=..., format="parquet", ...)`.

The flow is a call to a library function in `feather_connect.templates.generic`, so a fix to the template
reaches the project when it upgrades. Pass the function what differs (connection names, a `map`,
`extra_routes`, a `schedule`), or copy it in with `feather new flow --template file-drop-to-db` for full control.

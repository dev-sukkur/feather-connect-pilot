# db-to-warehouse

Postgres table → DuckDB/MotherDuck bronze, by a change stamp.

Every 15 minutes it reads the `items` rows whose `updated_at` is at or after the last watermark from the shop's
Postgres and upserts them by `item_id` into `bronze_items` in MotherDuck, with Feather's `_fc_*` columns.

## Run it

`feather run --local --sim` runs the flow on this machine with the `dev` overlay: every connection the
overlay names as `simulation.source` or `simulation.sink` runs on a stand-in of the real connector, with
`fixtures/<dataset>.json` as its rows, so nothing outside is touched. `qa` does the same; `uat` and `prod` use
the real systems. `uv run pytest tests` runs the template's own tests.

## What to change for real systems

Set `APP_DB_DSN` and `MOTHERDUCK_TOKEN`, and the `database` and `schema` of the connections in `feather.yaml`
or an overlay. Another table is `db_to_warehouse(table=..., key=..., stamp=...)`.

The flow is a call to a library function in `feather_connect.templates.generic`, so a fix to the template
reaches the project when it upgrades. Pass the function what differs (connection names, a `map`,
`extra_routes`, a `schedule`), or copy it in with `feather new flow --template db-to-warehouse` for full control.

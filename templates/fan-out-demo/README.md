# fan-out-demo

JSON simulation → Postgres + DuckDB + HTTP: the `feather dev` demo.

Every minute it reads the shop's orders from a JSON simulation (`fixtures/orders.json`) and delivers each to three
destinations: the `orders` table in Postgres, `bronze_orders` in a DuckDB file, and `PUT /orders/{order_id}` on
the ERP's API. Each route keeps its own ledger, so one destination failing holds up only its own deliveries.

## Run it

`feather run --local --sim` runs the flow on this machine with the `dev` overlay: every connection the
overlay names as `simulation.source` or `simulation.sink` runs on a stand-in of the real connector, with
`fixtures/<dataset>.json` as its rows, so nothing outside is touched. `qa` does the same; `uat` and `prod` use
the real systems. `uv run pytest tests` runs the template's own tests.

## What to change for real systems

The source stays a simulation in every environment; pass `faults=simulation.Faults(...)` to `fan_out_demo` to
make it misbehave. Set `WAREHOUSE_DSN`, `ERP_TOKEN` and the ERP's `base_url`, and `MOTHERDUCK_TOKEN` for uat and
prod, whose lake is in MotherDuck.

The flow is a call to a library function in `feather_connect.templates.generic`, so a fix to the template
reaches the project when it upgrades. Pass the function what differs (connection names, a `map`,
`extra_routes`, a `schedule`), or copy it in with `feather new flow --template fan-out-demo` for full control.

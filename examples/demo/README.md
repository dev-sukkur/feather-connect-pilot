# demo

The `feather dev` demo, a copy of `templates/fan-out-demo`: a JSON simulation of a shop's orders
(`fixtures/orders.json`), read every minute and delivered to three destinations. Each route keeps its own ledger,
so one destination failing holds up only its own deliveries.

| Route | dev (`feather dev`) | qa | uat and prod |
|---|---|---|---|
| `warehouse` | `demo.orders` in feather dev's own Postgres | simulation | the warehouse at `WAREHOUSE_DSN` |
| `lake` | `.data/lake.duckdb`, table `bronze_orders` | simulation | MotherDuck |
| `erp` | `PUT /orders/{order_id}` on `sims/targets.py`, port 29001 | simulation | the ERP's API |

`feather dev` starts `sims/targets.py` once its Postgres is up. The helper makes `demo.orders` (`sims/demo.sql`), then
stands in for the ERP: <http://127.0.0.1:29001/> lists the orders it has taken.

## Things to try

- **A destination that is down.** `curl -X POST http://127.0.0.1:29001/_targets/down` makes the ERP answer 503. The
  next change to the fixture reaches Postgres and DuckDB, and the `erp` delivery fails after its retries. Bring it
  back with `curl -X POST http://127.0.0.1:29001/_targets/up`, open the failure on the dashboard and retry it: the ERP
  catches up, and the other routes are not sent anything again.
- **New data.** Edit an order in `fixtures/orders.json`. The next run delivers only that order to each destination.
- **Look at the data.** `select * from demo.orders` in Postgres, and `duckdb .data/lake.duckdb "from bronze_orders"`.

## Without feather dev

`feather run orders --local --sim --env qa` runs the flow once on this machine, on simulation stand-ins of all
three destinations, so nothing outside is touched. `uv run pytest examples/demo/tests` runs the project's own tests.

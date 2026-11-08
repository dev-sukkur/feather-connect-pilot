"""The shop's orders, fanned out: the `fan_out_demo` template over this project's fixture.

Each order goes to three destinations: `demo.orders` in Postgres, `bronze_orders` in DuckDB, and `PUT /orders/{id}`
on the ERP's API. Each route keeps its own ledger, so a destination that is down holds up only its own deliveries:
stop the ERP and the other two carry on, then retry its failure from the dashboard and watch it catch up.
"""

from pathlib import Path

from feather_connect.templates.generic import fan_out_demo  # pyright: ignore[reportMissingImports]

orders = fan_out_demo(fixture=Path(__file__).resolve().parents[1] / "fixtures" / "orders.json", name="orders")
orders.owner = "demo"
orders.tags = ("demo",)

"""The demo flow: a simulated shop's orders, read every 10 seconds and delivered to two destinations.

The source misbehaves a little on purpose (a failed or slow read now and then, a duplicated row, a rewritten key),
so the dashboard shows steady runs, the odd retry, and deliveries that have something to move.
"""

import feather_connect as fc
from feather_connect.connectors import simulation

orders = fc.Dataset(
    "orders",
    source=simulation.Source(
        "shop",
        generator={
            "order_id": simulation.seq(),
            "customer": simulation.name(),
            "status": simulation.choice(["new", "packed", "shipped"]),
            "total": simulation.amount(5, 500),
        },
        rows_per_partition=20,
        faults=simulation.Faults(
            http_429=0.03, http_5xx=0.08, timeout=0.04, duplicate=0.05, key_rewrite=0.05
        ),
    ),
    key="order_id",
    strategy=fc.strategy.replace(),
)

demo = fc.Flow(
    "orders",
    reads=orders,
    deliver=[
        fc.to(simulation.Sink("warehouse")),
        fc.to(simulation.Sink("archive")),
    ],
    triggers=[fc.every("10s")],
    owner="demo",
    tags=["demo"],
)

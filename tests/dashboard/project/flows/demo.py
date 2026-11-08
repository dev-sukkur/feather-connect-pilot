"""`demo.items`, read a day at a time from a fixture the test writes, delivered to `lake` and to `flaky`.

The fixture and the lake's capture file are named by FEATHER_DASHBOARD_FIXTURE and FEATHER_DASHBOARD_CAPTURE.
`flaky` turns down the first delivery of every version, and takes it when it is retried.
"""

import os

import feather_connect as fc
from feather_connect.connectors import simulation

items = fc.Dataset(
    "demo.items",
    source=simulation.Source("shop", fixture=os.environ["FEATHER_DASHBOARD_FIXTURE"]),
    key="id",
    strategy=fc.strategy.replace(),
    partition=fc.partition.by_day("day", tz="UTC"),
)
flow = fc.Flow(
    "demo",
    reads=items,
    deliver=[
        fc.to(simulation.Sink("lake", capture_to=os.environ["FEATHER_DASHBOARD_CAPTURE"]), name="lake"),
        fc.to(simulation.Sink("flaky", fail_rate=1.0), name="flaky"),
    ],
    triggers=[fc.every("1d")],
    owner="ops",
)

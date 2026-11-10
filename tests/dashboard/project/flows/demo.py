"""`demo.items`, read a day at a time from a fixture the test writes, delivered to `lake` and to `flaky`.

The fixture and the lake's capture file are named by FEATHER_DASHBOARD_FIXTURE and FEATHER_DASHBOARD_CAPTURE.
`flaky` turns down the first delivery of every version, and takes it when it is retried. When it turns one down it
answers each record with a 503 and the body it was sent with, as an HTTP sink does, so that a trace can show them.
"""

import os
from pathlib import Path

import feather_connect as fc
from feather_connect.connectors import simulation
from feather_connect.core.connector import ApplyBatch, ApplyResult, RecordResponse
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError


class Flaky(simulation.Sink):
    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        try:
            return super().apply(ctx, batch)
        except PermanentError:
            for record in batch.records:
                ctx.answered(RecordResponse(record.key, 503, sent=dict(record.payload or {})))
            raise


items = fc.Dataset(
    "demo.items",
    source=simulation.Source(
        "shop",
        fixture=os.environ.get("FEATHER_DASHBOARD_FIXTURE", str(Path(__file__).with_name("empty.json"))),
    ),
    key="id",
    strategy=fc.strategy.replace(),
    partition=fc.partition.by_day("day", tz="UTC"),
)
flow = fc.Flow(
    "demo",
    reads=items,
    deliver=[
        fc.to(
            simulation.Sink("lake", capture_to=os.environ.get("FEATHER_DASHBOARD_CAPTURE", "unset")),
            name="lake",
        ),
        fc.to(Flaky("flaky", fail_rate=1.0), name="flaky"),
    ],
    triggers=[fc.every("1d")],
    owner="ops",
)

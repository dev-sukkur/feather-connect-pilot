"""The flow of the latency and burst checks: the package's declared webhook into a sink that reports through a table of
the database under test.

The time the server received each event is in the inbox (`feather.inbox.received_at`), which the test reads; the sink
adds the time of its write. Both are by this machine's clock. Nothing here calls anything outside the database.
"""

import os
import time

import psycopg

import feather_connect as fc
from feather_connect.connectors import simulation
from feather_connect.core.connector import ApplyBatch, ApplyResult, CheckResult, ConnectorSpec
from feather_connect.core.context import Context


class ReportingSink:
    """Writes what it applies to `perf.sink_rows`, with the time of the write. A row per write, so that a repeat
    shows."""

    spec = ConnectorSpec(name="perf.sink", modes=frozenset({"upsert"}))

    def __init__(self, connection: str) -> None:
        self.connection = connection

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        payloads = [record.payload for record in batch.records]
        with psycopg.connect(os.environ["FEATHER_STACK_DATABASE_URL"], autocommit=True) as conn:
            conn.execute(
                """insert into perf.sink_rows (key, sent_at, received_at, applied_at)
                   select unnest(%s::text[]), unnest(%s::float8[]), null, %s""",
                (
                    [str(record.key) for record in batch.records],
                    [payload.get("sent_at") for payload in payloads],
                    time.time(),
                ),
            )
        return ApplyResult(len(batch.records))


# Feather's own simulation source: a package may not bring a webhook source of its own (flow packages §12).
hooks = fc.Dataset(
    "perf_hooks",
    source=simulation.Source(
        "perf_hooks", generator={"id": simulation.seq()}, rows_per_partition=0, unsigned=True
    ),
    key="id",
    strategy=fc.strategy.push(),
)
push = fc.Flow(
    "perf_push", reads=hooks, deliver=[fc.to(ReportingSink("perf_sink"))], triggers=[fc.on_event(hooks)]
)

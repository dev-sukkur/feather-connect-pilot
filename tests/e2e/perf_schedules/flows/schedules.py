"""The flows of the schedule checks: a poller every 5 seconds and a flow on a cron of every minute, each into a sink that
reports through a table of the database under test. A package of its own, so that the latency checks have no poller
ticking along beside them. Nothing here calls anything outside the database: the clock source makes its row from the
time.
"""

import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import psycopg

import feather_connect as fc
from feather_connect.connectors import simulation
from feather_connect.core.connector import ApplyBatch, ApplyResult, CheckResult, ConnectorSpec, ReadRequest
from feather_connect.core.context import Context


class ClockSource(simulation.Source):
    """A new row at each read, so that each tick has something to seal."""

    spec = ConnectorSpec(name="perf.clock_source", strategies=frozenset({"by_stamp"}))

    def __init__(self, connection: str) -> None:
        super().__init__(connection, generator={"id": simulation.seq()}, rows_per_partition=0)

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        yield [{"id": time.time_ns(), "at": datetime.now(UTC).isoformat()}]


class ReportingSink:
    """Writes what it applies to `perf.sink_rows`, with the time of the write. A row per write."""

    spec = ConnectorSpec(name="perf.sink", modes=frozenset({"upsert"}))

    def __init__(self, connection: str) -> None:
        self.connection = connection

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        with psycopg.connect(os.environ["FEATHER_STACK_DATABASE_URL"], autocommit=True) as conn:
            conn.execute(
                """insert into perf.sink_rows (key, sent_at, received_at, applied_at)
                   select unnest(%s::text[]), null, null, %s""",
                ([str(record.key) for record in batch.records], time.time()),
            )
        return ApplyResult(len(batch.records))


ticks = fc.Dataset(
    "perf_ticks", source=ClockSource("perf_ticks"), key="id", strategy=fc.strategy.by_stamp("at")
)
poller = fc.Flow(
    "perf_poller", reads=ticks, deliver=[fc.to(ReportingSink("perf_sink"))], triggers=[fc.every("5s")]
)
minutes = fc.Dataset(
    "perf_minutes", source=ClockSource("perf_cron"), key="id", strategy=fc.strategy.by_stamp("at")
)
by_cron = fc.Flow(
    "perf_by_cron",
    reads=minutes,
    deliver=[fc.to(ReportingSink("perf_cron_sink"))],
    triggers=[fc.cron("* * * * *", tz="UTC")],
)

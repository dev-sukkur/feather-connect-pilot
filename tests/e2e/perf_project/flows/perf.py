"""The flows of the latency, burst and schedule checks, and connectors that report through a table of the database under
test. Which flows exist is the test's choice, as a poller ticking along would be load on a latency check.

Nothing here calls anything outside the database: the clock source makes its row from the time.
"""

import os
import time
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

import psycopg

import feather_connect as fc
from feather_connect.connectors import simulation
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyResult,
    CheckResult,
    ConnectorSpec,
    Deleted,
    InboundRequest,
    ReadRequest,
)
from feather_connect.core.context import Context

FLOWS = os.environ.get("FEATHER_PERF_FLOWS", "")


class StampedSource(simulation.Source):
    """Signed webhooks, each record of which carries the time the server received it, to compare at the sink."""

    spec = ConnectorSpec(name="perf.source", strategies=frozenset({"push"}))

    def __init__(self, connection: str) -> None:
        super().__init__(connection, generator={"id": simulation.seq()}, rows_per_partition=0)

    def receive(self, ctx: Context, request: InboundRequest) -> list[Mapping[str, Any] | Deleted]:
        stamp = request.received_at.timestamp()
        return [
            {**row, "received_at": stamp} for row in super().receive(ctx, request) if isinstance(row, dict)
        ]


class ClockSource(simulation.Source):
    """A new row at each read, so that each tick has something to seal."""

    spec = ConnectorSpec(name="perf.clock_source", strategies=frozenset({"by_stamp"}))

    def __init__(self, connection: str) -> None:
        super().__init__(connection, generator={"id": simulation.seq()}, rows_per_partition=0)

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        yield [{"id": time.time_ns(), "at": datetime.now(UTC).isoformat()}]


class ReportingSink:
    """Writes what it applies to `perf.sink_rows`, with the time of the write by this machine's clock, which is the
    clock the server stamped `received_at` by. A row per write, so that a repeat shows."""

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
                   select unnest(%s::text[]), unnest(%s::float8[]), unnest(%s::float8[]), %s""",
                (
                    [str(record.key) for record in batch.records],
                    [payload.get("sent_at") for payload in payloads],
                    [payload.get("received_at") for payload in payloads],
                    time.time(),
                ),
            )
        return ApplyResult(len(batch.records))


if FLOWS == "push":
    hooks = fc.Dataset(
        "perf_hooks", source=StampedSource("perf_hooks"), key="id", strategy=fc.strategy.push()
    )
    push = fc.Flow(
        "perf_push", reads=hooks, deliver=[fc.to(ReportingSink("perf_sink"))], triggers=[fc.on_event(hooks)]
    )

if FLOWS == "schedules":
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

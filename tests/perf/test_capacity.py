"""NFR-03: at least 5 million records a day on the *small* deployment, as a sustained run (M3 plan 1 P1).

Five million records, as ten batches of 500,000 in a dataset each, are read, spooled to Postgres, sealed and applied
to a sink, one batch after another, as a day's work would come. The run must keep up with a day's rate many times
over: it fails if the whole of it takes more than `BUDGET_S`, a twelfth of a day, so that a day of 5 million records
fits in two hours of the worker's time, with the rest left for retries, other flows and peaks.

It takes some minutes, so it runs nightly and for the release checklist only: `FEATHER_PERF_CAPACITY=1`.
"""

import logging
import os
import time
from typing import Any

import pytest
from perf_results import record
from real_stack import scratch_database, server_url

import feather_connect as fc
from feather_connect.clock import SystemClock
from feather_connect.connectors import memory, simulation
from feather_connect.core.connector import ApplyBatch, ApplyResult
from feather_connect.core.context import Context
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.apply import run_apply
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import run_extract
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.perf,
    pytest.mark.skipif(
        os.environ.get("FEATHER_PERF_CAPACITY") != "1", reason="nightly: FEATHER_PERF_CAPACITY=1"
    ),
]

BATCHES = 10
ROWS = 500_000
PER_DAY = 5_000_000
BUDGET_S = 86_400 / 12


class CountingSink(memory.Sink):
    """A memory sink that counts what it gets and keeps none of it, so five million records fit in a test."""

    def __init__(self) -> None:
        super().__init__("dst")
        self.received = 0

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        result = super().apply(ctx, batch)
        self.received += len(batch.records)
        self.calls.clear()
        self._keyed.clear()  # pyright: ignore[reportPrivateUsage]
        return result


@pytest.mark.timeout(int(BUDGET_S) + 600)
def test_five_million_records_a_day_with_room_to_spare() -> None:
    sink = CountingSink()
    route = fc.to(sink, name="dst")
    with scratch_database(server_url()) as url:
        migrate(url)
        store, spool = connect(url)
        try:
            clock = SystemClock()

            def context(connection: str) -> Context:
                return Context({}, logging.getLogger(connection), clock.now(), "UTC")

            deps = RuntimeDeps(store, spool, clock, "perf", context)
            engine = LocalEngine()  # runs each task as it is submitted, by the wall clock
            started = time.perf_counter()
            for n in range(BATCHES):
                source = simulation.Source(
                    "src",
                    generator={
                        "id": simulation.seq(),
                        "customer": simulation.name(),
                        "amount": simulation.amount(1, 1000),
                        "status": simulation.choice(["new", "paid", "shipped", "returned"]),
                    },
                    rows_per_partition=ROWS,
                )
                dataset: Any = fc.Dataset(
                    f"orders_{n}", source=source, key="id", strategy=fc.strategy.replace()
                )
                run_id = store.runs.create_run("capacity", "manual", "test", {})
                [version] = run_extract(dataset, run_id, "*", routes=[route], stage_only=False, deps=deps)
                task = f"apply-{n}"
                engine.register(
                    {task: lambda ctx, args, ds=dataset: run_apply(ctx, ds, route, args["v"], deps)}
                )
                engine.submit(_call(task, version.version_id))
                engine.run_until_idle()
            elapsed = time.perf_counter() - started
        finally:
            store.close()

    total = BATCHES * ROWS
    per_day = total / elapsed * 86_400
    record("NFR-03", "records_per_day", per_day, "records/day", PER_DAY, higher_is_better=True)
    record("NFR-03", "five_million_s", elapsed, "s", BUDGET_S, higher_is_better=False)
    assert sink.received == total, f"{sink.received} of {total} delivered"
    assert elapsed <= BUDGET_S, f"{total:,} records took {elapsed:,.0f} s, more than {BUDGET_S:,.0f} s"


def _call(task: str, version_id: str) -> Any:
    from feather_connect.engine.port import TaskCall

    return TaskCall(task, {"v": version_id}, idempotency_key=f"{task}:{version_id}")

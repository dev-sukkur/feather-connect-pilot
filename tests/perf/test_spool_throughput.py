"""NFR-02: at least 20,000 rows/s per worker from source batch to spool, excluding source-side limits."""

import subprocess
import sys
import time
from typing import Any

import pytest
from perf_results import record
from real_stack import scratch_database, server_url

import feather_connect as fc
from feather_connect.clock import SystemClock
from feather_connect.connectors import memory, simulation
from feather_connect.core.context import Context
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import run_extract
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate

pytestmark = [pytest.mark.postgres, pytest.mark.perf]

ROWS = 500_000
WARM_UP_ROWS = 20_000  # a first, small run that is not timed: imports, caches and the pool warm up
TARGET_ROWS_PER_SECOND = 20_000


def busiest() -> str:
    """The machine's busiest processes, on Linux; a CI runner shared with what earlier tests left behind shows it."""
    if sys.platform == "win32":
        return ""
    found = subprocess.run(
        ["ps", "-eo", "pid,ppid,pcpu,etime,args", "--sort=-pcpu"], capture_output=True, text=True, check=False
    )
    return "\n".join(found.stdout.splitlines()[:15])


def orders(name: str, rows: int) -> Any:
    source = simulation.Source(  # made before the clock starts: a row's generation isn't the worker's work
        "src",
        generator={
            "id": simulation.seq(),
            "customer": simulation.name(),
            "amount": simulation.amount(1, 1000),
            "ordered_on": simulation.date_in("2026-01-01", "2026-12-31"),
            "status": simulation.choice(["new", "paid", "shipped", "returned"]),
        },
        rows_per_partition=rows,
    )
    return fc.Dataset(name, source=source, key="id", strategy=fc.strategy.replace())


@pytest.mark.timeout(300)
def test_extract_to_spool_meets_20k_rows_per_second(record_property):
    print("\n" + busiest())  # what else has the machine, for a slow number to be read against
    dataset = orders("orders", ROWS)
    route = fc.to(memory.Sink("dst"))
    with scratch_database(server_url()) as url:
        migrate(url)
        store, spool = connect(url)
        try:
            run_id = store.runs.create_run("perf", "manual", "test", {})
            clock = SystemClock()

            def context(connection: str) -> Context:
                return Context({}, None, clock.now(), "UTC")  # type: ignore[arg-type]

            deps = RuntimeDeps(store, spool, clock, "perf", context)
            run_extract(
                orders("warm_up", WARM_UP_ROWS), run_id, "*", routes=[route], stage_only=False, deps=deps
            )
            started = time.perf_counter()
            [version] = run_extract(dataset, run_id, "*", routes=[route], stage_only=False, deps=deps)
            elapsed = time.perf_counter() - started
            chunks = spool.list_chunks(version.version_id)
        finally:
            store.close()

    rate = ROWS / elapsed
    record_property("rows_per_second", round(rate))
    record("NFR-02", "extract_to_spool", rate, "rows/s", TARGET_ROWS_PER_SECOND, higher_is_better=True)
    print(f"\nNFR-02: {ROWS} rows through run_extract in {elapsed:.2f} s: {rate:,.0f} rows/s")
    assert version.upserts == ROWS and sum(chunk.rows for chunk in chunks) == ROWS
    assert rate >= TARGET_ROWS_PER_SECOND, (
        f"{rate:,.0f} rows/s against a target of {TARGET_ROWS_PER_SECOND:,}"
    )

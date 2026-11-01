from datetime import UTC, datetime
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
DAYS = [f"date=2026-10-{day:02d}" for day in range(1, 8)]
ROWS = [{"id": day, "date": f"2026-10-{day:02d}"} for day in range(1, 8)]
LIVE = {"queued", "running", "parked"}


class PickySource(Source):
    """Fails one partition for good."""

    def __init__(self, bad: str | None = None) -> None:
        super().__init__(rows=ROWS)
        self.bad = bad

    def read(self, ctx: Context, req: ReadRequest) -> Any:
        if req.partition == self.bad:
            raise PermanentError("the source refuses this day")
        return super().read(ctx, req)


class Harness:
    def __init__(self, bad: str | None = None) -> None:
        self.clock = FrozenClock(T0)
        self.store = MemoryStore(self.clock)
        self.engine = LocalEngine(self.clock)
        dataset = fc.Dataset(
            "d",
            source=PickySource(bad),
            key="id",
            strategy=fc.strategy.replace(),
            partition=fc.partition.by_day("date", tz="UTC"),
        )
        flow = fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("a"))])
        self.rt = Runtime([flow], self.store, MemorySpool(), self.engine, code_sha="sha", clock=self.clock)
        self.engine.register(self.rt.tasks())

    def extracts_alive(self) -> int:
        calls = [c for c in self.engine.submitted() if c.task == "feather.extract"]
        return sum(self.engine.status(c.idempotency_key) in LIVE for c in calls)

    def drain_checking(self, limit: int, *, after_each: Any = None) -> int:
        """Run one task at a time and return the most extracts that were submitted and unfinished at once."""
        most = 0
        while True:
            try:
                self.engine.run_until_idle(max_tasks=1)
                return max(most, self.extracts_alive())
            except RuntimeError:  # one task ran and more are ready
                most = max(most, self.extracts_alive())
                if after_each is not None:
                    after_each()
                assert most <= limit


def test_a_limit_keeps_only_that_many_extracts_submitted() -> None:
    h = Harness()
    run = h.rt.start_run("f", partitions=DAYS, max_concurrent=2)
    most = h.drain_checking(2)
    assert most == 2
    assert h.store.runs.get_run(run).status == "succeeded"
    assert len(h.store.versions.sealed_for_run(run, "d")) == 7
    assert {r.status for r in h.store.runs.list_tasks(run)} == {"succeeded"}


def test_without_a_limit_every_extract_is_submitted_at_once() -> None:
    h = Harness()
    h.rt.start_run("f", partitions=DAYS)
    try:
        h.engine.run_until_idle(max_tasks=1)
    except RuntimeError:
        pass
    assert h.extracts_alive() == 7


def test_a_failed_slice_hands_on_its_place_and_the_run_fails() -> None:
    h = Harness(bad=DAYS[1])
    run = h.rt.start_run("f", partitions=DAYS, max_concurrent=1)
    h.drain_checking(1)
    run_row = h.store.runs.get_run(run)
    assert run_row.status == "failed" and "refuses this day" in (run_row.reason or "")
    done = [t for t in h.store.runs.list_tasks(run) if t.kind == "extract"]
    assert [t.status for t in done].count("succeeded") == 6 and [t.status for t in done].count("failed") == 1


def test_cancelling_the_run_stops_the_chain() -> None:
    h = Harness()
    run = h.rt.start_run("f", partitions=DAYS, max_concurrent=2)
    h.drain_checking(2, after_each=lambda: h.rt.cancel_run(run, actor="op") if h.extracts_alive() else None)
    assert h.store.runs.get_run(run).status == "cancelled"
    submitted = [c for c in h.engine.submitted() if c.task == "feather.extract"]
    assert len(submitted) < 7


def test_a_limit_below_one_is_refused() -> None:
    h = Harness()
    with pytest.raises(ValueError, match="max_concurrent"):
        h.rt.start_run("f", partitions=DAYS, max_concurrent=0)

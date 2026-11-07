from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import ApplyBatch, ApplyResult, ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import BudgetDeferred
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import (
    BudgetSpec,
    BudgetStore,
    Grant,
    Priority,
    Store,
    WindowUsage,
    parse_limits,
)
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)


def handle_on(spec: BudgetSpec) -> tuple[BudgetHandle, FrozenClock, list[float]]:
    clock = FrozenClock(T0)
    store = MemoryStore(clock)
    store.budgets.define(spec)
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(timedelta(seconds=seconds))

    return BudgetHandle(store.budgets, spec.name, "normal", sleep), clock, sleeps


def test_parse_limits_reads_units_per_period():
    assert [(w.units, w.seconds) for w in parse_limits(["90/minute", "24000/day", "5/second", "2/hour"])] == [
        (90, 60),
        (24000, 86400),
        (5, 1),
        (2, 3600),
    ]
    for bad in ["90", "x/minute", "90/week", "0/day"]:
        with pytest.raises(ValueError):
            parse_limits([bad])


def test_a_granted_acquire_returns_without_sleeping():
    handle, _, sleeps = handle_on(BudgetSpec("z", parse_limits(["10/minute"])))
    handle.acquire(3)
    assert sleeps == []


def test_short_wait_sleeps_in_process():
    handle, clock, sleeps = handle_on(BudgetSpec("z", parse_limits(["10/minute"])))
    handle.acquire(10)
    handle.acquire()  # the sleep gives it a unit
    assert sleeps == [6.0] and clock.now() == T0 + timedelta(seconds=6)


def test_long_wait_raises_budget_deferred():
    handle, _, sleeps = handle_on(BudgetSpec("z", parse_limits(["10/minute", "10/day"])))
    handle.acquire(10)
    with pytest.raises(BudgetDeferred) as e:
        handle.acquire()
    assert e.value.budget == "z" and e.value.wait > timedelta(seconds=60)
    assert sleeps == []


# Flows that meter their calls: a source takes one unit per read and a sink one per apply.


class MeteredSource(Source):
    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        assert ctx.budget is not None
        ctx.budget.acquire()
        yield from super().read(ctx, req)


class MeteredSink(Sink):
    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        assert ctx.budget is not None
        ctx.budget.acquire()
        return super().apply(ctx, batch)


class SpyBudgets(BudgetStore):
    """Wraps a budget store and notes when each grant was made, and at what priority."""

    def __init__(self, inner: BudgetStore, clock: FrozenClock) -> None:
        self._inner, self._clock = inner, clock
        self.grants: list[tuple[datetime, Priority, int]] = []

    def define(self, spec: BudgetSpec) -> None:
        self._inner.define(spec)

    def acquire(self, name: str, units: int, priority: Priority = "normal") -> Grant:
        grant = self._inner.acquire(name, units, priority)
        if grant.granted:
            self.grants.append((self._clock.now(), priority, units))
        return grant

    def usage(self, name: str) -> list[WindowUsage]:
        return self._inner.usage(name)


def days(first: int, count: int) -> list[str]:
    return [f"date=2026-10-{first + n:02d}" for n in range(count)]


def rows_on(partitions: list[str], first_id: int) -> list[dict[str, Any]]:
    return [{"id": first_id + n, "date": p.removeprefix("date=")} for n, p in enumerate(partitions)]


def by_day_dataset(name: str, source: Source, **kw: Any) -> fc.Dataset:
    return fc.Dataset(
        name,
        source=source,
        key="id",
        strategy=fc.strategy.snapshot_diff(),
        partition=fc.partition.by_day("date", tz="UTC"),
        **kw,
    )


class World:
    """A runtime on a frozen clock, whose budget store notes every grant."""

    def __init__(self, spec: BudgetSpec, flows: list[fc.Flow], budgets: dict[str, str]) -> None:
        self.clock = FrozenClock(T0)
        base = MemoryStore(self.clock)
        self.spy = SpyBudgets(base.budgets, self.clock)
        self.store = Store(
            runs=base.runs,
            versions=base.versions,
            ledger=base.ledger,
            state=base.state,
            quarantine=base.quarantine,
            budgets=self.spy,
            catalog=base.catalog,
            workers=base.workers,
            schedules=base.schedules,
            overrides=base.overrides,
            users=base.users,
            audit=base.audit,
            reads=base.reads,
            inbox=base.inbox,
            responses=base.responses,
        )
        self.store.budgets.define(spec)
        self.engine = LocalEngine(self.clock)
        self.runtime = Runtime(
            flows, self.store, MemorySpool(), self.engine, clock=self.clock, budgets=budgets
        )
        self.engine.register(self.runtime.tasks())


def test_two_flows_share_one_budget():  # H7
    read_days, write_days = days(1, 6), days(11, 8)
    out = MeteredSink("dst-w")
    # A high-priority reader and a low-priority writer, both metered by the one budget.
    reader = by_day_dataset("reads", MeteredSource("src-r", rows_on(read_days, 1)), priority="high")
    writer = by_day_dataset("writes", Source("src-w", rows_on(write_days, 100)))
    world = World(
        BudgetSpec("z", parse_limits(["10/hour"]), reserve={"high": 0.6}),
        [
            fc.Flow("reading", reads=reader, deliver=[fc.to(Sink("dst-r"))]),
            fc.Flow("writing", reads=writer, deliver=[fc.to(out, priority="low")]),
        ],
        {"src-r": "z", "dst-w": "z"},
    )
    reading = world.runtime.start_run("reading", partitions=read_days)
    writing = world.runtime.start_run("writing", partitions=write_days)
    world.engine.run_until_idle()

    assert world.store.runs.get_run(reading).status == world.store.runs.get_run(writing).status == "succeeded"
    assert len(out.rows) == 8
    grants = world.spy.grants
    first = [g for g in grants if g[0] == T0]  # everything asked for at once, before any refill
    assert sum(g[2] for g in first) <= 10 and sum(g[2] for g in first if g[1] == "high") >= 6
    assert sum(g[2] for g in first if g[1] == "low") <= 4
    elapsed = world.clock.now() - T0
    assert sum(g[2] for g in grants) <= 10 + Fraction(10 * (elapsed // timedelta(seconds=1)), 3600)


def test_budget_exhaustion_defers_and_does_not_fail():  # H8
    both = days(1, 2)
    dataset = by_day_dataset("d", Source("src", rows_on(both, 1)))
    world = World(
        BudgetSpec("z", parse_limits(["1/hour"])),
        [fc.Flow("f", reads=dataset, deliver=[fc.to(MeteredSink("dst"), name="r")])],
        {"dst": "z"},
    )
    run = world.runtime.start_run("f", partitions=both)
    world.engine.run_until_idle()

    version_ids = [v.version_id for v in world.store.versions.list_versions("d")]
    assert len(version_ids) == 2
    assert all(world.store.ledger.get("r", v).status == "applied" for v in version_ids)
    assert world.store.runs.get_run(run).status == "succeeded"
    assert world.clock.now() - T0 >= timedelta(hours=1)


def test_a_connection_gets_a_handle_at_the_priority_of_its_work():
    clock = FrozenClock(T0)
    dataset = fc.Dataset("d", source=Source("src"), key="id", strategy=fc.strategy.replace(), priority="high")
    flow = fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("dst"), priority="low")])
    runtime = Runtime(
        [flow],
        MemoryStore(clock),
        MemorySpool(),
        LocalEngine(clock),
        clock=clock,
        budgets={"src": "z", "dst": "z"},
    )
    make = runtime._deps.make_context  # pyright: ignore[reportPrivateUsage]
    src, dst = make("src").budget, make("dst").budget
    assert src is not None and src.priority == "high"
    assert dst is not None and dst.priority == "low"
    assert make("elsewhere").budget is None


def test_a_budgeted_connection_cannot_work_at_two_priorities():
    dataset = fc.Dataset(
        "d", source=Source("shared"), key="id", strategy=fc.strategy.replace(), priority="high"
    )
    flow = fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("shared"), priority="low")])
    with pytest.raises(ValueError, match="two priorities"):
        Runtime([flow], MemoryStore(), MemorySpool(), LocalEngine(), budgets={"shared": "z"})


def test_an_extract_that_hits_the_budget_waits_durably_and_completes():
    both = days(1, 2)
    dataset = by_day_dataset("d", MeteredSource("src", rows_on(both, 1)))
    out = Sink("dst")
    world = World(
        BudgetSpec("z", parse_limits(["1/hour"])),
        [fc.Flow("f", reads=dataset, deliver=[fc.to(out, name="r")])],
        {"src": "z"},
    )
    run = world.runtime.start_run("f", partitions=both)
    world.engine.run_until_idle()

    assert world.store.runs.get_run(run).status == "succeeded" and len(out.rows) == 2
    assert world.clock.now() - T0 >= timedelta(hours=1)


ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


class DrySource(Source):
    """Reads fail with a budget deferral for the first `dry` reads."""

    def __init__(self, connection: str, rows: list[dict[str, Any]], dry: int) -> None:
        super().__init__(connection, rows)
        self.dry, self.reads = dry, 0

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        self.reads += 1
        if self.reads <= self.dry:
            raise BudgetDeferred(timedelta(hours=8), "daily")
        yield from super().read(ctx, req)


def test_a_deferred_extract_does_not_use_up_an_attempt():
    # More deferrals than the six attempts a task gets: waiting must not count as failing.
    dry = DrySource("src", ROWS, dry=7)
    clock = FrozenClock(T0)
    store, engine, out = MemoryStore(clock), LocalEngine(clock), Sink("dst")
    flow = fc.Flow(
        "f",
        reads=fc.Dataset("d", source=dry, key="id", strategy=fc.strategy.replace()),
        deliver=[fc.to(out, name="r")],
    )
    runtime = Runtime([flow], store, MemorySpool(), engine, clock=clock)
    engine.register(runtime.tasks())
    run = runtime.start_run("f")
    engine.run_until_idle()

    assert store.runs.get_run(run).status == "succeeded" and out.rows == ROWS
    assert dry.reads == 8 and clock.now() - T0 >= timedelta(hours=56)
    [extract] = [t for t in store.runs.list_tasks(run) if t.kind == "extract"]
    assert extract.status == "succeeded"


class PagedSource(Source):
    """Makes one metered call per row, as a source that reads many pages does."""

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        assert ctx.budget is not None
        for batch in super().read(ctx, req):
            for row in batch:
                ctx.budget.acquire()
                yield [row]


def paged_world(calls: int, limit: str, **kw: Any) -> World:
    reserve = kw.pop("reserve", {})
    dataset = fc.Dataset(
        "d", source=PagedSource("src", rows_of(calls)), key="id", strategy=fc.strategy.replace(), **kw
    )
    return World(
        BudgetSpec("z", parse_limits([limit]), reserve=reserve),
        [fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("dst"), name="r")])],
        {"src": "z"},
    )


def rows_of(calls: int) -> list[dict[str, Any]]:
    return [{"id": n} for n in range(calls)]


def test_a_read_that_needs_more_than_the_budget_holds_fails_with_the_reason():  # F12
    world = paged_world(60, "50/hour")
    run = world.runtime.start_run("f")
    world.engine.run_until_idle()

    found = world.store.runs.get_run(run)
    assert found.status == "failed"
    assert found.reason is not None and "budget 'z'" in found.reason and "holds 50" in found.reason
    assert sum(g[2] for g in world.spy.grants) == 50  # it stops, instead of spending the budget for good
    [extract] = [t for t in world.store.runs.list_tasks(run) if t.kind == "extract"]
    assert extract.status == "failed" and extract.error == found.reason


def test_a_read_that_fits_finishes_as_the_tokens_build_up():  # F12
    world = paged_world(40, "50/hour")
    assert world.store.budgets.acquire("z", 30).granted  # other work has used most of the window
    run = world.runtime.start_run("f")
    world.engine.run_until_idle()

    assert world.store.runs.get_run(run).status == "succeeded"
    assert world.clock.now() - T0 >= timedelta(minutes=20)  # what forty calls need to build up


def test_a_read_that_never_fits_the_share_left_to_it_still_ends():  # F12
    world = paged_world(30, "50/hour", priority="low", reserve={"high": 0.6})
    run = world.runtime.start_run("f")
    world.engine.run_until_idle()

    found = world.store.runs.get_run(run)
    assert found.status == "failed" and found.reason is not None and "budget 'z'" in found.reason
    assert world.clock.now() - T0 < timedelta(days=1)


def test_reads_competing_for_a_budget_only_wait():  # F12: being told to wait is not being too big
    both = days(1, 6)
    dataset = by_day_dataset("d", MeteredSource("src", rows_on(both, 1)))
    world = World(
        BudgetSpec("z", parse_limits(["1/hour"])),
        [fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("dst"), name="r")])],
        {"src": "z"},
    )
    run = world.runtime.start_run("f", partitions=both)
    world.engine.run_until_idle()
    assert world.store.runs.get_run(run).status == "succeeded"

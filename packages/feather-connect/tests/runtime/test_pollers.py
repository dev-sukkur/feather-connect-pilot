"""High-frequency pollers: an `every()` under five minutes ticks into one daily stream run (design 6.3 and 6.7)."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.triggers import Trigger, is_stream
from feather_connect.engine.local import LocalEngine
from feather_connect.engine.port import TaskCall
from feather_connect.runtime.runtime import FlowPaused, RunNotStarted, Runtime
from feather_connect.runtime.scheduler import TickResult, tick
from feather_connect.runtime.sweeper import sweep_once
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import InvalidTransition, Run, TaskRow, VersionDraft
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
TID = "every:30s"
HALF_MINUTE = timedelta(seconds=30)


def stamped(n: int) -> dict[str, Any]:
    return {"id": n, "at": (T0 + timedelta(seconds=n)).isoformat()}


class Flaky(Source):
    """A source that fails while `fail` says so."""

    fail = False

    def read(self, ctx: Context, req: Any) -> Any:
        if self.fail:
            raise PermanentError("the source said no")
        return super().read(ctx, req)


class World:
    def __init__(
        self,
        *triggers: Trigger,
        timezone: str = "UTC",
        strategy: Any = None,
        partition: Any = None,
        start: datetime = T0,
        **flow: Any,
    ) -> None:
        self.clock = FrozenClock(start)
        self.store = MemoryStore(self.clock)
        self.spool = MemorySpool(self.clock)
        self.engine = LocalEngine(self.clock)
        self.source = Flaky(rows=[stamped(0)])
        self.sink = Sink("a")
        dataset = fc.Dataset(
            "d",
            source=self.source,
            key="id",
            strategy=strategy or fc.strategy.by_stamp("at"),
            **({"partition": partition} if partition else {}),
        )
        self.flow = fc.Flow(
            "f",
            reads=dataset,
            deliver=[fc.to(self.sink)],
            triggers=triggers or [fc.every("30s")],
            **flow,
        )
        self.rt = Runtime(
            [self.flow], self.store, self.spool, self.engine, clock=self.clock, timezone=timezone
        )
        self.engine.register(self.rt.tasks())

    def tick(self) -> list[TickResult]:
        return tick(self.rt, now=self.clock.now())

    def step(self, delta: timedelta = HALF_MINUTE, *, drain: bool = True) -> list[TickResult]:
        self.clock.advance(delta)
        results = self.tick()
        if drain:
            self.engine.run_until_idle()
        return results

    def runs(self) -> list[Run]:
        return sorted(self.store.runs.list_runs(flow="f", limit=5000), key=lambda r: r.run_id)

    def ticks(self, run_id: str) -> list[TaskRow]:
        """The rows that record the ticks of the run, whether they went ahead or not."""
        return self.store.runs.list_tasks(run_id, kinds=("plan", "skip"))


def test_only_an_interval_under_five_minutes_is_stream_mode():
    assert is_stream(fc.every("30s")) and is_stream(fc.every("4m"))
    assert not is_stream(fc.every("5m")) and not is_stream(fc.every("15m"))
    assert not is_stream(fc.cron("* * * * *", tz="UTC")) and not is_stream(fc.manual())


def test_poller_uses_one_run_per_day():
    w = World()
    w.tick()
    for _ in range(4):
        w.step()
    [run] = w.runs()
    assert run.trigger == "stream" and run.params == {"day": "2026-10-18"}
    assert len(w.ticks(run.run_id)) == 4


def test_a_new_day_opens_a_new_stream_run():
    # 18:29 UTC is 23:59 in Kolkata, so the day ends at 18:30 UTC and not at midnight UTC.
    w = World(timezone="Asia/Kolkata", start=datetime(2026, 10, 18, 18, 29, tzinfo=UTC))
    w.tick()
    w.step()  # 18:29:30
    w.step()  # 18:30:00
    w.step()
    first, second = w.runs()
    assert first.params == {"day": "2026-10-18"} and second.params == {"day": "2026-10-19"}
    assert len(w.ticks(first.run_id)) == 1 and len(w.ticks(second.run_id)) == 2


def test_each_tick_is_recorded_as_a_task():
    w = World()
    w.tick()
    first = w.step()
    w.source.load([stamped(0), stamped(1)])
    second = w.step()
    assert first[0].run_id == second[0].run_id
    plans = w.ticks(first[0].run_id)
    assert [t.kind for t in plans] == ["plan", "plan"] and {t.status for t in plans} == {"succeeded"}
    kinds = [t.kind for t in w.store.runs.list_tasks(first[0].run_id)]
    assert kinds.count("extract") == 2 and kinds.count("apply") == 2  # each tick's own work is there too


def test_ticks_produce_distinct_versions():
    w = World()
    w.tick()
    w.step()
    w.source.load([stamped(0), stamped(1)])
    w.step()
    [run] = w.runs()
    versions = w.store.versions.sealed_for_run(run.run_id, "d")
    assert len(versions) == 2 and len({v.version_id for v in versions}) == 2
    assert [r["id"] for r in w.sink.rows] == [0, 1]


def test_a_tick_that_finds_nothing_new_makes_no_version():
    w = World(strategy=fc.strategy.snapshot_diff())
    w.tick()
    w.step()
    w.step()
    [run] = w.runs()
    assert len(w.store.versions.sealed_for_run(run.run_id, "d")) == 1
    assert run.status == "succeeded"


def test_a_tick_run_again_gets_the_versions_it_sealed_and_not_the_days():
    w = World()
    w.tick()
    w.step()
    [run] = w.runs()
    w.source.load([stamped(0), stamped(1)])
    w.step()
    versions = w.store.versions.sealed_for_run(run.run_id, "d")
    assert w.store.versions.sealed_for_run(run.run_id, "d", tick=versions[1].tick) == versions[1:]


def test_paused_tick_is_recorded_as_a_skipped_task():
    w = World()
    w.tick()
    w.step()
    w.rt.pause_flow("f", actor="op", reason="maintenance")
    [result] = w.step()
    assert (result.status, result.reason) == ("skipped", "paused")
    [run] = w.runs()
    assert result.run_id == run.run_id and run.status == "succeeded"
    skipped = w.ticks(run.run_id)[-1]
    assert (skipped.kind, skipped.status, skipped.error) == ("skip", "skipped", "paused")


def test_a_first_tick_that_is_paused_still_opens_the_days_run():
    w = World()
    w.tick()
    w.rt.pause_flow("f", actor="op")
    [result] = w.step()
    assert result.status == "skipped"
    assert [t.error for t in w.ticks(result.run_id)] == ["paused"]


def test_disabled_tick_is_recorded_as_a_skipped_task():
    w = World(enabled=False)
    w.tick()
    [result] = w.step()
    assert (result.status, result.reason) == ("skipped", "disabled")
    assert w.ticks(result.run_id)[0].error == "disabled"


def test_overlapping_tick_is_recorded_as_a_skipped_task():
    w = World()
    w.tick()
    [first] = w.step(drain=False)
    assert first.status == "running"
    [second] = w.step(drain=False)
    assert (second.status, second.reason) == ("skipped", "overlap")
    w.engine.run_until_idle()
    assert [t.status for t in w.ticks(first.run_id)] == ["succeeded", "skipped"]
    assert w.ticks(first.run_id)[1].error == "overlap"


def test_overlap_is_skip_whatever_the_flow_asks():
    w = World(overlap="cancel_previous")
    w.tick()
    w.step(drain=False)
    [second] = w.step(drain=False)
    assert second.reason == "overlap"
    w.engine.run_until_idle()
    assert w.store.runs.list_tasks(second.run_id, kinds=("plan",))[0].status == "succeeded"


def test_a_tick_skips_while_a_manual_run_reads_the_same_watermark():
    w = World()
    w.tick()
    w.rt.start_run("f")
    [result] = w.step(drain=False)
    assert result.reason == "overlap"


def test_open_deliveries_do_not_make_a_tick_overlap():
    w = World()
    w.tick()
    w.rt.pause_route("a", actor="op")
    w.step()  # its delivery waits, with its row running
    w.source.load([stamped(0), stamped(1)])
    [result] = w.step()
    assert result.status != "skipped"


def test_ticks_missed_during_downtime_collapse_into_one_skipped_task_and_one_tick():
    w = World(catch_up="each")
    w.tick()
    results = w.step(timedelta(minutes=5))
    assert [r.status for r in results] == ["skipped", "running"]
    assert results[0].reason == "catch_up_collapsed"
    rows = w.ticks(results[0].run_id)
    assert [t.status for t in rows] == ["skipped", "succeeded"]
    assert rows[0].error is not None and rows[0].error.startswith("catch_up_collapsed: 9 ticks")


def test_longer_interval_gets_a_run_per_tick():
    w = World(fc.every("5m"))
    w.tick()
    w.step(timedelta(minutes=5))
    w.step(timedelta(minutes=5))
    assert [r.trigger for r in w.runs()] == ["every:300s", "every:300s"]


def test_start_stream_tick_refuses_a_paused_flow_and_an_unknown_one():
    w = World()
    with pytest.raises(KeyError):
        w.rt.start_stream_tick("nothing", trigger_id=TID, at=T0)
    w.rt.pause_flow("f", actor="op")
    with pytest.raises(FlowPaused):
        w.rt.start_stream_tick("f", trigger_id=TID, at=T0)


def test_racing_replicas_share_the_days_run():
    w = World()
    other = Runtime([w.flow], w.store, w.spool, w.engine, clock=w.clock)
    assert w.rt.stream_run("f", T0) == other.stream_run("f", T0)
    assert len(w.runs()) == 1


def test_the_stream_run_of_a_store_method_is_one_per_flow_and_day():
    w = World()
    first = w.store.runs.stream_run("f", "2026-10-18")
    assert w.store.runs.stream_run("f", "2026-10-18") == first
    assert w.store.runs.stream_run("g", "2026-10-18") != first
    assert w.store.runs.stream_run("f", "2026-10-19") != first


# How a stream run stands, and what its actions do.


def test_a_stream_run_is_running_during_a_tick_and_settles_after_it():
    w = World()
    w.tick()
    [result] = w.step(drain=False)
    assert w.store.runs.get_run(result.run_id).status == "running"
    w.engine.run_until_idle()
    assert w.store.runs.get_run(result.run_id).status == "succeeded"


def test_a_failed_tick_shows_until_a_later_tick_gets_through():
    w = World()
    w.tick()
    w.source.fail = True
    [result] = w.step()
    run = w.store.runs.get_run(result.run_id)
    assert run.status == "failed" and "the source said no" in (run.reason or "")
    w.source.fail = False
    w.step()
    run = w.store.runs.get_run(result.run_id)
    assert run.status == "succeeded" and not run.reason


def test_a_failed_delivery_stays_failed_until_someone_acts():
    w = World()
    w.tick()
    w.sink.fail_next(PermanentError("no such table"))
    [result] = w.step()
    run_id = result.run_id
    assert w.store.runs.get_run(run_id).status == "failed"
    w.source.load([stamped(0), stamped(1)])
    w.step()  # its version waits behind the failed one, with a row running
    w.engine.run_until_idle()
    assert w.store.runs.get_run(run_id).status == "running"
    [failed] = w.store.ledger.list_deliveries(route="a", status="failed")
    w.rt.retry("a", failed.version_id, actor="op")
    w.engine.run_until_idle()
    assert w.store.runs.get_run(run_id).status == "succeeded"
    assert [r["id"] for r in w.sink.rows] == [0, 1]


def test_skipping_a_failed_delivery_settles_the_stream_run():
    w = World()
    w.tick()
    w.sink.fail_next(PermanentError("no such table"))
    [result] = w.step()
    [failed] = w.store.ledger.list_deliveries(route="a", status="failed")
    w.rt.skip("a", failed.version_id, actor="op")
    w.engine.run_until_idle()
    assert w.store.runs.get_run(result.run_id).status == "succeeded"


def test_replaying_the_latest_delivery_of_a_stream_run_applies_it_again():
    w = World()
    w.tick()
    [result] = w.step()
    [applied] = w.store.ledger.list_deliveries(route="a", status="applied")
    w.rt.replay("a", applied.version_id, actor="op")
    assert w.store.runs.get_run(result.run_id).status == "running"
    w.engine.run_until_idle()
    assert w.store.ledger.get("a", applied.version_id).replays == 1
    assert w.store.runs.get_run(result.run_id).status == "succeeded"


def test_a_stream_run_of_a_past_day_takes_no_more_ticks():
    w = World()
    w.tick()
    [today] = w.step()
    w.clock.advance(timedelta(days=1))
    w.tick()
    w.engine.run_until_idle()
    assert w.store.runs.get_run(today.run_id).status == "succeeded"
    assert len(w.ticks(today.run_id)) == 1


def test_cancelling_a_stream_run_is_refused_with_a_way_out():
    w = World()
    w.tick()
    [result] = w.step(drain=False)
    with pytest.raises(InvalidTransition, match="pause the flow"):
        w.rt.cancel_run(result.run_id, actor="op")
    assert w.store.runs.get_run(result.run_id).status == "running"


def test_a_sweep_does_not_discard_the_staged_versions_of_a_failed_stream_run():
    w = World()
    w.tick()
    w.source.fail = True
    [result] = w.step()
    w.source.fail = False
    assert w.store.runs.get_run(result.run_id).status == "failed"
    version = "v-in-flight"
    w.store.runs.start_task(result.run_id, "extract", "tick:in-flight:extract")  # its tick still reads
    w.store.versions.claim(version, "writer", result.run_id, "d", "_")
    w.store.versions.stage(fc_draft(version, result.run_id), "writer")
    report = sweep_once(w.store, w.rt, w.engine, now=w.clock.now())
    assert version not in report.discarded_versions


def test_a_sweep_discards_what_a_tick_left_staged_once_nothing_of_its_run_reads():  # S12
    w = World()
    w.tick()
    [result] = w.step()
    version = "v-left"
    w.store.versions.claim(version, "writer", result.run_id, "d", "_")
    w.store.versions.stage(fc_draft(version, result.run_id), "writer")
    report = sweep_once(w.store, w.rt, w.engine, now=w.clock.now())
    assert version in report.discarded_versions
    assert w.store.versions.staged_for_run(result.run_id, "d") == []


def test_a_sweep_settles_an_earlier_days_stream_run_left_running():  # S12
    w = World()
    w.tick()
    [yesterday] = w.step()
    w.store.runs.start_task(yesterday.run_id, "extract", "tick:stuck:extract")
    w.store.runs.set_run_status(yesterday.run_id, "running")
    w.clock.advance(timedelta(days=1))
    w.engine.has_ended = lambda ref: True  # type: ignore[method-assign]  # its task is gone
    sweep_once(w.store, w.rt, w.engine, now=w.clock.now())
    assert w.store.runs.get_run(yesterday.run_id).status != "running"
    assert w.store.runs.list_tasks(yesterday.run_id, status="running") == []


def fc_draft(version_id: str, run_id: str) -> VersionDraft:
    return VersionDraft(version_id, "d", "_", run_id, False, 1, 0, "h", None, None, "sha", tick="t")


def test_a_failed_tick_leaves_nothing_staged():
    w = World()
    w.tick()
    w.source.load([stamped(0)])
    real_seal = w.store.versions.seal

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise PermanentError("the database said no")

    w.store.versions.seal = fail  # type: ignore[method-assign]
    [result] = w.step()
    w.store.versions.seal = real_seal  # type: ignore[method-assign]
    assert w.store.runs.get_run(result.run_id).status == "failed"
    assert w.store.versions.staged_for_run(result.run_id, "d") == []


def test_a_plan_of_several_slices_is_refused_in_stream_mode():
    w = World(
        strategy=fc.strategy.replace(),
        partition=fc.partition.by_day("at", tz="UTC"),
    )
    w.tick()
    [result] = w.step()
    run = w.store.runs.get_run(result.run_id)
    assert run.status == "failed" and "by_stamp" in (run.reason or "")


# When a tick can't start, or was lost.


class Refusing(LocalEngine):
    refuse = False

    def submit(self, call: TaskCall) -> str:
        if self.refuse:
            raise RuntimeError("the engine is away")
        return super().submit(call)


def test_a_tick_that_cannot_be_submitted_fails_alone_and_the_next_one_goes_ahead():
    w = World()
    engine = Refusing(w.clock)
    rt = Runtime([w.flow], w.store, w.spool, engine, clock=w.clock)
    engine.register(rt.tasks())
    engine.refuse = True
    with pytest.raises(RunNotStarted) as raised:
        rt.start_stream_tick("f", trigger_id=TID, at=T0)
    [row] = w.ticks(raised.value.run_id)
    assert row.status == "failed" and "could not start" in (row.error or "")
    assert w.store.runs.get_run(raised.value.run_id).status == "failed"
    engine.refuse = False
    rt.start_stream_tick("f", trigger_id=TID, at=T0 + HALF_MINUTE)
    engine.run_until_idle()
    assert w.store.runs.get_run(raised.value.run_id).status == "succeeded"


def test_a_tick_lost_after_its_row_was_opened_does_not_block_the_ticks_for_good():
    w = World()
    run_id = w.rt.stream_run("f", T0)
    w.store.runs.start_task(run_id, "plan", f"tick:{run_id}:{TID}@{T0.isoformat()}:plan")
    w.tick()
    [first] = w.step(drain=False)
    assert first.reason == "overlap"  # too soon to tell
    w.clock.advance(timedelta(minutes=2))
    w.tick()  # submits the lost plan again under its own key
    w.engine.run_until_idle()
    assert w.store.runs.list_tasks(run_id, kinds=("plan",))[0].status == "succeeded"


def test_a_row_whose_task_ended_does_not_block_the_ticks_for_good():
    w = World()
    w.tick()
    [first] = w.step(drain=False)
    [plan] = w.store.runs.list_tasks(first.run_id, kinds=("plan",))
    w.engine.run_until_idle()
    # The engine knows the task ended, and its row still says running.
    w.store.runs._d.tasks[plan.task_id] = replace(plan, status="running", finished_at=None)  # type: ignore[attr-defined]
    w.source.load([stamped(0), stamped(1)])
    [result] = w.step()
    assert result.status != "skipped"
    row = w.store.runs.task_by_ref(first.run_id, plan.ref)
    assert row is not None and row.status == "failed"

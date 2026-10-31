from datetime import UTC, datetime, timedelta
from typing import Any

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.flow import CatchUp, Overlap
from feather_connect.core.triggers import Trigger
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.scheduler import TickResult, tick
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import Run
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
ROWS = [{"id": 1, "v": "a"}]
EVERY_15 = "every:900s"


class World:
    """One flow `f` with the given triggers, on a frozen clock."""

    def __init__(
        self,
        *triggers: Trigger,
        overlap: Overlap = "queue",
        catch_up: CatchUp = "latest",
        enabled: bool = True,
        start: datetime = T0,
        extra: tuple[fc.Flow, ...] = (),
    ) -> None:
        self.clock = FrozenClock(start)
        self.store = MemoryStore(self.clock)
        self.engine = LocalEngine(self.clock)
        self.sink = Sink("a")
        dataset = fc.Dataset("d", source=Source(rows=ROWS), key="id", strategy=fc.strategy.replace())
        flow = fc.Flow(
            "f",
            reads=dataset,
            deliver=[fc.to(self.sink)],
            triggers=triggers,
            overlap=overlap,
            catch_up=catch_up,
            enabled=enabled,
        )
        self.rt = Runtime(
            [flow, *extra], self.store, MemorySpool(self.clock), self.engine, code_sha="sha", clock=self.clock
        )
        self.engine.register(self.rt.tasks())

    def tick(self) -> list[TickResult]:
        return tick(self.rt, now=self.clock.now())

    def runs(self, flow: str = "f") -> list[Run]:
        return sorted(self.store.runs.list_runs(flow=flow, limit=5000), key=lambda r: r.run_id)

    def finish(self) -> None:
        self.engine.run_until_idle()

    def go(self, delta: timedelta) -> list[TickResult]:
        self.clock.advance(delta)
        return self.tick()


def test_cron_fires_in_the_flow_timezone() -> None:
    # 06:00 in Kolkata is 00:30 UTC.
    w = World(fc.cron("0 6 * * *", tz="Asia/Kolkata"), start=datetime(2026, 10, 18, 0, 0, tzinfo=UTC))
    assert w.tick() == []  # the first tick only notes the time
    assert w.go(timedelta(minutes=29)) == []  # 00:29 UTC
    due = w.go(timedelta(minutes=1))  # 00:30 UTC
    assert [(r.due_at, r.status) for r in due] == [(datetime(2026, 10, 18, 0, 30, tzinfo=UTC), "queued")]
    assert due[0].trigger_id == "cron:0 6 * * *:Asia/Kolkata"
    assert w.go(timedelta(hours=6)) == []  # not at 06:00 UTC, nor again the same day
    assert len(w.runs()) == 1


def test_first_tick_after_start_does_not_backfill() -> None:
    w = World(fc.every("15m"), start=T0)
    assert w.tick() == [] and w.runs() == []
    assert w.store.schedules.last_tick("f", EVERY_15) == T0


def test_a_run_started_by_a_tick_carries_the_trigger_and_actor_and_runs() -> None:
    w = World(fc.every("15m"))
    w.tick()
    (result,) = w.go(timedelta(minutes=15))
    run = w.store.runs.get_run(result.run_id)
    assert (run.trigger, run.actor, run.flow) == (EVERY_15, "scheduler", "f")
    w.finish()
    assert w.store.runs.get_run(result.run_id).status == "succeeded" and w.sink.rows == ROWS


def test_overlap_skip_records_a_skipped_run() -> None:
    w = World(fc.every("15m"), overlap="skip")
    w.tick()
    first = w.go(timedelta(minutes=15))[0]  # not run to the end, so it stays active
    (second,) = w.go(timedelta(minutes=15))
    assert (second.status, second.reason) == ("skipped", "overlap")
    run = w.store.runs.get_run(second.run_id)
    assert run.status == "skipped" and run.reason == "overlap"
    w.finish()
    assert w.store.runs.get_run(first.run_id).status == "succeeded"
    assert w.go(timedelta(minutes=15))[0].status == "queued"  # nothing is active any more


def test_overlap_queue_starts_anyway() -> None:
    w = World(fc.every("15m"), overlap="queue")
    w.tick()
    w.go(timedelta(minutes=15))
    (second,) = w.go(timedelta(minutes=15))
    assert second.status == "queued" and len(w.rt.active_runs("f")) == 2
    w.finish()
    assert {r.status for r in w.runs()} == {"succeeded"}


def test_overlap_cancel_previous_cancels_the_active_run() -> None:
    w = World(fc.every("15m"), overlap="cancel_previous")
    w.tick()
    (first,) = w.go(timedelta(minutes=15))
    (second,) = w.go(timedelta(minutes=15))
    assert second.status == "queued"
    assert w.store.runs.get_run(first.run_id).status == "cancelled"
    assert "cancel_previous" in (w.store.runs.get_run(first.run_id).reason or "")
    w.finish()
    assert w.store.runs.get_run(first.run_id).status == "cancelled"  # finishing the rest doesn't undo it
    assert w.store.runs.get_run(second.run_id).status == "succeeded"
    assert w.sink.rows == ROWS


def test_catch_up_latest_collapses_missed_ticks() -> None:  # Review Focus 1
    w = World(fc.every("15m"), catch_up="latest")
    w.tick()
    results = w.go(timedelta(days=1))
    w.finish()
    runs = w.runs()
    assert sum(r.status in ("queued", "succeeded") for r in runs) == 1
    assert [r.reason for r in runs if r.status == "skipped"] == ["catch_up_collapsed"]
    skipped = next(r for r in runs if r.status == "skipped")
    assert skipped.params["collapsed"] == 95
    assert [r.status for r in results].count("queued") == 1
    started = next(r for r in results if r.status == "queued")
    assert started.due_at == T0 + timedelta(days=1)


def test_catch_up_each_runs_every_missed_tick() -> None:
    w = World(fc.every("15m"), catch_up="each")
    w.tick()
    results = w.go(timedelta(hours=2))
    assert [r.status for r in results] == ["queued"] * 8
    assert [r.due_at for r in results] == [T0 + timedelta(minutes=15 * n) for n in range(1, 9)]
    w.finish()
    assert {r.status for r in w.runs()} == {"succeeded"}


def test_catch_up_each_is_capped_at_1000_and_the_cap_is_recorded() -> None:
    w = World(fc.every("1m"), catch_up="each")
    w.tick()
    results = w.go(timedelta(minutes=1500))
    assert [r.status for r in results].count("queued") == 1000
    assert results[-1].due_at == T0 + timedelta(minutes=1500)
    skipped = [r for r in w.runs() if r.status == "skipped"]
    assert [r.reason for r in skipped] == ["catch_up_collapsed"] and skipped[0].params["collapsed"] == 500


def test_restart_does_not_double_fire() -> None:
    w = World(fc.every("15m"))
    w.tick()
    assert len(w.go(timedelta(minutes=15))) == 1
    assert w.tick() == []  # a second tick at the same instant
    again = Runtime([w.rt.flow("f")], w.store, MemorySpool(), w.engine, code_sha="sha", clock=w.clock)
    assert tick(again, now=w.clock.now()) == []  # a restarted server has only the store to go by
    assert len(w.runs()) == 1


def test_every_interval_fires_on_schedule() -> None:
    w = World(fc.every("30s"))
    w.tick()
    fired: list[datetime] = []
    for _ in range(10):
        fired += [r.due_at for r in w.go(timedelta(seconds=10))]
    assert fired == [T0 + timedelta(seconds=30 * n) for n in (1, 2, 3)]


def test_cron_fires_once_a_day_over_both_dst_changes() -> None:
    zone = "America/New_York"
    for expr, start, days in (
        ("30 2 * * *", datetime(2026, 3, 6, 12, tzinfo=UTC), 5),  # 02:30 does not exist on 8 March
        ("30 1 * * *", datetime(2026, 10, 30, 12, tzinfo=UTC), 5),  # 01:30 happens twice on 1 November
    ):
        w = World(fc.cron(expr, tz=zone), start=start)
        w.tick()
        results: list[TickResult] = []
        for _ in range(days * 24):
            results += w.go(timedelta(hours=1))
        assert len(results) == days, expr
        assert len({r.due_at.astimezone(UTC).date() for r in results}) == days, expr


def test_disabled_flow_records_a_skip() -> None:
    w = World(fc.every("15m"), enabled=False)
    w.tick()
    (result,) = w.go(timedelta(minutes=15))
    assert (result.status, result.reason) == ("skipped", "disabled")
    assert w.store.runs.get_run(result.run_id).status == "skipped"
    w.finish()
    assert w.sink.rows == []


def test_a_failing_flow_does_not_stop_other_flows_ticking() -> None:
    dataset = fc.Dataset("d2", source=Source(rows=ROWS), key="id", strategy=fc.strategy.replace())
    broken = fc.Flow(
        "broken", reads=dataset, deliver=[fc.to(Sink("b"))], triggers=[fc.cron("not a cron", tz="UTC")]
    )
    w = World(fc.every("15m"), extra=(broken,))
    w.tick()
    results = w.go(timedelta(minutes=15))
    assert [(r.flow, r.status) for r in results] == [("f", "queued")]


def test_a_run_that_cannot_start_is_recorded_as_failed() -> None:
    w = World(fc.every("15m"))
    w.tick()

    def refuse(call: Any) -> str:
        raise RuntimeError("engine unreachable")

    w.engine.submit = refuse  # type: ignore[method-assign]
    (result,) = w.go(timedelta(minutes=15))
    run = w.store.runs.get_run(result.run_id)
    assert result.status == "failed" and "engine unreachable" in (run.reason or "")


def test_manual_and_event_triggers_are_left_alone() -> None:
    w = World(fc.manual())
    w.tick()
    assert w.go(timedelta(days=1)) == [] and w.runs() == []


def test_last_tick_is_kept_per_trigger() -> None:
    w = World(fc.every("15m"), fc.cron("0 * * * *", tz="UTC"))
    w.tick()
    results = w.go(timedelta(hours=1))
    assert {r.trigger_id for r in results} == {EVERY_15, "cron:0 * * * *:UTC"}

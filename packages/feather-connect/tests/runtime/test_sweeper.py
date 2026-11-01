from datetime import UTC, datetime, timedelta
from typing import Any

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.errors import PermanentError
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.sweeper import sweep_once
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
DAY_17, DAY_18 = "date=2026-10-17", "date=2026-10-18"
ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


class Harness:
    def __init__(self, reads_kw: dict[str, Any] | None = None, rows: Any = ROWS) -> None:
        self.clock = FrozenClock(T0)
        self.store = MemoryStore(self.clock)
        self.engine = LocalEngine(self.clock)
        self.a, self.b = Sink("a"), Sink("b")
        kw: dict[str, Any] = {"strategy": fc.strategy.replace(), **(reads_kw or {})}
        dataset = fc.Dataset("d", source=Source(rows=rows), key="id", **kw)
        flow = fc.Flow("f", reads=dataset, deliver=[fc.to(self.a), fc.to(self.b)])
        self.rt = Runtime(
            [flow], self.store, MemorySpool(self.clock), self.engine, code_sha="sha", clock=self.clock
        )
        self.engine.register(self.rt.tasks())

    def sweep(self, **kw: Any):
        return sweep_once(self.store, self.rt, self.engine, now=self.clock.now(), **kw)

    def crashed_run(self, step: str) -> str:
        """Start a run whose task crashes just before `step`, and leave it crashed."""
        self.engine.crash_before(step)
        run = self.rt.start_run("f")
        self.engine.run_until_idle()
        return run

    def status(self, route: str, version_id: str) -> str:
        return self.store.ledger.get(route, version_id).status


def two_days() -> dict[str, Any]:
    days = [{"id": n, "date": d.removeprefix("date=")} for n, d in enumerate((DAY_17, DAY_18))]
    return {
        "reads_kw": {
            "strategy": fc.strategy.snapshot_diff(),
            "partition": fc.partition.by_day("date", tz="UTC"),
        },
        "rows": days,
    }


def test_dead_worker_is_marked_and_recovered() -> None:
    h = Harness()
    run = h.crashed_run("extract")
    h.store.workers.heartbeat("w1", {}, "sha", 1)
    h.clock.advance(timedelta(seconds=31))
    report = h.sweep()
    assert report.dead_workers == ["w1"]
    assert report.recovered_tasks == h.engine.task_ids()[1:2]
    assert h.store.workers.list_workers()[0].state == "dead"
    h.engine.run_until_idle()
    assert h.store.runs.get_run(run).status == "succeeded"
    assert h.sweep().dead_workers == []  # it is dead already


def test_live_worker_is_left_alone() -> None:
    h = Harness()
    h.crashed_run("extract")
    h.store.workers.heartbeat("w1", {}, "sha", 1)
    h.clock.advance(timedelta(seconds=29))
    report = h.sweep()
    assert report.dead_workers == [] and report.recovered_tasks == []
    assert h.store.workers.list_workers()[0].state == "alive"
    assert h.engine.status(h.engine.task_ids()[1]) == "crashed"


def test_a_draining_worker_that_stops_beating_is_recovered_too() -> None:
    h = Harness()
    h.store.workers.heartbeat("w1", {}, "sha", 1)
    h.store.workers.mark("w1", "draining")
    h.clock.advance(timedelta(minutes=5))
    assert h.sweep().dead_workers == ["w1"]


def test_worker_that_returns_is_marked_alive_again() -> None:
    h = Harness()
    h.store.workers.heartbeat("w1", {}, "sha", 1)
    h.clock.advance(timedelta(minutes=1))
    assert h.sweep().dead_workers == ["w1"]
    h.store.workers.heartbeat("w1", {}, "sha", 1)
    assert h.store.workers.list_workers()[0].state == "alive"
    assert h.sweep().dead_workers == []


def test_pending_delivery_with_no_active_task_is_resubmitted() -> None:  # the outbox
    h = Harness()
    run = h.crashed_run("submit_applies")  # sealed, and crashed before the applies were submitted
    version = h.store.versions.sealed_for_run(run, "d")[0].version_id
    assert h.status("a", version) == "pending" and h.sweep().resubmitted_deliveries == []  # too soon
    h.clock.advance(timedelta(minutes=1, seconds=1))
    report = h.sweep()
    assert report.resubmitted_deliveries == [("a", version), ("b", version)]
    h.engine.run_until_idle()
    assert h.status("a", version) == h.status("b", version) == "applied"
    assert h.a.rows == h.b.rows == ROWS
    assert h.sweep().resubmitted_deliveries == []  # nothing is pending now


def test_resubmit_is_safe_when_a_task_exists() -> None:  # the same idempotency key, so one execution
    h = Harness()
    run = h.crashed_run("submit_applies")
    version = h.store.versions.sealed_for_run(run, "d")[0].version_id
    h.rt.submit_apply("a", version)  # submitted, and not yet run
    h.clock.advance(timedelta(minutes=2))
    assert len(h.sweep().resubmitted_deliveries) == 2
    h.sweep()
    h.engine.run_until_idle()
    assert len(h.a.calls) == len(h.b.calls) == 1
    applies = [c for c in h.engine.submitted() if c.task == "feather.apply"]
    assert len(applies) == 2
    assert len([t for t in h.store.runs.list_tasks(run) if t.kind == "apply"]) == 2


def test_run_ready_to_seal_is_resubmitted() -> None:
    h = Harness(**two_days())
    run = h.crashed_run("submit_seal")  # every slice is read, and nobody asked for the seal
    assert h.store.versions.sealed_for_run(run, "d") == []
    report = h.sweep()
    assert report.resubmitted_seals == [(run, "d")]
    h.engine.run_until_idle()
    assert len(h.store.versions.sealed_for_run(run, "d")) == 2
    assert h.sweep().resubmitted_seals == []


def test_blocked_deliveries_are_not_resubmitted() -> None:  # failed and held stay as they are
    h = Harness()
    run = h.crashed_run("submit_applies")
    version = h.store.versions.sealed_for_run(run, "d")[0].version_id
    h.store.ledger.transition("a", version, "running", actor="t")
    h.store.ledger.transition("a", version, "failed", actor="t")
    h.store.ledger.transition("b", version, "running", actor="t")
    h.store.ledger.transition("b", version, "held", actor="t")
    h.clock.advance(timedelta(hours=1))
    assert h.sweep().resubmitted_deliveries == []
    h.engine.run_until_idle()
    assert (h.status("a", version), h.status("b", version)) == ("failed", "held")


def test_one_delivery_that_cannot_be_submitted_does_not_stop_the_sweep() -> None:
    h = Harness()
    run = h.crashed_run("submit_applies")
    version = h.store.versions.sealed_for_run(run, "d")[0].version_id

    class Flaky(Runtime):
        def submit_apply(self, route: str, version_id: str) -> None:
            if route == "a":
                raise RuntimeError("engine unreachable")
            super().submit_apply(route, version_id)

    flaky = Flaky(list(h.rt._flows.values()), h.store, MemorySpool(), h.engine, code_sha="sha", clock=h.clock)  # pyright: ignore[reportPrivateUsage]
    h.clock.advance(timedelta(minutes=2))
    report = sweep_once(h.store, flaky, h.engine, now=h.clock.now())
    assert report.resubmitted_deliveries == [("b", version)]
    h.sweep()  # the next sweep tries "a" again
    h.engine.run_until_idle()
    assert h.status("a", version) == h.status("b", version) == "applied"


def test_a_cancelled_run_is_not_sealed() -> None:
    h = Harness(**two_days())
    run = h.crashed_run("submit_seal")
    h.rt.cancel_run(run, actor="op", reason="not wanted")
    assert h.sweep().resubmitted_seals == []
    h.engine.run_until_idle()
    assert h.store.versions.sealed_for_run(run, "d") == []


class FailingRecovery(LocalEngine):
    """An engine whose recovery fails while `broken` is set, as it may just after a stall."""

    broken = True

    def recover_worker(self, worker_id: str) -> list[str]:
        if self.broken:
            raise TimeoutError("the pool timed out")
        return super().recover_worker(worker_id)


def test_a_failed_recovery_leaves_the_worker_to_the_next_sweep() -> None:  # F5
    h = Harness()
    engine = FailingRecovery(h.clock)
    h.engine = engine
    h.rt = Runtime(h.rt.flows(), h.store, MemorySpool(h.clock), engine, code_sha="sha", clock=h.clock)
    engine.register(h.rt.tasks())
    run = h.crashed_run("extract")
    h.store.workers.heartbeat("w1", {}, "sha", 1)
    h.clock.advance(timedelta(seconds=31))

    first = h.sweep()
    assert first.dead_workers == [] and first.recovered_tasks == []
    assert h.store.workers.list_workers()[0].state == "alive"

    engine.broken = False
    second = h.sweep()
    assert second.dead_workers == ["w1"] and len(second.recovered_tasks) == 1
    assert h.store.workers.list_workers()[0].state == "dead"
    engine.run_until_idle()
    assert h.store.runs.get_run(run).status == "succeeded"


def test_a_queued_run_that_lost_its_submit_is_submitted_again() -> None:  # F6
    h = Harness()
    run = h.store.runs.create_run("f", "manual", "op", {})  # a server killed before the submit left this
    assert h.sweep().resubmitted_runs == []  # too young to tell
    h.clock.advance(timedelta(minutes=2))
    assert h.sweep().resubmitted_runs == [run]
    h.engine.run_until_idle()
    assert h.store.runs.get_run(run).status == "succeeded"
    assert h.sweep().resubmitted_runs == []


def test_a_queued_run_of_a_paused_flow_waits_for_the_pause_to_end() -> None:  # F6
    h = Harness()
    run = h.store.runs.create_run("f", "manual", "op", {})
    h.rt.pause_flow("f", actor="op")
    h.clock.advance(timedelta(minutes=2))
    assert h.sweep().resubmitted_runs == []
    h.rt.resume_flow("f", actor="op")
    assert h.sweep().resubmitted_runs == [run]


def stranded(h: Harness, step: str = "end") -> tuple[str, str]:
    """Run until a delivery to `a` is left open with its task ended: the step that closes it raises for good."""
    real = h.rt.tasks()["feather.apply"]

    def failing(ctx: Any, args: Any) -> Any:
        def broken_step(name: str, fn: Any) -> Any:
            if name.startswith(f"{step}:") and args["route"] == "a":
                raise PermanentError("the step broke")
            return original(name, fn)

        original = ctx.step
        ctx.step = broken_step
        return real(ctx, args)

    h.engine.register({**h.rt.tasks(), "feather.apply": failing})
    run = h.rt.start_run("f")
    h.engine.run_until_idle()
    h.engine.register(h.rt.tasks())
    return run, h.store.versions.sealed_for_run(run, "d")[0].version_id


def test_a_task_that_ends_with_its_delivery_open_fails_the_delivery() -> None:  # F7
    h = Harness()
    _, version = stranded(h, "end")
    delivery = h.store.ledger.get("a", version)
    assert delivery.status == "failed" and "the step broke" in (delivery.last_error or "")
    h.rt.retry("a", version, actor="op")
    h.engine.run_until_idle()
    assert h.status("a", version) == "applied"


def test_the_sweeper_fails_a_running_delivery_whose_task_has_ended() -> None:  # F7
    h = Harness()
    run, version = stranded_for_sweeper(h, "running")
    assert h.status("a", version) == "running"
    report = h.sweep()
    assert report.stranded_deliveries == [("a", version)]
    delivery = h.store.ledger.get("a", version)
    assert delivery.status == "failed" and "ended" in (delivery.last_error or "")
    assert h.store.runs.get_run(run).status == "failed"
    h.rt.skip("a", version, actor="op")  # the dashboard can move it now
    assert h.status("a", version) == "skipped"


def test_the_sweeper_fails_a_pending_delivery_whose_task_has_ended() -> None:  # F7
    h = Harness()
    _, version = stranded_for_sweeper(h, "pending")
    assert h.status("a", version) == "pending"
    h.clock.advance(timedelta(minutes=2))
    assert h.sweep().stranded_deliveries == [("a", version)]
    assert h.status("a", version) == "failed"
    h.rt.retry("a", version, actor="op")
    h.engine.run_until_idle()
    assert h.status("a", version) == "applied"


def test_the_sweeper_leaves_a_delivery_whose_task_is_alive() -> None:  # F7
    h = Harness()
    h.engine.crash_before("target")  # the task of the first delivery is crashed, so it has not ended
    run = h.rt.start_run("f")
    h.engine.run_until_idle()
    open_ = [d for d in h.store.ledger.list_deliveries() if d.status not in ("applied",)]
    h.clock.advance(timedelta(minutes=2))
    assert open_ and h.sweep().stranded_deliveries == []
    assert h.store.runs.get_run(run).status == "running"


def stranded_for_sweeper(h: Harness, status: str) -> tuple[str, str]:
    """Leave a delivery to `a` in `status` with its task ended and its journal row still open."""
    real = h.rt.tasks()["feather.apply"]

    def dies(ctx: Any, args: Any) -> Any:
        if args["route"] != "a":
            return real(ctx, args)
        if status == "running":
            h.store.ledger.transition("a", args["version_id"], "running", actor="system")
        raise _Vanished

    h.engine.register({**h.rt.tasks(), "feather.apply": dies})
    run = h.rt.start_run("f")
    h.engine.run_until_idle()
    h.engine.register(h.rt.tasks())
    return run, h.store.versions.sealed_for_run(run, "d")[0].version_id


class _Vanished(PermanentError):
    """A task that ended without a word to the journal, as when the database is away for its last attempt."""

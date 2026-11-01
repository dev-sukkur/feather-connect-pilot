from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import FlowPaused, Runtime
from feather_connect.runtime.scheduler import tick
from feather_connect.runtime.sweeper import sweep_once
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


class Harness:
    def __init__(self) -> None:
        self.clock = FrozenClock(T0)
        self.store = MemoryStore(self.clock)
        self.ledger = self.store.ledger
        self.engine = LocalEngine(self.clock)
        self.a, self.b = Sink("a"), Sink("b")
        self.source = Source(rows=ROWS)
        dataset = fc.Dataset("d", source=self.source, key="id", strategy=fc.strategy.replace())
        flow = fc.Flow("f", reads=dataset, deliver=[fc.to(self.a), fc.to(self.b)], triggers=[fc.every("15m")])
        self.rt = Runtime(
            [flow], self.store, MemorySpool(self.clock), self.engine, code_sha="sha", clock=self.clock
        )
        self.engine.register(self.rt.tasks())

    def run(self, **kw: Any) -> str:
        run = self.rt.start_run("f", **kw)
        self.engine.run_until_idle()
        return run

    def version_of(self, run: str) -> str:
        return self.store.versions.sealed_for_run(run, "d")[0].version_id

    def status(self, route: str, version_id: str) -> str:
        return self.ledger.get(route, version_id).status

    def run_status(self, run: str) -> str:
        return self.store.runs.get_run(run).status


def test_paused_flow_records_skipped_ticks() -> None:
    h = Harness()
    tick(h.rt, now=h.clock.now())
    h.rt.pause_flow("f", actor="op", reason="vendor maintenance")
    h.clock.advance(timedelta(minutes=15))
    (result,) = tick(h.rt, now=h.clock.now())
    assert (result.status, result.reason) == ("skipped", "paused")
    assert h.store.runs.get_run(result.run_id).reason == "paused"
    h.rt.resume_flow("f", actor="op")
    h.clock.advance(timedelta(minutes=15))
    assert [r.status for r in tick(h.rt, now=h.clock.now())] == ["queued"]


def test_paused_route_holds_deliveries_while_others_continue() -> None:
    h = Harness()
    h.rt.pause_route("a", actor="op", reason="vendor window")
    run = h.run()
    v1 = h.version_of(run)
    assert h.status("a", v1) == "pending" and h.status("b", v1) == "applied"
    assert h.a.rows == [] and h.b.rows == ROWS
    assert h.run_status(run) == "running"  # the held delivery still belongs to the run


def test_extraction_continues_while_a_route_is_paused() -> None:  # I5
    h = Harness()
    h.rt.pause_route("a", actor="op")
    h.rt.pause_route("b", actor="op")
    run = h.run()
    rows = h.store.runs.list_tasks(run)
    assert {r.kind: r.status for r in rows if r.kind != "apply"} == {
        "plan": "succeeded",
        "extract": "succeeded",
    }
    assert h.store.versions.sealed_for_run(run, "d")  # the data is read and sealed, and waits
    assert h.a.rows == h.b.rows == []


def test_resume_route_applies_the_backlog_in_order() -> None:
    h = Harness()
    h.rt.pause_route("a", actor="op")
    first = h.run()
    h.source.load([{"id": 1, "v": "changed"}, {"id": 2, "v": "b"}])
    second = h.run()
    v1, v2 = h.version_of(first), h.version_of(second)
    assert h.status("a", v1) == h.status("a", v2) == "pending"
    h.rt.resume_route("a", actor="op")
    h.engine.run_until_idle()
    assert h.status("a", v1) == h.status("a", v2) == "applied"
    assert [call.version_id for call in h.a.calls] == [v1, v2]  # seal order (I2)
    assert h.a.rows == h.b.rows
    assert h.run_status(first) == h.run_status(second) == "succeeded"


def test_a_task_already_submitted_when_the_route_is_paused_leaves_the_delivery_alone() -> None:
    h = Harness()
    run = h.rt.start_run("f")
    h.engine.crash_before("submit_applies")
    h.engine.run_until_idle()  # sealed, with the applies not yet submitted
    v1 = h.version_of(run)
    h.rt.submit_apply("a", v1)
    h.rt.pause_route("a", actor="op")
    h.engine.run_until_idle()  # the task of "a" runs, finds the pause and ends
    assert h.status("a", v1) == "pending" and h.a.rows == []
    assert h.ledger.attempts("a", v1) == []  # untouched: no attempt, no transition
    rows = [t for t in h.store.runs.list_tasks(run) if t.route == "a"]
    assert [t.status for t in rows] == ["cancelled", "running"]  # the pause ended one, and another waits
    h.rt.resume_route("a", actor="op")
    h.engine.run_until_idle()
    assert h.status("a", v1) == "applied" and h.a.rows == ROWS


def test_resuming_twice_or_a_route_that_was_not_paused_changes_nothing() -> None:
    h = Harness()
    run = h.run()
    h.rt.resume_route("a", actor="op")
    h.rt.pause_route("a", actor="op")
    h.rt.resume_route("a", actor="op")
    h.rt.resume_route("a", actor="op")
    h.engine.run_until_idle()
    assert h.status("a", h.version_of(run)) == "applied" and len(h.a.calls) == 1


def test_expired_pause_lifts_by_itself() -> None:
    h = Harness()
    h.rt.pause_route("a", actor="op", reason="one hour", expires_at=T0 + timedelta(hours=1))
    run = h.run()
    v1 = h.version_of(run)
    assert h.status("a", v1) == "pending"
    assert sweep_once(h.store, h.rt, h.engine, now=h.clock.now()).resubmitted_deliveries == []  # still paused
    h.clock.advance(timedelta(hours=2))
    assert not h.rt.is_route_paused("a")
    report = sweep_once(h.store, h.rt, h.engine, now=h.clock.now())
    assert report.resubmitted_deliveries == [("a", v1)]
    h.engine.run_until_idle()
    assert h.status("a", v1) == "applied" and h.run_status(run) == "succeeded"


def test_manual_run_of_a_paused_flow_is_refused_with_a_reason() -> None:
    h = Harness()
    h.rt.pause_flow("f", actor="op", reason="vendor maintenance")
    with pytest.raises(FlowPaused, match="vendor maintenance"):
        h.rt.start_run("f", actor="ann")
    assert h.store.runs.list_runs() == []
    h.rt.resume_flow("f", actor="op")
    assert h.run_status(h.rt.start_run("f")) == "queued"


def test_pausing_something_that_does_not_exist_is_refused() -> None:
    h = Harness()
    with pytest.raises(KeyError):
        h.rt.pause_flow("nope", actor="op")
    with pytest.raises(KeyError):
        h.rt.pause_route("nope", actor="op")


def test_a_pause_is_visible_to_whoever_lists_overrides() -> None:
    h = Harness()
    h.rt.pause_route("a", actor="op", reason="window", expires_at=T0 + timedelta(hours=1))
    h.rt.pause_flow("f", actor="op")
    found = {(o.target, o.key, o.value, o.reason, o.created_by) for o in h.store.overrides.list_all()}
    assert found == {("route:a", "paused", True, "window", "op"), ("flow:f", "paused", True, None, "op")}


def test_a_route_resumed_just_before_the_task_ends_loses_nothing() -> None:  # F7
    h = Harness()
    real = h.rt.tasks()["feather.apply"]

    def paused_then_resumed(ctx: Any, args: Any) -> Any:
        original = ctx.step

        def step(name: str, fn: Any) -> Any:
            if args["route"] == "a" and name == "paused:0" and not ctx.task_ref.endswith("#1"):
                h.rt.pause_route("a", actor="op")
            if args["route"] == "a" and name == "finish" and h.rt.is_route_paused("a"):
                h.store.overrides.clear("route:a", "paused", actor="op")
            return original(name, fn)

        ctx.step = step
        return real(ctx, args)

    h.engine.register({**h.rt.tasks(), "feather.apply": paused_then_resumed})
    run = h.run()
    version = h.version_of(run)
    assert h.status("a", version) == "applied"  # no key was used up, and nothing is left stuck
    assert h.run_status(run) == "succeeded"

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.engine.local import LocalEngine
from feather_connect.engine.port import RetryPolicy
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import InvalidTransition
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
ALLOWED_ARGS = {"run_id", "flow", "dataset", "partition", "route", "version_id"}
DAY_16, DAY_17, DAY_18 = "date=2026-10-16", "date=2026-10-17", "date=2026-10-18"


class Harness:
    def __init__(
        self, reads_kw: dict[str, Any] | None = None, *, source: Any = None, rows: Any = ROWS
    ) -> None:
        self.clock = FrozenClock(T0)
        self.store = MemoryStore(self.clock)
        self.ledger = self.store.ledger
        self.engine = LocalEngine(self.clock)
        self.a, self.b = Sink("a"), Sink("b")
        self.source: Any = Source(rows=rows) if source is None else source
        kw: dict[str, Any] = {"strategy": fc.strategy.replace(), **(reads_kw or {})}
        self.dataset = fc.Dataset("d", source=self.source, key="id", **kw)
        flow = fc.Flow("f", reads=self.dataset, deliver=[fc.to(self.a), fc.to(self.b)])
        self.rt = Runtime([flow], self.store, MemorySpool(), self.engine, code_sha="sha", clock=self.clock)
        self.engine.register(self.rt.tasks())

    def run(self, **kw: Any) -> str:
        run = self.rt.start_run("f", **kw)
        self.engine.run_until_idle()
        return run

    def status(self, run: str) -> str:
        return self.store.runs.get_run(run).status

    def reason(self, run: str) -> str | None:
        return self.store.runs.get_run(run).reason

    def tasks_named(self, name: str) -> list[Any]:
        return [call for call in self.engine.submitted() if call.task == name]

    def version_of(self, run: str, partition: str = "_") -> str:
        return next(
            v.version_id for v in self.store.versions.sealed_for_run(run, "d") if v.partition == partition
        )

    def delivery(self, route: str, version_id: str) -> str:
        return self.ledger.get(route, version_id).status


def by_day(**kw: Any) -> dict[str, Any]:
    return {"strategy": fc.strategy.snapshot_diff(), "partition": fc.partition.by_day("date", tz="UTC"), **kw}


def on(*days: str) -> list[dict[str, Any]]:
    return [{"id": n, "date": day.removeprefix("date=")} for n, day in enumerate(days)]


def test_a_run_fans_out_to_all_routes() -> None:
    h = Harness()
    run = h.run()
    assert h.a.rows == h.b.rows == ROWS
    assert h.status(run) == "succeeded"
    assert [call.task for call in h.engine.submitted()] == [
        "feather.plan",
        "feather.extract",
        "feather.apply",
        "feather.apply",
    ]


def test_task_args_carry_ids_only() -> None:  # I8
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    h.run()
    for call in h.engine.submitted():
        assert set(call.args) <= ALLOWED_ARGS
        assert len(json.dumps(call.args)) < 64_000


def test_the_keys_of_tasks_follow_the_design() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    run = h.run()
    keys = {(call.task, call.concurrency_key) for call in h.engine.submitted()}
    assert keys == {
        ("feather.plan", "plan:default:f"),
        ("feather.extract", f"extract:default:d:{DAY_17}"),
        ("feather.extract", f"extract:default:d:{DAY_18}"),
        ("feather.seal_run", "seal:default:d"),
        ("feather.apply", f"apply:default:a:{DAY_17}"),
        ("feather.apply", f"apply:default:a:{DAY_18}"),
        ("feather.apply", f"apply:default:b:{DAY_17}"),
        ("feather.apply", f"apply:default:b:{DAY_18}"),
    }
    version = h.version_of(run, DAY_17)
    assert f"apply:a:{version}:0:0" in {call.idempotency_key for call in h.engine.submitted()}


def test_explicit_partitions_override_the_plan() -> None:
    planner_calls: list[datetime] = []

    def planner(now: datetime) -> list[str]:
        planner_calls.append(now)
        return [DAY_18]

    h = Harness(by_day(planner=planner), rows=on(DAY_16, DAY_17, DAY_18))
    run = h.run(partitions=[DAY_16])
    assert [call.args["partition"] for call in h.tasks_named("feather.extract")] == [DAY_16]
    assert [row["date"] for row in h.a.rows] == ["2026-10-16"]
    assert planner_calls == []
    assert h.status(run) == "succeeded"


def test_default_plan_uses_the_scheme_recent_window() -> None:
    h = Harness(by_day(), rows=on(DAY_16, DAY_17, DAY_18))
    h.run()
    assert [call.args["partition"] for call in h.tasks_named("feather.extract")] == [DAY_17, DAY_18]
    assert sorted(row["date"] for row in h.a.rows) == ["2026-10-17", "2026-10-18"]


def test_custom_planner_is_used() -> None:
    seen: list[datetime] = []

    def planner(now: datetime) -> list[str]:
        seen.append(now)
        return [DAY_16]

    h = Harness(by_day(planner=planner), rows=on(DAY_16, DAY_17, DAY_18))
    h.run()
    assert seen == [T0]
    assert [row["date"] for row in h.a.rows] == ["2026-10-16"]


def test_a_scheme_that_cannot_plan_fails_the_run_with_a_hint() -> None:
    h = Harness({"strategy": fc.strategy.replace(), "partition": fc.partition.by_fields("store")})
    run = h.run()
    assert h.status(run) == "failed"
    assert "planner" in (h.reason(run) or "")
    assert h.tasks_named("feather.extract") == []


def test_by_stamp_plans_one_extract_with_partition_star() -> None:
    rows = [{**row, "at": "2026-10-18T09:00:00+00:00"} for row in on(DAY_17, DAY_18)]
    h = Harness(by_day(strategy=fc.strategy.by_stamp("at")), rows=rows)
    run = h.run(partitions=[DAY_17, DAY_18])
    assert [call.args["partition"] for call in h.tasks_named("feather.extract")] == ["*"]
    assert h.tasks_named("feather.seal_run") == []
    assert {v.partition for v in h.store.versions.sealed_for_run(run, "d")} == {DAY_17, DAY_18}
    assert len(h.a.rows) == 2
    assert h.status(run) == "succeeded"


def test_multi_slice_run_seals_after_the_last_slice() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    run = h.run()
    assert len(h.tasks_named("feather.seal_run")) == 1
    order = [call.task for call in h.engine.submitted()]
    assert order.index("feather.seal_run") > order.index("feather.extract")
    assert len(h.store.versions.sealed_for_run(run, "d")) == 2
    assert h.store.versions.staged_for_run(run, "d") == []
    assert h.status(run) == "succeeded"


def test_a_slice_with_nothing_new_still_counts_towards_the_seal() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    h.run()
    h.source.load([*on(DAY_17), {"id": 1, "date": "2026-10-18", "v": "new"}])
    run = h.run()
    assert [v.partition for v in h.store.versions.sealed_for_run(run, "d")] == [DAY_18]
    assert h.status(run) == "succeeded"


def test_single_slice_run_seals_without_seal_run() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    run = h.run(partitions=[DAY_17])
    assert h.tasks_named("feather.seal_run") == []
    assert h.store.versions.slices_for_run(run, "d") == []
    assert len(h.store.versions.sealed_for_run(run, "d")) == 1


def test_a_run_with_nothing_new_succeeds_without_apply_tasks() -> None:
    h = Harness({"strategy": fc.strategy.snapshot_diff()})
    h.run()
    before = len(h.tasks_named("feather.apply"))
    run = h.run()
    assert len(h.tasks_named("feather.apply")) == before
    assert h.status(run) == "succeeded"


def test_every_task_records_its_code_sha() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    run = h.run()
    rows = h.store.runs.list_tasks(run)
    assert {row.kind for row in rows} == {"plan", "extract", "seal_run", "apply"}
    assert {row.code_sha for row in rows} == {"sha"}
    assert {row.status for row in rows} == {"succeeded"}
    assert len(rows) == len(h.engine.submitted())


class RaisingSource(Source):
    def __init__(self, error: Exception) -> None:
        super().__init__(rows=ROWS)
        self.error = error
        self.reads = 0

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        self.reads += 1
        raise self.error


def test_failed_extract_fails_the_run_with_the_reason() -> None:
    h = Harness(source=RaisingSource(PermanentError("the table is gone")))
    run = h.run()
    assert h.status(run) == "failed"
    assert "the table is gone" in (h.reason(run) or "")
    assert h.source.reads == 1


def test_an_extract_that_cannot_produce_a_version_fails_at_once() -> None:
    h = Harness({"on_record_error": "fail"}, rows=[{"v": "no key"}])
    run = h.run()
    assert h.status(run) == "failed"
    assert "on_record_error" in (h.reason(run) or "")
    assert h.engine.clock.now() == T0  # with no retry delay


def test_an_extract_that_keeps_failing_fails_the_run_on_its_last_attempt() -> None:
    h = Harness(source=RaisingSource(TransientError("timeout")))
    run = h.run()
    assert h.source.reads == RetryPolicy().max_attempts
    assert h.status(run) == "failed"
    assert "timeout" in (h.reason(run) or "")


def test_a_failed_slice_never_seals_and_fails_the_run() -> None:
    class HalfBroken(Source):
        def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
            if req.partition == DAY_18:
                raise PermanentError("day 18 is broken")
            return super().read(ctx, req)

    h = Harness(by_day(), source=HalfBroken(rows=on(DAY_17, DAY_18)))
    run = h.run()
    assert h.tasks_named("feather.seal_run") == []
    assert h.a.rows == []
    assert h.status(run) == "failed"
    assert "day 18 is broken" in (h.reason(run) or "")


def test_an_empty_read_stopped_at_the_seal_fails_the_run() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    h.run()
    h.source.load([])
    run = h.run()
    assert h.status(run) == "failed"
    assert "allow_empty" in (h.reason(run) or "")
    assert len(h.a.rows) == 2


def test_replay_applies_an_applied_version_again() -> None:  # FR-B8
    h = Harness()
    run = h.run()
    v1 = h.version_of(run)
    h.rt.replay("a", v1, actor="op", reason="fix")
    assert h.status(run) == "running"
    h.engine.run_until_idle()
    assert [c.version_id for c in h.a.calls].count(v1) == 2
    attempts = h.ledger.attempts("a", v1)
    assert attempts[-1].to_status == "applied"
    assert (attempts[-3].actor, attempts[-3].reason) == ("op", "fix")
    assert [c.version_id for c in h.b.calls].count(v1) == 1
    assert h.status(run) == "succeeded"


def test_a_failed_delivery_fails_the_run_while_the_other_route_delivers() -> None:
    h = Harness()
    h.a.fail_next(PermanentError("no such table"))
    run = h.run()
    v1 = h.version_of(run)
    assert h.delivery("a", v1) == "failed"
    assert h.delivery("b", v1) == "applied"
    assert h.status(run) == "failed"
    assert "no such table" in (h.reason(run) or "")


def test_retry_and_skip_unblock_a_failed_version() -> None:
    # Retry: the same version goes through, as a new task, and so does the one that waited behind it.
    h = Harness()
    h.a.fail_next(PermanentError("no such table"))
    run1 = h.run()
    v1 = h.version_of(run1)
    h.source.load([{"id": 1, "v": "new"}])
    run2 = h.run()
    v2 = h.version_of(run2)
    assert (h.delivery("a", v2), h.status(run2)) == ("pending", "running")
    assert (h.delivery("b", v2), h.a.rows) == ("applied", [])

    h.rt.retry("a", v1, actor="op", reason="table is back")
    h.engine.run_until_idle()
    assert (h.delivery("a", v1), h.delivery("a", v2)) == ("applied", "applied")
    assert h.a.rows == [{"id": 1, "v": "new"}]
    assert (h.status(run1), h.status(run2)) == ("succeeded", "succeeded")
    assert h.ledger.get("a", v1).retries == 1

    # Skip: the version stays undelivered, and the one behind it goes through.
    h = Harness()
    h.a.fail_next(PermanentError("no such table"))
    run1 = h.run()
    v1 = h.version_of(run1)
    h.source.load([{"id": 1, "v": "new"}])
    run2 = h.run()
    v2 = h.version_of(run2)

    h.rt.skip("a", v1, actor="op", reason="not needed")
    h.engine.run_until_idle()
    assert (h.delivery("a", v1), h.delivery("a", v2)) == ("skipped", "applied")
    assert h.a.rows == [{"id": 1, "v": "new"}]
    assert v1 not in [c.version_id for c in h.a.calls]
    assert (h.status(run1), h.status(run2)) == ("succeeded", "succeeded")


def test_a_retry_that_fails_again_can_be_retried_again() -> None:
    h = Harness()
    h.a.fail_next(PermanentError("one"))
    run = h.run()
    v1 = h.version_of(run)
    h.a.fail_next(PermanentError("two"))
    h.rt.retry("a", v1, actor="op")
    h.engine.run_until_idle()
    assert h.status(run) == "failed"
    assert "two" in (h.reason(run) or "")
    h.rt.retry("a", v1, actor="op")
    h.engine.run_until_idle()
    assert h.delivery("a", v1) == "applied"
    assert h.status(run) == "succeeded"


def test_only_a_new_state_makes_a_new_apply_task() -> None:
    h = Harness()
    run = h.run()
    v1 = h.version_of(run)
    before = len(h.tasks_named("feather.apply"))
    h.rt.submit_apply("a", v1)
    assert len(h.tasks_named("feather.apply")) == before
    h.rt.replay("a", v1, actor="op")
    assert len(h.tasks_named("feather.apply")) == before + 1
    assert h.engine.submitted()[-1].idempotency_key == f"apply:a:{v1}:1:0"
    assert h.engine.submitted()[-1].retry == RetryPolicy()


def test_retry_and_replay_take_only_their_own_status() -> None:
    h = Harness()
    v1 = h.version_of(h.run())
    with pytest.raises(InvalidTransition, match="retry"):
        h.rt.retry("a", v1, actor="op")
    h.a.fail_next(PermanentError("bad"))
    h.rt.replay("a", v1, actor="op")
    h.engine.run_until_idle()
    with pytest.raises(InvalidTransition, match="replay"):
        h.rt.replay("a", v1, actor="op")
    assert h.delivery("a", v1) == "failed"


def test_a_crash_before_any_step_loses_and_repeats_nothing() -> None:
    for step in (
        "plan",
        "extract",
        "seal",
        "submit_seal",
        "submit_applies",
        "finish_slice",
        "target",
        "finish",
    ):
        h = Harness(by_day(), rows=on(DAY_17, DAY_18))
        h.engine.crash_before(step)
        run = h.rt.start_run("f")
        h.engine.run_until_idle()
        h.engine.recover()
        h.engine.run_until_idle()
        assert h.status(run) == "succeeded", step
        assert len(h.a.rows) == 2, step
        assert len(h.a.calls) == 2, step
        assert len(h.store.runs.list_tasks(run)) == len(h.engine.submitted()), step


def test_a_second_run_on_another_day_does_not_wait_for_a_blocked_day() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    h.a.fail_next(PermanentError("bad"))
    run = h.run(partitions=[DAY_17])
    other = h.run(partitions=[DAY_18])
    assert h.status(run) == "failed"
    assert h.status(other) == "succeeded"


def test_the_clock_gives_a_default_context() -> None:
    seen: list[Context] = []

    class Watching(Source):
        def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
            seen.append(ctx)
            return super().read(ctx, req)

    h = Harness(source=Watching(rows=ROWS))
    h.clock.advance(timedelta(hours=1))
    h.run()
    assert seen[0].now == T0 + timedelta(hours=1)
    assert seen[0].config == {}


def test_lookups_name_what_is_missing() -> None:
    h = Harness()
    assert h.rt.flow("f").name == "f"
    assert h.rt.dataset("d") is h.dataset
    assert h.rt.route("a").sink is h.a
    with pytest.raises(KeyError, match="nope"):
        h.rt.flow("nope")
    with pytest.raises(KeyError, match="nope"):
        h.rt.dataset("nope")
    with pytest.raises(KeyError, match="nope"):
        h.rt.route("nope")


def test_two_flows_cannot_share_a_route_name() -> None:
    def flow(name: str, dataset: str) -> fc.Flow:
        return fc.Flow(
            name,
            reads=fc.Dataset(dataset, source=Source(), key="id", strategy=fc.strategy.replace()),
            deliver=[fc.to(Sink("a"))],
        )

    with pytest.raises(ValueError, match="route"):
        Runtime([flow("f", "d"), flow("g", "e")], MemoryStore(), MemorySpool(), LocalEngine())


def two_versions(*, fail_first: bool = False) -> tuple[Harness, str, str, str, str]:
    h = Harness()
    if fail_first:
        h.a.fail_next(PermanentError("bad"))
    run1 = h.run()
    h.source.load([{"id": 1, "v": "new"}])
    run2 = h.run()
    return h, run1, run2, h.version_of(run1), h.version_of(run2)


def test_replay_waits_for_a_newer_open_delivery() -> None:
    h, _, _, v1, v2 = two_versions()
    h.rt.replay("a", v2, actor="op")  # the newest may replay, and now it is open
    with pytest.raises(InvalidTransition, match=v2):
        h.rt.replay("a", v1, actor="op")
    assert h.delivery("a", v1) == "applied"
    h.engine.run_until_idle()

    # Drained, the older one replays, and a failure of it strands nothing.
    h.a.fail_next(PermanentError("boom"))
    h.rt.replay("a", v1, actor="op")
    h.engine.run_until_idle()
    assert (h.delivery("a", v1), h.delivery("a", v2)) == ("failed", "applied")
    h.rt.retry("a", v1, actor="op")
    h.engine.run_until_idle()
    assert (h.delivery("a", v1), h.delivery("a", v2)) == ("applied", "applied")


def test_replay_ignores_an_open_delivery_of_another_partition_or_route() -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    h.a.fail_next(PermanentError("bad"))
    run17 = h.run(partitions=[DAY_17])
    h.run(partitions=[DAY_18])
    v18 = h.version_of(h.store.runs.list_runs(limit=1)[0].run_id, DAY_18)
    h.rt.replay("a", v18, actor="op")
    h.rt.replay("b", h.version_of(run17, DAY_17), actor="op")
    h.engine.run_until_idle()
    assert h.delivery("a", v18) == "applied"


def test_submit_apply_keeps_the_key_of_a_delivery_that_is_not_next() -> None:
    h, _, run2, v1, v2 = two_versions(fail_first=True)
    before = len(h.tasks_named("feather.apply"))
    rows = len(h.store.runs.list_tasks(run2))
    h.rt.submit_apply("a", v2)  # as a sweeper would
    assert len(h.tasks_named("feather.apply")) == before
    assert len(h.store.runs.list_tasks(run2)) == rows
    h.engine.run_until_idle()
    assert h.delivery("a", v2) == "pending"

    h.rt.retry("a", v1, actor="op")
    h.engine.run_until_idle()
    assert (h.delivery("a", v1), h.delivery("a", v2)) == ("applied", "applied")
    assert h.status(run2) == "succeeded"


@pytest.mark.parametrize(
    ("step", "missing"),
    [("plan", "flow"), ("extract", "flow"), ("seal", "flow"), ("target", "route")],
)
def test_a_task_whose_flow_a_deploy_removed_fails_its_row(step: str, missing: str) -> None:
    h = Harness(by_day(), rows=on(DAY_17, DAY_18))
    h.engine.crash_before(step)
    run = h.rt.start_run("f")
    h.engine.run_until_idle()
    deployed = Runtime([], h.store, MemorySpool(), h.engine, code_sha="new", clock=h.clock)
    h.engine.register(deployed.tasks())
    h.engine.recover()
    h.engine.run_until_idle()
    assert {row.status for row in h.store.runs.list_tasks(run)} <= {"succeeded", "failed"}
    assert h.status(run) == "failed"
    assert f"no {missing} is named" in (h.reason(run) or "")


def test_a_connector_is_given_the_connections_config_and_the_time_zone():
    seen: list[Any] = []

    class Spy(Source):
        def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
            seen.append((ctx.config["token"], ctx.tz))
            yield from super().read(ctx, req)

    h = Harness(source=Spy(rows=ROWS))
    h.rt = Runtime(
        [h.rt.flow("f")],
        h.store,
        MemorySpool(),
        h.engine,
        clock=h.clock,
        config_of=lambda connection: {"token": f"for {connection}"},
        timezone="Asia/Kolkata",
    )
    h.engine.register(h.rt.tasks())
    h.run()
    assert seen == [(f"for {h.source.connection}", "Asia/Kolkata")]

from datetime import date, timedelta
from typing import Any

import pytest

from feather_connect.core.connector import CheckResult, ConnectorSpec
from feather_connect.core.errors import PermanentError
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import FlowPaused
from feather_server.auth import Actor
from feather_server.services.actions import DeliverySelection
from feather_server.services.previews import PreviewExpired, PreviewInvalid, PreviewStale, PreviewUsed
from feather_server.testing import SECRET, World, day

ANN = Actor("human", "ann")
BOB = Actor("token", "bob-ci")
DAYS_17_18 = [day(17), day(18)]


def deliveries(world: World, route: str, status: str) -> list[str]:
    return sorted(
        d.partition
        for d in world.store.ledger.list_deliveries(route=route, status=status)  # type: ignore[arg-type]
    )


def fail_two_warehouse_deliveries(world: World) -> None:
    for _ in range(2):
        world.sink("warehouse").fail_next(PermanentError("no such table"))
    world.run()


def test_run_flow_starts_a_manual_run_for_the_actor(world: World) -> None:
    run_id = world.actions.run_flow(ANN, "orders", reason="month end")
    world.engine.run_until_idle()
    run = world.store.runs.get_run(run_id)
    assert (run.trigger, run.actor, run.reason, run.status) == ("manual", "ann", "month end", "succeeded")
    world.state.runtime.pause_flow("orders", actor="op", reason="maintenance")
    with pytest.raises(FlowPaused):
        world.actions.run_flow(ANN, "orders")
    with pytest.raises(KeyError):
        world.actions.run_flow(ANN, "nothing")


def test_rerun_preview_shows_impact_and_changes_nothing(world: World) -> None:
    world.run()
    runs_before, audit_before = len(world.store.runs.list_runs()), len(world.store.audit.list_audit())
    submitted_before = len(world.engine.submitted())
    preview = world.actions.preview_rerun(ANN, "orders", DAYS_17_18)
    assert preview.action == "rerun" and preview.token
    impact = preview.impact
    assert (impact.partitions, impact.deliveries) == (2, 4)  # two partitions to two routes
    assert impact.estimated_budget_units == 8  # 4 rows read, 2 units each
    assert impact.samples == ("17 Oct 2026", "18 Oct 2026")
    assert len(world.store.runs.list_runs()) == runs_before
    assert len(world.store.audit.list_audit()) == audit_before
    assert len(world.engine.submitted()) == submitted_before


def test_rerun_preview_cannot_cost_what_is_not_known(world: World) -> None:
    preview = world.actions.preview_rerun(ANN, "orders", [day(12)])  # never read: its size is not known
    assert preview.impact.estimated_budget_units is None
    with pytest.raises(KeyError):
        world.actions.preview_rerun(ANN, "no-such-dataset", [day(12)])
    with pytest.raises(ValueError, match="partition"):
        world.actions.preview_rerun(ANN, "orders", [])
    with pytest.raises(ValueError, match="partition"):
        world.actions.preview_rerun(ANN, "orders", ["not-a-partition"])


def test_rerun_with_the_token_runs_those_partitions(world: World) -> None:
    preview = world.actions.preview_rerun(ANN, "orders", [day(17)])
    run_id = world.actions.rerun(ANN, preview.token, reason="source was fixed")
    world.engine.run_until_idle()
    run = world.store.runs.get_run(run_id)
    assert (run.trigger, run.actor, run.reason, run.status) == (
        "rerun",
        "ann",
        "source was fixed",
        "succeeded",
    )
    assert run.params["partitions"] == [day(17)]
    assert [v.partition for v in world.store.versions.sealed_for_run(run_id, "orders")] == [day(17)]


def test_token_is_single_use_and_expires(world: World) -> None:  # Review Focus 1
    preview = world.actions.preview_rerun(ANN, "orders", [day(17)])
    world.actions.rerun(ANN, preview.token)
    with pytest.raises(PreviewUsed):
        world.actions.rerun(ANN, preview.token)
    late = world.actions.preview_rerun(ANN, "orders", [day(17)])
    world.clock.advance(timedelta(minutes=9))
    assert world.actions.rerun(ANN, late.token)  # still good at nine minutes
    stale = world.actions.preview_rerun(ANN, "orders", [day(17)])
    world.clock.advance(timedelta(minutes=10, seconds=1))
    with pytest.raises(PreviewExpired):
        world.actions.rerun(ANN, stale.token)


def test_a_token_is_for_its_own_action_and_cannot_be_forged(world: World) -> None:
    preview = world.actions.preview_rerun(ANN, "orders", [day(17)])
    with pytest.raises(PreviewInvalid):
        world.actions.backfill(ANN, preview.token)  # a rerun token does not start a backfill
    with pytest.raises(PreviewInvalid):
        world.actions.apply_deliveries(ANN, preview.token)
    body, _, signature = preview.token.partition(".")
    for forged in (f"{body}.{signature[:-2]}xx", f"{signature}.{body}", "", "garbage", f"{body}."):
        with pytest.raises(PreviewInvalid):
            world.actions.rerun(ANN, forged)
    assert world.actions.rerun(ANN, preview.token)  # the real one was never spent by the attempts above


def test_changed_selection_refuses_the_token(world: World) -> None:  # Review Focus 1
    fail_two_warehouse_deliveries(world)
    preview = world.actions.preview_deliveries(ANN, "retry", DeliverySelection(route="warehouse"))
    assert preview.impact.deliveries == 2
    # A delivery in the selection moves on after the preview.
    first = world.store.ledger.list_deliveries(route="warehouse", status="failed")[0]
    world.state.runtime.skip("warehouse", first.version_id, actor="someone-else")
    with pytest.raises(PreviewStale):
        world.actions.apply_deliveries(ANN, preview.token)
    assert deliveries(world, "warehouse", "failed") != []  # nothing was retried
    again = world.actions.preview_deliveries(ANN, "retry", DeliverySelection(route="warehouse"))
    assert again.impact.deliveries == 1  # a fresh preview sees the new state


def test_a_backfill_token_starts_the_backfill_it_was_made_for(world: World) -> None:
    preview = world.actions.preview_backfill(ANN, "orders", start=date(2026, 10, 12), end=date(2026, 10, 18))
    other = world.actions.preview_backfill(ANN, "orders", start=date(2026, 10, 12), end=date(2026, 10, 14))
    assert preview.token != other.token
    run_id = world.actions.backfill(ANN, other.token)
    assert world.store.runs.get_run(run_id).params["partitions"] == [day(12), day(13), day(14)]


def test_bulk_retry_skip_and_replay_affect_only_the_selection(world: World) -> None:
    fail_two_warehouse_deliveries(world)
    assert deliveries(world, "warehouse", "failed") == [day(17), day(18)]

    retry = world.actions.preview_deliveries(
        ANN, "retry", DeliverySelection(route="warehouse", partition=day(17))
    )
    assert retry.impact.deliveries == 1 and retry.impact.partitions == 1
    done = world.actions.apply_deliveries(ANN, retry.token, reason="table is back")
    world.engine.run_until_idle()
    assert len(done) == 1
    assert deliveries(world, "warehouse", "applied") == [day(17)]
    assert deliveries(world, "warehouse", "failed") == [day(18)]  # the other one was left alone

    skip = world.actions.preview_deliveries(ANN, "skip", DeliverySelection(route="warehouse"))
    world.actions.apply_deliveries(ANN, skip.token, reason="not needed")
    assert deliveries(world, "warehouse", "skipped") == [day(18)]

    replay = world.actions.preview_deliveries(ANN, "replay", DeliverySelection(route="gofrugal"))
    assert replay.impact.deliveries == 2
    world.actions.apply_deliveries(ANN, replay.token)
    world.engine.run_until_idle()
    assert [d.replays for d in world.store.ledger.list_deliveries(route="gofrugal")] == [1, 1]
    assert [d.replays for d in world.store.ledger.list_deliveries(route="warehouse")] == [0, 0]


def test_replay_of_a_named_older_version_is_refused_at_the_preview(world: World) -> None:
    world.run([day(17)])
    older = world.store.versions.latest_sealed("orders", day(17))
    assert older is not None
    source: Any = world.state.runtime.dataset("orders").source
    source.load([{**dict(row), "note": "changed"} for row in source._rows])
    world.run([day(17)])
    newer = world.store.versions.latest_sealed("orders", day(17))
    assert newer is not None and newer.version_id != older.version_id
    with pytest.raises(ValueError, match=f"older.*replay {newer.version_id}"):
        world.actions.preview_deliveries(
            ANN, "replay", DeliverySelection(route="gofrugal", version_ids=(older.version_id,))
        )


def test_a_selection_by_error_takes_one_failure_group(world: World) -> None:
    world.sink("warehouse").fail_next(PermanentError("no such table"))
    world.sink("warehouse").fail_next(PermanentError("quota exceeded"))
    world.run()
    preview = world.actions.preview_deliveries(
        ANN, "retry", DeliverySelection(route="warehouse", error="quota exceeded")
    )
    assert preview.impact.deliveries == 1
    world.actions.apply_deliveries(ANN, preview.token)
    world.engine.run_until_idle()
    assert [d.last_error for d in world.store.ledger.list_deliveries(route="warehouse", status="failed")] == [
        "no such table"
    ]


def test_a_selection_must_be_small_enough_to_review(world: World) -> None:
    with pytest.raises(ValueError, match="nothing"):
        world.actions.preview_deliveries(ANN, "retry", DeliverySelection(route="warehouse"))
    with pytest.raises(ValueError, match="action"):
        world.actions.preview_deliveries(ANN, "delete", DeliverySelection(route="warehouse"))  # type: ignore[arg-type]


def test_every_action_writes_an_audit_row_with_actor_and_reason(world: World) -> None:
    fail_two_warehouse_deliveries(world)
    actions, runtime = world.actions, world.state.runtime
    run_id = actions.run_flow(ANN, "orders", reason="r-run")
    rerun = actions.preview_rerun(ANN, "orders", [day(17)])
    actions.rerun(BOB, rerun.token, reason="r-rerun")
    backfill = actions.preview_backfill(ANN, "orders", start=date(2026, 10, 10), end=date(2026, 10, 11))
    actions.backfill(ANN, backfill.token, reason="r-backfill")
    actions.cancel_run(ANN, run_id, reason="r-cancel")
    retry = actions.preview_deliveries(ANN, "retry", DeliverySelection(route="warehouse", partition=day(17)))
    actions.apply_deliveries(ANN, retry.token, reason="r-apply")
    actions.pause_flow(ANN, "orders", reason="r-pause-flow")
    actions.resume_flow(ANN, "orders", reason="r-resume-flow")
    actions.pause_route(ANN, "gofrugal", reason="r-pause-route")
    actions.resume_route(ANN, "gofrugal", reason="r-resume-route")
    actions.check_connection(ANN, "warehouse")
    world.engine.run_until_idle()
    rows = list(reversed(world.store.audit.list_audit()))
    assert [r.action for r in rows] == [
        "run_flow",
        "rerun",
        "backfill",
        "cancel_run",
        "retry_deliveries",
        "pause_flow",
        "resume_flow",
        "pause_route",
        "resume_route",
        "check_connection",
    ]
    assert [(r.actor_type, r.actor) for r in rows][:2] == [("human", "ann"), ("token", "bob-ci")]
    reasons = [r.request.get("reason") for r in rows]
    assert reasons == [
        "r-run",
        "r-rerun",
        "r-backfill",
        "r-cancel",
        "r-apply",
        "r-pause-flow",
        "r-resume-flow",
        "r-pause-route",
        "r-resume-route",
        None,
    ]
    assert rows[0].target == "flow:orders" and rows[4].target == "route:warehouse"
    assert all(r.result.startswith("ok") for r in rows)
    assert runtime.is_flow_paused("orders") is False and runtime.is_route_paused("gofrugal") is False
    assert SECRET not in repr(rows)


def test_a_refused_action_is_audited_as_failed(world: World) -> None:
    world.state.runtime.pause_flow("orders", actor="op")
    with pytest.raises(FlowPaused):
        world.actions.run_flow(ANN, "orders", reason="try")
    (row,) = world.store.audit.list_audit()
    assert row.action == "run_flow" and row.result.startswith("failed:") and "paused" in row.result


def test_backfill_limits_concurrent_slices(world: World) -> None:  # FR-D4
    engine: LocalEngine = world.engine
    preview = world.actions.preview_backfill(
        ANN, "orders", start=date(2026, 10, 12), end=date(2026, 10, 18), max_concurrent=2
    )
    assert preview.impact.partitions == 7
    run_id = world.actions.backfill(ANN, preview.token, reason="catch up")
    live = {"queued", "running", "parked"}
    most = 0
    while True:
        try:
            engine.run_until_idle(max_tasks=1)
            break
        except RuntimeError:  # one task ran and more are ready
            calls = [c for c in engine.submitted() if c.task == "feather.extract"]
            alive = sum(engine.status(c.idempotency_key) in live for c in calls)
            most = max(most, alive)
            assert alive <= 2
    assert most == 2
    assert world.store.runs.get_run(run_id).status == "succeeded"
    assert len(world.store.versions.sealed_for_run(run_id, "orders")) == 7


def test_backfill_preview_estimates_budget_units(world: World) -> None:
    world.run([day(17), day(18)])
    known = world.actions.preview_backfill(ANN, "orders", start=date(2026, 10, 17), end=date(2026, 10, 18))
    assert known.impact.estimated_budget_units == 8 and known.impact.deliveries == 4
    unknown = world.actions.preview_backfill(ANN, "orders", start=date(2026, 10, 10), end=date(2026, 10, 18))
    assert unknown.impact.estimated_budget_units is None and unknown.impact.partitions == 9
    assert unknown.impact.samples[0] == "10 Oct 2026"


def test_backfill_refuses_what_it_cannot_do(world: World) -> None:
    with pytest.raises(ValueError, match="before"):
        world.actions.preview_backfill(ANN, "orders", start=date(2026, 10, 18), end=date(2026, 10, 1))
    with pytest.raises(ValueError, match="at least 1"):
        world.actions.preview_backfill(
            ANN, "orders", start=date(2026, 10, 1), end=date(2026, 10, 2), max_concurrent=0
        )
    with pytest.raises(ValueError, match="days"):
        world.actions.preview_backfill(ANN, "orders", start=date(2020, 1, 1), end=date(2026, 10, 2))


def test_cancel_stops_queued_tasks_and_records_the_reason(world: World) -> None:
    preview = world.actions.preview_backfill(
        ANN, "orders", start=date(2026, 10, 12), end=date(2026, 10, 18), max_concurrent=1
    )
    run_id = world.actions.backfill(ANN, preview.token)
    world.actions.cancel_run(BOB, run_id, reason="wrong dates")
    world.engine.run_until_idle()
    run = world.store.runs.get_run(run_id)
    assert (
        run.status == "cancelled" and "wrong dates" in (run.reason or "") and "bob-ci" in (run.reason or "")
    )
    assert {t.status for t in world.store.runs.list_tasks(run_id)} == {"cancelled"}
    assert world.store.versions.sealed_for_run(run_id, "orders") == []
    (row,) = [r for r in world.store.audit.list_audit() if r.action == "cancel_run"]
    assert row.target == f"run:{run_id}" and row.request["reason"] == "wrong dates"


def test_dismiss_quarantine_closes_the_entry(world: World) -> None:
    run_id = world.run()
    version = world.store.versions.sealed_for_run(run_id, "orders")[0]
    world.store.quarantine.add(
        "warehouse", version.version_id, "k1", "bad value", "orders", version.partition
    )
    world.store.quarantine.add(
        "warehouse", version.version_id, "k2", "bad value", "orders", version.partition
    )
    world.actions.dismiss_quarantine(ANN, "warehouse", version.version_id, "k1", reason="known issue")
    assert [q.key for q in world.store.quarantine.list_open("warehouse")] == ["k2"]
    with pytest.raises(LookupError):
        world.actions.dismiss_quarantine(ANN, "warehouse", version.version_id, "k1")
    rows = [r for r in world.store.audit.list_audit() if r.action == "dismiss_quarantine"]
    assert [r.result.startswith("ok") for r in rows] == [False, True]  # the refusal is on record too


def test_pause_and_resume_go_through_the_runtime(world: World) -> None:
    world.actions.pause_route(ANN, "gofrugal", reason="vendor window")
    assert world.state.runtime.is_route_paused("gofrugal")
    world.actions.resume_route(ANN, "gofrugal")
    assert not world.state.runtime.is_route_paused("gofrugal")
    world.actions.pause_flow(ANN, "orders")
    assert world.state.runtime.is_flow_paused("orders")
    with pytest.raises(KeyError):
        world.actions.pause_flow(ANN, "nothing")


def test_check_connection_reports_what_it_really_checked(world: World) -> None:
    result = world.actions.check_connection(ANN, "warehouse")
    assert result.ok and "nothing was checked" in result.message  # a simulated connection has no check
    with pytest.raises(KeyError):
        world.actions.check_connection(ANN, "nowhere")


def test_check_connection_uses_the_connectors_own_check(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[Any] = []

    def check(ctx: Any) -> CheckResult:
        seen.append(dict(ctx.config))
        return CheckResult(False, "401 from the API")

    sink = world.sink("warehouse")
    monkeypatch.setattr(sink, "spec", ConnectorSpec(name="warehouse.real"), raising=False)
    monkeypatch.setattr(sink, "check", check, raising=False)
    result = world.actions.check_connection(ANN, "warehouse")
    assert (result.ok, result.message) == (False, "401 from the API")
    assert seen and seen[0]["token"] == SECRET  # the check got the resolved credential...
    rows = world.store.audit.list_audit()
    assert (
        SECRET not in repr(rows) and rows[0].result == "failed: 401 from the API"
    )  # ...and the audit has none


def test_a_check_that_raises_is_a_failed_check_without_the_secret(
    world: World, monkeypatch: pytest.MonkeyPatch
) -> None:
    def check(ctx: Any) -> CheckResult:
        raise ConnectionError(f"could not log in with {ctx.config['token']}")

    sink = world.sink("warehouse")
    monkeypatch.setattr(sink, "spec", ConnectorSpec(name="warehouse.real"), raising=False)
    monkeypatch.setattr(sink, "check", check, raising=False)
    result = world.actions.check_connection(ANN, "warehouse")
    assert not result.ok and "could not log in" in result.message and SECRET not in result.message

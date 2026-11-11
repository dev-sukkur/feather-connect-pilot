"""Backfill and partition checks of packaged datasets (flow packages §12): the server has none of a package's code, so it
queues the work on the active version's host, which names a backfill's partitions as it plans and answers a check."""

from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pytest
from fastapi import FastAPI

from feather_connect.core.errors import PermanentError
from feather_connect.runtime.plan import BACKFILL
from feather_connect.runtime.runtime import VERIFY
from feather_server.app import AppState, create_app
from feather_server.auth import Actor
from feather_server.services import actions as actions_module
from feather_server.services.actions import Actions
from feather_server.testing import World, day, host_up, prepare, sample_wheel, sign_in

ADMIN = Actor("human", "root", "admin", "root")
ANN = Actor("human", "ann", "operator", "ann")
NAME, FLOW, DATASET = "acme-invoices", "acme-invoices/invoices", "acme-invoices/invoices"


@pytest.fixture
def app(state: AppState, tmp_path: Path) -> FastAPI:
    app = create_app(state=state, loops=lambda s: [])
    wheel = sample_wheel(tmp_path / "1.0.0", "1.0.0")
    app.state.registries.publish(ADMIN, wheel.name, wheel.read_bytes())
    app.state.packages.install(ADMIN, NAME, "1.0.0")
    prepare(state, NAME, "1.0.0")
    state.store.packages.activate(FLOW, NAME, "1.0.0", by="root", at=state.clock.now())
    state.clock.advance(timedelta(minutes=1))  # type: ignore[attr-defined]
    host_up(state, NAME, "1.0.0")
    return app


def _queued(state: AppState, trigger: str) -> Any:
    [run] = [r for r in state.store.runs.list_runs() if r.trigger == trigger]
    return run


def test_a_packaged_backfill_is_previewed_by_days_and_queued_for_its_host(app: FastAPI, state: AppState):
    do = Actions(state)
    preview = do.preview_backfill(
        ANN, FLOW, start=date(2026, 10, 12), end=date(2026, 10, 14), max_concurrent=2
    )
    assert (preview.impact.partitions, preview.impact.deliveries) == (3, 3)  # one route
    assert preview.impact.samples == ("2026-10-12", "2026-10-13", "2026-10-14")
    assert preview.impact.note and "days" in preview.impact.note
    run_id = do.backfill(ANN, preview.token, reason="refill")
    run = state.store.runs.get_run(run_id)
    assert (run.trigger, run.status, run.params["version"]) == ("backfill", "queued", "1.0.0")
    assert run.params[BACKFILL]["start"] == "2026-10-12" and run.params[BACKFILL]["end"] == "2026-10-14"
    assert run.params["max_concurrent"] == 2 and "partitions" not in run.params
    with pytest.raises(ValueError, match="at most"):
        do.preview_backfill(ANN, FLOW, start=date(2025, 1, 1), end=date(2026, 10, 14))


def test_a_packaged_check_is_answered_by_its_host(
    app: FastAPI, state: AppState, monkeypatch: pytest.MonkeyPatch
):
    def host_answers(_: float) -> None:  # stands in for the host, which plans the queued run
        run = _queued(state, "verify")
        assert run.params[VERIFY] == {"dataset": DATASET, "partition": "all"}
        state.store.runs.set_run_result(run.run_id, {"upserts": 2, "deletes": 1})
        state.store.runs.set_run_status(run.run_id, "succeeded")

    monkeypatch.setattr(actions_module, "_sleep", host_answers)
    found = Actions(state).verify_partition(ANN, DATASET, "all")
    assert (found.upserts, found.deletes) == (2, 1)
    [audited] = [a.result for a in state.store.audit.list_audit(20) if a.action == "verify_partition"]
    assert audited == "ok: 2 upserts and 1 deletes differ"


def test_the_dashboard_says_what_the_host_found_or_that_it_is_still_checking(
    app: FastAPI, state: AppState, monkeypatch: pytest.MonkeyPatch
):
    operator = sign_in(app)
    page = operator.get(f"/datasets/{quote(DATASET, safe='')}/partitions").text
    csrf = page.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]
    url = f"/datasets/{quote(DATASET, safe='')}/partitions/all/verify"

    def refused(_: float) -> None:
        run = next(r for r in state.store.runs.list_runs() if r.trigger == "verify" and r.status == "queued")
        state.store.runs.set_run_result(run.run_id, {"error": "'all' is not a partition of dataset x"})
        state.store.runs.set_run_status(run.run_id, "failed", "no")

    monkeypatch.setattr(actions_module, "_sleep", refused)
    bad = operator.post(url, data={"csrf_token": csrf})
    assert bad.status_code == 422 and "is not a partition of" in bad.text

    monkeypatch.setattr(actions_module, "VERIFY_WAIT_S", 0.0)
    slow = operator.post(url, data={"csrf_token": csrf})
    assert slow.status_code == 202 and "still running" in slow.text


def test_a_check_the_host_could_not_make_says_why(
    app: FastAPI, state: AppState, monkeypatch: pytest.MonkeyPatch
):
    def fails(_: float) -> None:
        run = next(r for r in state.store.runs.list_runs() if r.trigger == "verify" and r.status == "queued")
        state.store.runs.set_run_result(run.run_id, {"error": "dataset x is a push dataset"})
        state.store.runs.set_run_status(run.run_id, "failed", "dataset x is a push dataset")

    monkeypatch.setattr(actions_module, "_sleep", fails)
    with pytest.raises(PermanentError, match="push dataset"):
        Actions(state).verify_partition(ANN, DATASET, "all")


# The host's side, on a runtime that has the flow's code.


def test_the_host_names_a_backfills_partitions_from_its_days(world: World) -> None:
    days = {"start": "2026-10-12", "end": "2026-10-14", "timezone": "UTC"}
    run_id = world.store.runs.create_run("orders", "backfill", "ann", {BACKFILL: days})
    world.state.runtime.submit_plan("orders", run_id)
    world.engine.run_until_idle()
    extracts = {t.partition for t in world.store.runs.list_tasks(run_id) if t.kind == "extract"}
    assert extracts == {day(12), day(13), day(14)}
    assert world.store.runs.get_run(run_id).status == "succeeded"


def test_the_host_answers_a_check_and_changes_nothing(world: World) -> None:
    world.run()
    source = world.state.runtime.dataset("orders").source
    rows = [{"id": 171, "date": "2026-10-17"}, {"id": 173, "date": "2026-10-17"}]
    source.load(rows + [{"id": 181, "date": "2026-10-18"}, {"id": 182, "date": "2026-10-18"}])  # type: ignore[attr-defined]
    versions = world.store.versions.list_versions("orders")
    check = {"dataset": "orders", "partition": day(17)}
    run_id = world.store.runs.create_run("orders", "verify", "ann", {VERIFY: check})
    world.state.runtime.submit_plan("orders", run_id)
    world.engine.run_until_idle()
    run = world.store.runs.get_run(run_id)
    assert (run.status, run.result) == ("succeeded", {"upserts": 1, "deletes": 1})
    assert world.store.versions.list_versions("orders") == versions
    assert [t.kind for t in world.store.runs.list_tasks(run_id)] == ["plan"]

    wrong = world.store.runs.create_run(
        "orders", "verify", "ann", {VERIFY: {**check, "partition": "nonsense"}}
    )
    world.state.runtime.submit_plan("orders", wrong)
    world.engine.run_until_idle()
    refused = world.store.runs.get_run(wrong)
    assert refused.status == "failed" and refused.result is not None
    assert "is not a partition of" in refused.result["error"]


def test_links_to_a_packaged_flow_keep_its_slash(app: FastAPI, state: AppState):
    operator = sign_in(app)
    page = operator.get(f"/flows/{FLOW}")
    assert page.status_code == 200 and "%2F" not in page.text
    csrf = page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]
    paused = operator.post(f"/flows/{FLOW}/pause", data={"csrf_token": csrf})
    assert paused.status_code == 303 and paused.headers["location"].startswith(f"/flows/{FLOW}?")
    found = operator.get("/search.json", params={"q": "invoices"}).text
    assert f"/flows/{FLOW}" in found and "%2F" not in found

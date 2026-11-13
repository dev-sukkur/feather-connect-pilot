"""Alerts (M2 plan 2 O6): each rule fires once for its subject and clears by itself, its channels hear of it once (again
when it turns critical), an acknowledged or snoozed one is quiet, and the rules are the environment's to set."""

import re
from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient

from feather_connect.core.errors import PermanentError
from feather_connect.storage.port import AlertRow
from feather_server.app import create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.access import Forbidden
from feather_server.services.actions import DeliverySelection
from feather_server.services.alerts import AlertActions, evaluate_once
from feather_server.testing import World, day, sign_in

API = "/api/v1"
OPS = Actor("human", "ops", "operator", "ops")
DEV = Actor("human", "dev", "developer", "dev")


class Told:
    def __init__(self, fail: bool = False) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.fail = fail

    def __call__(self, name: str, channel: Mapping[str, Any], alert: AlertRow) -> None:
        if self.fail:
            raise OSError("the hook is down")
        self.calls.append((name, alert.rule, alert.severity))


def _settle(world: World, **settings: Any) -> None:
    project = world.state.project
    world.state.project = replace(project, settings={**project.settings, **settings})


@pytest.fixture
def told(world: World) -> Told:
    _settle(world, alert_channels={"ops": {"kind": "webhook", "url": "http://hook"}})
    return Told()


def _firing(world: World, rule: str) -> list[AlertRow]:
    return [a for a in world.store.alerts.list_alerts("firing") if a.rule == rule]


def test_a_failed_delivery_alerts_once_after_15_minutes_and_clears_when_it_applies(world: World, told: Told):
    world.sink("warehouse").fail_next(PermanentError("warehouse is down"))
    world.run([day(18)])
    evaluate_once(world.state, told)
    assert _firing(world, "delivery_failed") == []  # not yet 15 minutes
    world.clock.advance(timedelta(minutes=16))
    report = evaluate_once(world.state, told)
    [alert] = _firing(world, "delivery_failed")
    assert alert.severity == "critical" and "warehouse is down" in alert.message and report.fired
    evaluate_once(world.state, told)
    assert told.calls == [("ops", "delivery_failed", "critical")]  # told once
    retry = world.actions.preview_deliveries(OPS, "retry", DeliverySelection(route="warehouse"))
    world.actions.apply_deliveries(OPS, retry.token, reason="back")
    world.engine.run_until_idle()
    report = evaluate_once(world.state, told)
    assert [a.alert_id for a in report.cleared] == [alert.alert_id] and _firing(
        world, "delivery_failed"
    ) == []


def test_a_budget_warns_then_goes_critical_and_its_channels_hear_again(world: World, told: Told):
    world.store.budgets.acquire("api", 75)
    evaluate_once(world.state, told)
    [alert] = _firing(world, "budget_high")
    assert alert.severity == "warning" and alert.subject == "budget:api"
    world.store.budgets.acquire("api", 12)
    evaluate_once(world.state, told)
    assert told.calls == [("ops", "budget_high", "warning"), ("ops", "budget_high", "critical")]


def test_an_acknowledged_or_snoozed_alert_is_quiet_and_a_failed_notice_is_retried(world: World, told: Told):
    world.store.budgets.acquire("api", 75)
    report = evaluate_once(world.state, Told(fail=True))
    [alert] = _firing(world, "budget_high")
    assert report.failed == (alert.alert_id,)
    AlertActions(world.state).snooze(OPS, alert.alert_id, 60, reason="month end")
    evaluate_once(world.state, told)
    assert told.calls == []  # snoozed
    world.clock.advance(timedelta(minutes=61))
    world.store.budgets.acquire("api", 75)  # it refilled in the hour
    evaluate_once(world.state, told)
    assert told.calls == [("ops", "budget_high", "warning")]  # retried once the snooze is over
    assert AlertActions(world.state).ack(OPS, alert.alert_id).acked_by == "ops"
    assert [a.action for a in world.store.audit.list_audit() if a.action.startswith("alerts.")] == [
        "alerts.ack",
        "alerts.snooze",
    ]


def test_skipped_ticks_failed_runs_and_stale_datasets_alert(world: World, told: Told):
    store = world.store
    for _ in range(3):
        store.runs.create_run("orders", "every:15m", "scheduler", {}, status="skipped", reason="overlap")
    evaluate_once(world.state, told)
    [skipped] = _firing(world, "schedule_skipped")
    assert skipped.subject == "flow:orders" and "overlap" in skipped.message
    assert _firing(world, "run_failed") == []  # off where the environment is not protected
    failed = store.runs.create_run("orders", "manual", "ann", {})
    store.runs.set_run_status(failed, "failed", "boom")
    _settle(world, protected=True)
    evaluate_once(world.state, told)
    [run_failed] = _firing(world, "run_failed")
    assert run_failed.subject == "flow:orders" and "boom" in run_failed.message
    object.__setattr__(world.state.runtime.dataset("orders"), "max_lag", "1h")
    world.run([day(18)])
    world.clock.advance(timedelta(hours=2))
    evaluate_once(world.state, told)
    [stale] = _firing(world, "dataset_stale")
    assert stale.subject == "dataset:orders" and "max_lag of 1h" in stale.message


def test_rules_are_set_by_a_developer_and_checked(world: World, told: Told):
    doing = AlertActions(world.state)
    rule = doing.set_rule(DEV, "delivery_failed", enabled=True, params={"after": "1h"}, channels=["ops"])
    assert rule.custom and rule.params["after"] == "1h" and rule.channels == ("ops",)
    for wrong in ({"after": "soon"}, {"every": 3}):
        with pytest.raises(ValueError):
            doing.set_rule(DEV, "delivery_failed", enabled=True, params=wrong, channels=[])
    with pytest.raises(ValueError, match="no alert channel"):
        doing.set_rule(DEV, "budget_high", enabled=True, params={}, channels=["pager"])
    with pytest.raises(ValueError, match="not an alert rule"):
        doing.set_rule(DEV, "volume_anomaly", enabled=True, params={}, channels=[])
    with pytest.raises(Forbidden):
        doing.set_rule(OPS, "budget_high", enabled=False, params={}, channels=[])
    assert not doing.reset_rule(DEV, "delivery_failed").custom


def test_the_api_and_the_alerts_page(world: World, told: Told):
    world.store.budgets.acquire("api", 75)
    evaluate_once(world.state, told)
    [alert] = _firing(world, "budget_high")
    app = create_app(state=world.state, loops=lambda s: [])
    token = issue_token(world.state.store, "ops", timedelta(days=1), role="operator", clock=world.clock)
    api = TestClient(app, headers={"Authorization": f"Bearer {token}"})
    found = api.get(f"{API}/alerts", params={"state": "firing"}).json()["items"]
    assert [a["rule"] for a in found] == ["budget_high"]
    snoozed = api.post(f"{API}/alerts/{alert.alert_id}:snooze", json={"minutes": 30})
    assert snoozed.status_code == 200 and snoozed.json()["snoozed_until"]
    assert api.post(f"{API}/alerts/{alert.alert_id}:ack", json={}).json()["acked_by"] == "ops"
    assert api.post(f"{API}/alerts/nope:ack", json={}).status_code == 404
    rules = api.get(f"{API}/alert-rules").json()
    assert rules["channels"] == ["ops"] and {r["name"] for r in rules["items"]} >= {
        "budget_high",
        "dataset_stale",
    }
    assert (
        api.put(f"{API}/alert-rules/budget_high", json={"enabled": False}).status_code == 403
    )  # a developer's
    viewer = sign_in(app, "vi", "viewer")
    page = viewer.get("/alerts")
    assert page.status_code == 200 and "budget_high" in page.text and '/ack"' not in page.text
    assert 'href="/alerts"' in page.text
    assert "budget api has used" in sign_in(app).get("/alerts").text


def test_no_worker_heartbeat_fires_worker_missing_and_the_next_one_clears_it(
    world: World, told: Told
):  # M3.1 O1
    world.state.settings = replace(
        world.state.settings, engine="dbos"
    )  # the local engine runs work in the server
    world.store.workers.heartbeat("w1", {}, "sha", 4)
    evaluate_once(world.state, told)
    assert _firing(world, "worker_missing") == []
    world.clock.advance(timedelta(minutes=3))
    evaluate_once(world.state, told)
    [alert] = _firing(world, "worker_missing")
    assert (alert.subject, alert.severity) == ("environment:dev", "critical")
    assert ("ops", "worker_missing", "critical") in told.calls
    world.store.workers.heartbeat("w1", {}, "sha", 4)
    evaluate_once(world.state, told)
    assert _firing(world, "worker_missing") == []


def test_worker_missing_is_quiet_when_every_flow_is_paused_or_the_server_runs_the_work(
    world: World, told: Told
):
    world.clock.advance(timedelta(minutes=10))
    evaluate_once(world.state, told)  # the local engine: no workers to miss
    assert _firing(world, "worker_missing") == []
    world.state.settings = replace(world.state.settings, engine="dbos")
    world.store.overrides.set("flow:orders", "paused", True, actor="ops", reason="maintenance")
    evaluate_once(world.state, told)
    assert _firing(world, "worker_missing") == []


def test_a_teams_channel_gets_an_adaptive_card(world: World):
    from feather_server.services.alerts import teams_card

    world.store.alerts.fire("worker_missing", "environment:dev", "critical", "no worker")
    [alert] = world.store.alerts.list_alerts("firing")
    card = teams_card(alert)
    assert card["type"] == "message"
    [attachment] = card["attachments"]
    assert attachment["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert attachment["content"]["body"][1]["text"] == "no worker"


def test_a_dataset_that_seals_fewer_records_than_expected_is_quiet(world: World, told: Told):  # pilot review
    doing = AlertActions(world.state)
    doing.set_rule(
        DEV, "dataset_quiet", enabled=True, params={"datasets": {"orders": 5, "never": 1}}, channels=[]
    )
    evaluate_once(world.state, told)
    assert _firing(world, "dataset_quiet") == []  # nothing read yet: not judged
    world.run([day(18)])  # two records
    evaluate_once(world.state, told)
    [quiet] = _firing(world, "dataset_quiet")
    assert quiet.subject == "dataset:orders" and "sealed 2 records in the last 24h" in quiet.message
    doing.set_rule(DEV, "dataset_quiet", enabled=True, params={"datasets": {"orders": 2}}, channels=[])
    evaluate_once(world.state, told)
    assert _firing(world, "dataset_quiet") == []  # enough: it clears
    world.clock.advance(timedelta(hours=25))
    evaluate_once(world.state, told)
    [quiet] = _firing(world, "dataset_quiet")
    assert "sealed 0 records in the last 24h (0 versions)" in quiet.message
    for wrong in ({"datasets": {"orders": 0}}, {"datasets": ["orders"]}, {"within": "a while"}):
        with pytest.raises(ValueError):
            doing.set_rule(DEV, "dataset_quiet", enabled=True, params=wrong, channels=[])


def test_the_alerts_page_sets_dataset_quiet_s_datasets_as_name_equals_count(world: World, told: Told):
    browser = sign_in(create_app(state=world.state, loops=lambda s: []), "dev", "developer")
    token = re.search(r'<meta name="csrf-token" content="([^"]+)"', browser.get("/alerts").text)
    assert token
    form = {
        "csrf_token": token.group(1),
        "enabled": "on",
        "datasets": "orders=5, acme/invoices=1",
        "within": "12h",
    }
    page = browser.post("/alerts/rules/dataset_quiet", data=form)
    assert page.status_code == 200 and "Rule dataset_quiet is saved." in page.text
    [rule] = [r for r in world.store.alerts.rules() if r.name == "dataset_quiet"]
    assert rule.params == {"datasets": {"orders": 5, "acme/invoices": 1}, "within": "12h"}
    assert 'value="orders=5, acme/invoices=1"' in page.text
    wrong = browser.post("/alerts/rules/dataset_quiet", data={**form, "datasets": "orders"})
    assert wrong.status_code == 422 and "name=count" in wrong.text

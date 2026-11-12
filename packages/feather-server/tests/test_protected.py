"""A protected environment (M3 plan 1 E3, design §11, FR-H3): every change needs a reason, and tokens made there are
viewers unless a role is asked for. What only looks needs no reason."""

import re
from collections.abc import Callable
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from feather_connect.storage.port import VersionDraft
from feather_server.app import AppState, create_app
from feather_server.auth import Actor, issue_token, token_digest
from feather_server.services.access import ReasonRequired, require_reason
from feather_server.services.accounts import Accounts
from feather_server.testing import World, sign_in

API = "/api/v1"


@pytest.fixture
def protected(world: World, monkeypatch: pytest.MonkeyPatch) -> World:
    monkeypatch.setitem(world.state.project.settings, "protected", True)
    return world


@pytest.fixture
def api(protected: World) -> TestClient:
    token = issue_token(protected.store, "ci", timedelta(days=1), role="admin", clock=protected.clock)
    return TestClient(
        create_app(state=protected.state, loops=lambda s: []),
        base_url="http://localhost:8000",
        headers={"Authorization": f"Bearer {token}"},
    )


def test_a_change_without_a_reason_is_refused_only_where_protected() -> None:
    free, guarded = Actor("human", "ann", "admin"), Actor("human", "ann", "admin", protected=True)
    require_reason(free, "flows.pause", None)  # not protected: optional
    require_reason(guarded, "flows.pause", "supplier outage")
    for blank in (None, "", "   "):
        with pytest.raises(ReasonRequired, match="a reason is required in a protected environment"):
            require_reason(guarded, "flows.pause", blank)


def test_the_api_answers_422_reason_required_and_changes_nothing(api: TestClient, protected: World) -> None:
    answer = api.post(f"{API}/flows/orders:pause")
    assert answer.status_code == 422 and answer.json()["check"] == "reason_required", answer.text
    assert not protected.state.runtime.is_flow_paused("orders")
    assert api.post(f"{API}/flows/orders:pause", json={"reason": "supplier outage"}).status_code == 200
    assert protected.state.runtime.is_flow_paused("orders")
    [row] = protected.store.audit.list_audit(10, action="pause_flow")
    assert row.request["reason"] == "supplier outage" and row.result == "ok"


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("post", "/users", {"username": "zed", "password": "pw-123456"}),
        ("post", "/tokens", {"name": "t1"}),
        ("put", "/users/ci-user/role", {"role": "viewer"}),
        ("delete", "/secrets/NOPE", None),
        ("delete", "/alert-rules/run_failed", None),
        ("post", "/registries", {"name": "acme", "index_url": "https://pypi.example/simple"}),
    ],
)
def test_admin_changes_need_a_reason_too(
    api: TestClient, method: str, path: str, body: dict[str, str] | None
):
    answer = api.request(method.upper(), f"{API}{path}", json=body)
    assert answer.status_code == 422 and answer.json().get("check") == "reason_required", answer.text


def test_what_only_looks_needs_no_reason(api: TestClient) -> None:
    assert api.get(f"{API}/flows").status_code == 200
    assert api.get(f"{API}/audit").status_code == 200
    checked = api.post(f"{API}/connections/warehouse:check")
    assert checked.json().get("check") != "reason_required", checked.text


def test_a_token_made_there_is_a_viewer_unless_a_role_is_asked_for(protected: World) -> None:
    accounts = Accounts(protected.store, protected.clock)
    admin = Actor("human", "root", "admin", "root", protected=True)
    protected.store.users.create_user("root", "h", "admin")

    def role_of(token: str) -> str:
        found = protected.store.users.find_token(token_digest(token))
        assert found is not None
        return found.role

    assert (
        role_of(accounts.create_token(admin, "looks", timedelta(days=1), reason="the wall screen"))
        == "viewer"
    )
    assert (
        role_of(accounts.create_token(admin, "acts", timedelta(days=1), "operator", reason="nightly"))
        == "operator"
    )
    free = Actor("human", "root", "admin", "root")
    assert (
        role_of(accounts.create_token(free, "free", timedelta(days=1))) == "admin"
    )  # unprotected: the user's


def test_a_page_asks_for_the_reason_and_sends_the_form_again(
    client: TestClient, protected: World, csrf: Callable[[], str]
) -> None:
    asked = client.post(
        "/flows/orders/pause",
        data={"csrf_token": csrf()},
        headers={"referer": "http://localhost:8000/flows/orders"},
    )
    assert asked.status_code == 422 and "A reason is needed" in asked.text
    assert 'action="/flows/orders/pause"' in asked.text and 'href="/flows/orders"' in asked.text
    assert not protected.state.runtime.is_flow_paused("orders")
    token = re.search(r'name="csrf_token" value="([^"]+)"', asked.text)
    assert token
    sent = client.post(
        "/flows/orders/pause", data={"csrf_token": token.group(1), "reason": "supplier outage"}
    )
    assert sent.status_code == 303 and protected.state.runtime.is_flow_paused("orders")


def test_the_reason_page_keeps_the_forms_fields(
    client: TestClient, protected: World, csrf: Callable[[], str]
) -> None:
    alert, _ = protected.store.alerts.fire("budget_high", "api", "warning", "api at 81%")
    asked = client.post(f"/alerts/{alert.alert_id}/snooze", data={"csrf_token": csrf(), "minutes": "30"})
    assert asked.status_code == 422 and 'name="minutes" value="30"' in asked.text
    assert protected.store.alerts.get_alert(alert.alert_id).snoozed_until is None  # nothing done yet


# Destructive actions wait for a second person (M3 plan 1 E3).


def _applied(state: AppState, version: str = "v2") -> None:
    run = state.store.runs.create_run("acme/orders", "every", "t", {})
    state.store.versions.stage(
        VersionDraft(version, "acme/orders", "_", run, True, 1, 0, "h", None, None, "s")
    )
    state.store.versions.seal([version], ["acme/sap"])
    state.store.ledger.transition("acme/sap", version, "running", actor="system")
    state.store.ledger.transition("acme/sap", version, "applied", actor="system", rows_applied=1)


def _api(state: AppState, name: str, role: str, grants: tuple[str, ...] = ()) -> TestClient:
    state.store.users.create_user(name, "h", role)  # type: ignore[arg-type]
    if grants:
        state.store.users.set_grants(name, grants)  # type: ignore[arg-type]
    token = issue_token(
        state.store, f"{name}-t", timedelta(days=1), role=role, created_by=name, clock=state.clock
    )  # type: ignore[arg-type]
    return TestClient(
        create_app(state=state, loops=lambda s: []), headers={"Authorization": f"Bearer {token}"}
    )


def test_a_compensation_waits_for_a_second_person_who_carries_it_out(
    state: AppState, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setitem(state.project.settings, "protected", True)
    _applied(state)
    ann = _api(state, "ann", "operator", ("approver",))
    bob = _api(state, "bob", "operator", ("approver",))
    asked = ann.post(f"{API}/routes/acme/sap/deliveries/v2:compensate", json={"reason": "posted twice"})
    assert asked.status_code == 202 and asked.json()["status"] == "waiting", asked.text
    request_id = asked.json()["request_id"]
    assert state.store.ledger.get("acme/sap", "v2").status == "applied"  # nothing done yet
    [listed] = bob.get(f"{API}/action-requests").json()["items"]
    assert (listed["action"], listed["requested_by"], listed["reason"]) == (
        "compensate_delivery",
        "ann",
        "posted twice",
    )
    own = ann.post(f"{API}/action-requests/{request_id}:approve", json={"reason": "sure"})
    assert own.status_code == 403 and own.json()["check"] == "second_person", own.text
    done = bob.post(f"{API}/action-requests/{request_id}:approve", json={"reason": "checked with finance"})
    assert done.status_code == 200 and done.json()["status"] == "approved", done.text
    assert done.json()["decided_by"] == "bob" and done.json()["result"] == "compensating"
    again = bob.post(f"{API}/action-requests/{request_id}:approve", json={"reason": "again"})
    assert again.status_code == 409  # done at most once
    actions = [r.action for r in state.store.audit.list_audit(50)]
    assert "approve_request" in actions and "compensate_delivery" in actions
    assert bob.get(f"{API}/action-requests").json()["items"] == []


def test_a_rejected_request_does_nothing_and_needs_a_reason(state: AppState, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setitem(state.project.settings, "protected", True)
    _applied(state)
    ann = _api(state, "ann", "operator")
    bob = _api(state, "bob", "operator", ("approver",))
    request_id = ann.post(f"{API}/routes/acme/sap/deliveries/v2:compensate", json={"reason": "x"}).json()[
        "request_id"
    ]
    assert bob.post(f"{API}/action-requests/{request_id}:reject", json={}).status_code == 422
    no = bob.post(f"{API}/action-requests/{request_id}:reject", json={"reason": "not ours"})
    assert no.status_code == 200 and no.json()["status"] == "rejected"
    assert state.store.ledger.get("acme/sap", "v2").status == "applied"


def test_without_protection_a_compensation_is_done_at_once(state: AppState):
    _applied(state)
    ann = _api(state, "ann", "operator")
    done = ann.post(f"{API}/routes/acme/sap/deliveries/v2:compensate", json={"reason": "posted twice"})
    assert (
        done.status_code == 202
        and done.json()["status"] == "compensating"
        and "request_id" not in done.json()
    )


def test_the_holds_page_lists_requests_and_a_second_person_decides(
    state: AppState, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setitem(state.project.settings, "protected", True)
    _applied(state)
    ann = _api(state, "ann", "operator")
    request_id = ann.post(f"{API}/routes/acme/sap/deliveries/v2:compensate", json={"reason": "x"}).json()[
        "request_id"
    ]
    browser = sign_in(create_app(state=state, loops=lambda s: []), "carol", "operator", ("approver",))
    page = browser.get("/holds").text
    assert "Asked for, waiting for a second person" in page and f"/holds/requests/{request_id}/decide" in page
    csrf = re.search(r'<meta name="csrf-token" content="([^"]+)"', page)
    assert csrf
    sent = browser.post(
        f"/holds/requests/{request_id}/decide",
        data={"csrf_token": csrf.group(1), "decision": "approve", "reason": "checked"},
    )
    assert sent.status_code == 303 and state.store.requests.get(request_id).status == "approved"


def test_the_status_bar_says_protected(client: TestClient, protected: World) -> None:
    assert ">protected</span>" in client.get("/status").text

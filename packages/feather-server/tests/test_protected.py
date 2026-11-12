"""A protected environment (M3 plan 1 E3, design §11, FR-H3): every change needs a reason, and tokens made there are
viewers unless a role is asked for. What only looks needs no reason."""

import re
from collections.abc import Callable
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from feather_server.app import create_app
from feather_server.auth import Actor, issue_token, token_digest
from feather_server.services.access import ReasonRequired, require_reason
from feather_server.services.accounts import Accounts
from feather_server.testing import World

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

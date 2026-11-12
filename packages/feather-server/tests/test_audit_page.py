"""The audit log's page and API (M2 plan 2 O3): newest first, filtered by actor, action and target, a page at a time,
and an operator's to see, not a viewer's."""

from datetime import timedelta

from fastapi.testclient import TestClient

from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.testing import World, sign_in


def _fill(world: World) -> None:
    audit = world.state.store.audit
    audit.record("human", "ann", "retry", "route:erp", {"reason": "bad gateway"}, "ok")
    audit.record("human", "bob", "skip", "route:crm", {}, "refused: needs the operator role")
    audit.record("token", "ci", "retry", "route:erp", {}, "ok")


def test_the_api_lists_the_audit_log_filtered_and_by_page(world: World):
    _fill(world)
    state = world.state
    token = issue_token(state.store, "ops", timedelta(days=1), role="operator", clock=world.clock)
    api = TestClient(
        create_app(state=state, loops=lambda s: []), headers={"Authorization": f"Bearer {token}"}
    )
    retries = api.get("/api/v1/audit", params={"action": "retry"}).json()
    assert [r["actor"] for r in retries["items"]] == ["ci", "ann"] and retries["next_cursor"] is None
    assert [r["actor"] for r in api.get("/api/v1/audit", params={"target": "route:c"}).json()["items"]] == [
        "bob"
    ]
    first = api.get("/api/v1/audit", params={"limit": 1, "target": "route:"}).json()
    assert [r["actor"] for r in first["items"]] == ["ci"] and first["next_cursor"]
    rest = api.get("/api/v1/audit", params={"target": "route:", "cursor": first["next_cursor"]}).json()
    assert [r["actor"] for r in rest["items"]] == ["bob", "ann"]
    viewer = issue_token(state.store, "vi", timedelta(days=1), role="viewer", clock=world.clock)
    refused = api.get("/api/v1/audit", headers={"Authorization": f"Bearer {viewer}"})
    assert refused.status_code == 403


def test_the_audit_page_filters_and_hides_from_a_viewer(world: World):
    _fill(world)
    app = create_app(state=world.state, loops=lambda s: [])
    browser = sign_in(app)
    page = browser.get("/audit")
    assert page.status_code == 200 and "bad gateway" in page.text and 'href="/audit"' in page.text
    only = browser.get("/audit", params={"actor": "bob"})
    assert "refused: needs the operator role" in only.text and "bad gateway" not in only.text
    assert "Nothing in the audit log matches." in browser.get("/audit", params={"action": "none"}).text
    viewer = sign_in(app, "vi", "viewer")
    assert viewer.get("/audit").status_code == 403
    assert 'href="/audit"' not in viewer.get("/").text  # nor in its sidebar

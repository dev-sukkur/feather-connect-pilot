"""Holds (M2 plan 2 stage A1): a delivery held for approval, listed on the API and the Holds page, approved or
rejected by an operator, audited; a rejection needs a reason."""

import re
from datetime import timedelta

from fastapi.testclient import TestClient

from feather_connect.storage.port import VersionDraft
from feather_server.app import AppState, create_app
from feather_server.auth import issue_token
from feather_server.testing import sign_in


def _held(state: AppState, version: str = "v1") -> None:
    run = state.store.runs.create_run("acme/orders", "every", "t", {})
    state.store.versions.stage(
        VersionDraft(version, "acme/orders", "_", run, True, 1, 0, "h", None, None, "s")
    )
    state.store.versions.seal([version], ["acme/sap"])
    ledger = state.store.ledger
    ledger.transition("acme/sap", version, "running", actor="system")
    ledger.transition("acme/sap", version, "held", actor="system", reason="approval: 1 record to approve")


def _csrf(page: str) -> str:
    found = re.search(r'<meta name="csrf-token" content="([^"]+)"', page)
    assert found
    return found.group(1)


def test_the_api_lists_a_hold_and_an_operator_approves_it(state: AppState):
    _held(state)
    app = create_app(state=state, loops=lambda s: [])
    token = issue_token(state.store, "ops", timedelta(days=1), role="operator", clock=state.clock)
    api = TestClient(app, headers={"Authorization": f"Bearer {token}"})
    [hold] = api.get("/api/v1/holds").json()["holds"]
    assert (hold["route"], hold["kind"], hold["reason"]) == (
        "acme/sap",
        "approval",
        "approval: 1 record to approve",
    )
    refused = api.post("/api/v1/routes/acme/sap/deliveries/v1:reject", json={})
    assert refused.status_code in (400, 422)
    done = api.post("/api/v1/routes/acme/sap/deliveries/v1:approve", json={"reason": "checked"})
    assert done.status_code == 200, done.text
    assert state.store.ledger.get("acme/sap", "v1").status in ("pending", "running", "applied", "failed")
    [audit] = [r for r in state.store.audit.list_audit(20) if r.action == "approve_delivery"]
    assert audit.result == "ok" and audit.actor == "ops"


def test_the_holds_page_rejects_with_a_reason(state: AppState):
    _held(state)
    browser = sign_in(create_app(state=state, loops=lambda s: []))
    page = browser.get("/holds")
    assert page.status_code == 200 and "acme/sap" in page.text and "Approve" in page.text
    form = {"csrf_token": _csrf(page.text), "route": "acme/sap", "version_id": "v1", "decision": "reject"}
    assert browser.post("/holds/decide", data=form).status_code == 422
    done = browser.post("/holds/decide", data={**form, "reason": "wrong customer"})
    assert done.status_code == 303 and done.headers["location"] == "/holds?done=rejected"
    assert state.store.ledger.get("acme/sap", "v1").status == "skipped"
    assert "Nothing is held" in browser.get("/holds").text

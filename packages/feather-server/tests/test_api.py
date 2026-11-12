"""The JSON API under /api/v1: thin routers over the service layer."""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from feather_connect.core.errors import PermanentError
from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.testing import SECRET, World, day

API = "/api/v1"


@pytest.fixture
def api(world: World) -> TestClient:
    """A client calling the API with a bearer token of `ci`."""
    token = issue_token(world.store, "ci", timedelta(days=1), role="operator", clock=world.clock)
    return TestClient(
        create_app(state=world.state, loops=lambda s: []),
        base_url="http://localhost:8000",
        headers={"Authorization": f"Bearer {token}"},
    )


def _fail_warehouse(world: World) -> None:
    world.sink("warehouse").fail_next(PermanentError("no such table"))
    world.run([day(17)])


def test_openapi_is_3_1_and_lists_every_endpoint(api: TestClient) -> None:
    doc = api.get("/openapi.json").json()
    assert doc["openapi"].startswith("3.1")
    paths = set(doc["paths"])
    for path in (
        "/flows",
        "/flows/{flow}",
        "/datasets/{dataset}",
        "/routes/{route}",
        "/runs",
        "/runs/{run_id}",
        "/runs/{run_id}/tasks",
        "/tasks/{task_id}/logs",
        "/flows/{flow}:run",
        "/runs/{run_id}:cancel",
        "/flows/{flow}:pause",
        "/flows/{flow}:resume",
        "/datasets/{dataset}/partitions",
        "/datasets/{dataset}/partitions:rerun",
        "/datasets/{dataset}/partitions:rerun:preview",
        "/flows/{flow}:backfill",
        "/flows/{flow}:backfill:preview",
        "/versions/{version_id}",
        "/versions/{version_id}/records",
        "/routes/{route}/deliveries",
        "/deliveries:retry",
        "/deliveries:skip",
        "/deliveries:replay",
        "/deliveries:retry:preview",
        "/deliveries:skip:preview",
        "/deliveries:replay:preview",
        "/budgets",
        "/budgets/{name}",
        "/budgets/{name}/usage",
        "/connections",
        "/connections/{name}:check",
        "/summary",
    ):
        assert API + path in paths, path


def test_errors_are_problem_json(api: TestClient) -> None:
    for answer, status in (
        (api.get(f"{API}/flows/nope"), 404),
        (api.get(f"{API}/no-such-thing"), 404),
        (api.get(f"{API}/runs", params={"limit": 0}), 422),
        (api.post(f"{API}/flows/orders:backfill:preview", json={"start": "not a date"}), 422),
    ):
        assert answer.status_code == status
        assert answer.headers["content-type"].startswith("application/problem+json")
        doc = answer.json()
        assert doc["status"] == status and doc["title"] and doc["detail"]


def test_unauthenticated_calls_are_401_problems(world: World) -> None:
    anonymous = TestClient(create_app(state=world.state, loops=lambda s: []))
    answer = anonymous.get(f"{API}/flows")
    assert answer.status_code == 401
    assert answer.headers["content-type"].startswith("application/problem+json")


def test_pagination_uses_a_cursor(api: TestClient, world: World) -> None:
    for n in (14, 15, 16):
        world.run([day(n)])
    first = api.get(f"{API}/runs", params={"limit": 2}).json()
    assert len(first["items"]) == 2 and first["next_cursor"]
    second = api.get(f"{API}/runs", params={"limit": 2, "cursor": first["next_cursor"]}).json()
    assert len(second["items"]) == 1 and second["next_cursor"] is None
    assert {r["run_id"] for r in first["items"]}.isdisjoint(r["run_id"] for r in second["items"])
    bad = api.get(f"{API}/runs", params={"cursor": "made-up"})
    assert bad.status_code == 422


def test_mass_action_needs_a_preview_token(api: TestClient) -> None:
    answer = api.post(f"{API}/deliveries:retry", json={"reason": "go"})
    assert answer.status_code == 400
    assert answer.json()["preview"] == f"{API}/deliveries:retry:preview"
    answer = api.post(f"{API}/flows/orders:backfill", json={})
    assert answer.status_code == 400 and answer.json()["preview"].endswith(":backfill:preview")


def test_preview_then_confirm_retries_deliveries(api: TestClient, world: World) -> None:
    _fail_warehouse(world)
    preview = api.post(f"{API}/deliveries:retry:preview", json={"route": "warehouse"})
    assert preview.status_code == 200, preview.text
    doc = preview.json()
    assert doc["impact"]["deliveries"] == 1 and doc["confirmation_token"]
    done = api.post(f"{API}/deliveries:retry", json={"confirmation_token": doc["confirmation_token"]})
    assert done.status_code == 200, done.text
    assert len(done.json()["moved"]) == 1
    world.engine.run_until_idle()
    again = api.post(f"{API}/deliveries:retry", json={"confirmation_token": doc["confirmation_token"]})
    assert again.status_code == 409  # used already


def test_a_token_for_another_action_is_refused(api: TestClient, world: World) -> None:
    _fail_warehouse(world)
    token = api.post(f"{API}/deliveries:skip:preview", json={"route": "warehouse"}).json()[
        "confirmation_token"
    ]
    answer = api.post(f"{API}/deliveries:retry", json={"confirmation_token": token})
    assert answer.status_code == 400


def test_idempotency_key_repeats_return_the_first_result(api: TestClient, world: World) -> None:
    headers = {"Idempotency-Key": "k-1"}
    first = api.post(f"{API}/flows/orders:run", json={"partitions": [day(18)]}, headers=headers)
    assert first.status_code == 202, first.text
    second = api.post(f"{API}/flows/orders:run", json={"partitions": [day(18)]}, headers=headers)
    assert second.status_code == 202 and second.json() == first.json()
    assert len(world.store.runs.list_runs(flow="orders")) == 1
    elsewhere = api.post(f"{API}/flows/orders:pause", headers=headers)
    assert elsewhere.status_code == 422
    world.clock.advance(timedelta(hours=25))
    third = api.post(f"{API}/flows/orders:run", json={"partitions": [day(18)]}, headers=headers)
    assert third.json() != first.json()


def test_version_records_are_masked_and_paginated(api: TestClient, world: World) -> None:
    world.run([day(18)])
    version = world.store.versions.latest_sealed("orders", day(18))
    assert version is not None
    first = api.get(f"{API}/versions/{version.version_id}/records", params={"limit": 1}).json()
    assert first["masked"] is False and len(first["items"]) == 1 and first["next_cursor"]
    second = api.get(
        f"{API}/versions/{version.version_id}/records", params={"limit": 5, "cursor": first["next_cursor"]}
    ).json()
    assert len(second["items"]) == 1 and second["next_cursor"] is None
    assert {r["id"] for r in first["items"] + second["items"]} == {181, 182}
    assert api.get(f"{API}/versions/{version.version_id}").json()["partition"] == day(18)


def test_reason_reaches_the_audit_row(api: TestClient, world: World) -> None:
    answer = api.post(f"{API}/flows/orders:pause", json={"reason": "supplier outage"})
    assert answer.status_code == 200
    row = world.store.audit.list_audit(limit=1)[0]
    assert row.action == "pause_flow" and row.actor == "ci" and row.request["reason"] == "supplier outage"


def test_no_secret_in_any_connection_response(api: TestClient) -> None:
    listed = api.get(f"{API}/connections")
    checked = api.post(f"{API}/connections/warehouse:check")
    assert listed.status_code == 200 and checked.status_code == 200
    for answer in (listed, checked):
        assert SECRET not in answer.text
    warehouse = next(c for c in listed.json()["items"] if c["name"] == "warehouse")
    assert warehouse["credentials"] == ["auth.token"] or warehouse["credentials"]


def test_catalogue_partitions_budgets_and_summary(api: TestClient, world: World) -> None:
    world.run([day(18)])
    assert [f["name"] for f in api.get(f"{API}/flows").json()["items"]] == ["orders"]
    assert api.get(f"{API}/datasets/orders").json()["routes"] == ["warehouse", "gofrugal"]
    assert api.get(f"{API}/routes/warehouse").json()["connection"] == "warehouse"
    grid = api.get(f"{API}/datasets/orders/partitions").json()
    assert grid["items"][0]["partition"] == day(18) and grid["routes"] == ["warehouse", "gofrugal"]
    assert api.get(f"{API}/budgets").json()["items"][0]["name"] == "api"
    assert api.get(f"{API}/budgets/api/usage").json()["name"] == "api"
    assert api.get(f"{API}/budgets/none/usage").status_code == 404
    summary = api.get(f"{API}/summary").json()
    assert summary["deliveries"]["applied"] == 2
    run_id = world.store.runs.list_runs(limit=1)[0].run_id
    assert api.get(f"{API}/runs/{run_id}").json()["run"]["run_id"] == run_id
    tasks = api.get(f"{API}/runs/{run_id}/tasks").json()["items"]
    assert tasks and api.get(f"{API}/tasks/{tasks[0]['task_id']}/logs").status_code == 200


def test_rerun_and_backfill_through_previews(api: TestClient, world: World) -> None:
    world.run([day(18)])
    preview = api.post(
        f"{API}/datasets/orders/partitions:rerun:preview", json={"partitions": [day(18)]}
    ).json()
    assert preview["impact"]["partitions"] == 1
    rerun = api.post(
        f"{API}/datasets/orders/partitions:rerun", json={"confirmation_token": preview["confirmation_token"]}
    )
    assert rerun.status_code == 202 and rerun.json()["run_id"]
    preview = api.post(
        f"{API}/flows/orders:backfill:preview", json={"start": "2026-10-10", "end": "2026-10-11"}
    ).json()
    assert preview["impact"]["partitions"] == 2
    backfill = api.post(
        f"{API}/flows/orders:backfill", json={"confirmation_token": preview["confirmation_token"]}
    )
    assert backfill.status_code == 202


def test_a_session_call_needs_the_csrf_token(client: TestClient) -> None:
    assert client.get(f"{API}/flows").status_code == 200
    assert client.post(f"{API}/flows/orders:pause").status_code == 403

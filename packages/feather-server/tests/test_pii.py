"""PII masking (M2 plan 2 O4): a dataset's `pii` fields show masked in a version's records, a task's and a trace, on the
dashboard and in the API, unless a PII reader asks to see them; each unmasking is audited, and each refusal."""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.services.pii import MASK
from feather_server.testing import World, day, sign_in

API = "/api/v1"


@pytest.fixture
def sealed(world: World) -> str:
    """The version of 18 October, its `date` named PII."""
    object.__setattr__(world.state.runtime.dataset("orders"), "pii", ("date",))
    world.run([day(18)])
    found = world.store.versions.latest_sealed("orders", day(18))
    assert found is not None
    return found.version_id


def _api(world: World, user: str, role: str, grants: tuple[str, ...] = ()) -> TestClient:
    store = world.state.store
    store.users.create_user(user, "h", role)  # pyright: ignore[reportArgumentType]
    store.users.set_grants(user, list(grants))  # pyright: ignore[reportArgumentType]
    token = issue_token(store, user, timedelta(days=1), role=role, created_by=user, clock=world.clock)  # pyright: ignore[reportArgumentType]
    app = create_app(state=world.state, loops=lambda s: [])
    return TestClient(app, headers={"Authorization": f"Bearer {token}"})


def test_the_api_masks_pii_and_a_pii_reader_may_see_it_audited(world: World, sealed: str) -> None:
    op = _api(world, "op", "operator")
    found = op.get(f"{API}/versions/{sealed}/records").json()
    assert found["masked"] is True and found["pii"] == ["date"]
    assert {r["date"] for r in found["items"]} == {MASK} and {r["id"] for r in found["items"]} == {181, 182}
    assert op.get(f"{API}/versions/{sealed}/records", params={"unmask": True}).status_code == 403
    reader = _api(world, "pr", "viewer", ("pii-reader",))
    shown = reader.get(f"{API}/versions/{sealed}/records", params={"unmask": True}).json()
    assert shown["masked"] is False and {r["date"] for r in shown["items"]} == {"2026-10-18"}
    rows = [(a.actor, a.result) for a in world.store.audit.list_audit(action="pii.unmask")]
    assert rows == [("pr", "ok"), ("op", "refused: needs the pii-reader grant")]
    traced = op.get(f"{API}/datasets/orders/records/181/trace").json()
    assert traced["masked"] and traced["versions"][0]["record"]["date"] == MASK


def test_a_tasks_records_are_masked_too(world: World, sealed: str) -> None:
    op = _api(world, "op", "operator")
    [run] = world.store.runs.list_runs(limit=1)
    tasks = op.get(f"{API}/runs/{run.run_id}/tasks").json()["items"]
    extract = next(t for t in tasks if t["kind"] == "extract")
    found = op.get(f"{API}/runs/{run.run_id}/tasks/{extract['task_id']}/records").json()
    assert found["masked"] and {i["payload"]["date"] for i in found["items"]} == {MASK}


def test_the_dashboard_masks_pii_and_offers_a_reader_to_show_it(world: World, sealed: str) -> None:
    app = create_app(state=world.state, loops=lambda s: [])
    op = sign_in(app)
    page = op.get(f"/versions/{sealed}")
    assert "The PII fields date are masked." in page.text and MASK in page.text
    assert "Show them" not in page.text and op.get(f"/versions/{sealed}?unmask=1").status_code == 403
    reader = sign_in(app, "pr", "operator", ["pii-reader"])
    page = reader.get(f"/versions/{sealed}")
    assert f'href="/versions/{sealed}?unmask=1"' in page.text
    shown = reader.get(f"/versions/{sealed}?unmask=1").text
    assert "shown as they are" in shown and MASK not in shown
    assert MASK in op.get("/trace?dataset=orders&key=181").text

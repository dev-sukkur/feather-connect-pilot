"""The Budgets page and API (M2 plan 2 O1): each budget's windows with their use and the part kept for high-priority
work, and the tasks waiting for it."""

from datetime import timedelta

from fastapi.testclient import TestClient

from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.testing import World, sign_in


def test_the_api_shows_a_budgets_windows_reserve_and_waiting_tasks(world: World):
    state = world.state
    run = state.store.runs.create_run("acme/orders", "every", "t", {})
    state.store.budgets.acquire("api", 30)
    state.store.budgets.note_wait(
        "api", "apply:acme/erp:v1:0:0", run, world.clock.now() + timedelta(minutes=2)
    )
    token = issue_token(state.store, "viewer", timedelta(days=1), role="operator", clock=world.clock)
    api = TestClient(
        create_app(state=state, loops=lambda s: []), headers={"Authorization": f"Bearer {token}"}
    )
    found = api.get("/api/v1/budgets/api").json()
    [window] = found["windows"]
    assert (window["units"], window["seconds"], window["used"], window["kept_for_high"]) == (
        90,
        60,
        30.0,
        54.0,
    )
    [waiting] = found["waiting"]
    assert (waiting["task_ref"], waiting["run_id"], waiting["flow"], waiting["waits"]) == (
        "apply:acme/erp:v1:0:0",
        run,
        "acme/orders",
        1,
    )
    assert api.get("/api/v1/budgets/none").status_code == 404


def test_the_budgets_page_lists_each_budget_and_its_own_page(world: World):
    state = world.state
    state.store.budgets.note_wait(
        "api", "extract:r1:orders:*", None, world.clock.now() + timedelta(minutes=2)
    )
    browser = sign_in(create_app(state=state, loops=lambda s: []))
    page = browser.get("/budgets")
    assert page.status_code == 200
    assert (
        'href="/budgets/api"' in page.text
        and "extract:r1:orders:*" in page.text
        and "Kept for high" in page.text
    )
    one = browser.get("/budgets/api")
    assert one.status_code == 200 and "Tasks waiting for api" in one.text
    assert browser.get("/budgets/none").status_code == 404
    world.clock.advance(timedelta(minutes=3))  # the wait is over
    assert "No task is waiting for it." in browser.get("/budgets/api").text

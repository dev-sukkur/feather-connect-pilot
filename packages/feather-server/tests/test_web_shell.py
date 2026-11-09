"""The dashboard's shell and its list pages: the status bar, the command palette's search, the theme, the column
chooser, bulk actions and who may do them, the rows a list opens to, and the secrets page."""

import json
import re
from collections.abc import Callable
from datetime import timedelta

from fastapi.testclient import TestClient

from feather_connect.vault import MasterKey, Vault, new_master_key
from feather_server import web
from feather_server.app import create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.environment import Environment
from feather_server.testing import World, sign_in

Csrf = Callable[[], str]
ADMIN = Actor("human", "root", "admin", "root")
STATIC = web.STATIC_DIR


def _bearer(world: World, scope: str) -> TestClient:
    token = issue_token(world.store, f"t-{scope}", timedelta(days=1), role="admin", scope=scope)  # type: ignore[arg-type]
    return TestClient(
        create_app(state=world.state, loops=lambda s: []),
        base_url="http://localhost:8000",
        headers={"Authorization": f"Bearer {token}"},
    )


# The shell.


def test_every_page_has_the_shell_sidebar_top_bar_status_bar_and_dialogs(client: TestClient) -> None:
    html = client.get("/flows").text
    nav = html.split('<nav aria-label="Main">')[1].split("</nav>")[0]
    for label in (
        "Overview",
        "Flows",
        "Runs",
        "Datasets",
        "Deliveries",
        "Failures",
        "Quarantine",
        "Connections",
    ):
        assert f'<span class="label">{label}</span>' in nav
    assert '<svg class="ic"' in nav and 'aria-hidden="true"' in nav  # icons are decoration
    assert 'role="search"' in html and 'aria-keyshortcuts="Control+K Meta+K"' in html
    assert 'hx-get="/status" hx-trigger="load, every 15s"' in html
    for dialog in ('id="palette"', 'id="help"', 'id="confirm-dialog"'):
        assert dialog in html
    assert "P5: Packages and Registries" not in html  # a template comment: the slot, not shown


def test_the_theme_is_set_before_the_page_draws_and_kept_per_user(client: TestClient) -> None:
    html = client.get("/").text
    head = html.split("</head>")[0]
    assert '<script src="/static/theme.js"></script>' in head  # not deferred: it runs before the body draws
    assert '<html lang="en" data-user="op">' in html
    script = (STATIC / "theme.js").read_text(encoding="utf-8")
    assert '"fc-theme:"' in script and "data-user" in script and "localStorage" in script
    css = client.get("/static/app.css").text
    assert ':root:not([data-theme="dark"])' in css and ':root[data-theme="light"]' in css
    assert 'id="theme-toggle"' in html and 'id="theme-system"' in html


def test_the_status_bar_says_server_database_engine_environment_and_version(client: TestClient) -> None:
    bar = client.get("/status")
    assert bar.status_code == 200 and "<html" not in bar.text
    assert "Server up" in bar.text and "Database reachable" in bar.text
    assert "Engine local" in bar.text  # the world runs the local engine
    assert "env-dev" in bar.text and "Environment dev" in bar.text
    assert re.search(r"Feather Connect [\w.+-]+", bar.text)


def test_the_status_bar_needs_a_signed_in_user(world: World) -> None:
    anonymous = TestClient(create_app(state=world.state, loops=lambda s: []), follow_redirects=False)
    assert anonymous.get("/status").status_code == 303
    assert anonymous.get("/search.json?q=orders").status_code == 303


# The command palette's search.


def test_search_finds_pages_flows_datasets_routes_connections_and_runs_by_id(
    client: TestClient, world: World
) -> None:
    world.run()
    every = client.get("/search.json").json()["items"]
    assert {i["kind"] for i in every} == {"page"} and any(i["href"] == "/runs" for i in every)
    found = client.get("/search.json?q=orders")
    assert found.headers["content-type"].startswith("application/json")
    items = found.json()["items"]
    assert {"kind": "flow", "label": "orders", "href": "/flows/orders", "detail": "Flow"} in items
    assert any(i["kind"] == "dataset" and i["href"].endswith("/partitions") for i in items)
    warehouse = client.get("/search.json?q=wareh").json()["items"]
    assert any(i["kind"] == "route" and i["href"] == "/routes/warehouse/deliveries" for i in warehouse)
    assert any(i["kind"] == "connection" and i["label"] == "warehouse" for i in warehouse)
    run_id = world.store.runs.list_runs(limit=1)[0].run_id
    runs = client.get(f"/search.json?q={run_id[:6].lower()}").json()["items"]
    assert any(i["kind"] == "run" and i["href"] == f"/runs/{run_id}" for i in runs)
    assert client.get("/search.json?q=" + "x" * 201).status_code == 422  # bounded
    page = client.get("/search?q=orders").text  # the same, without JavaScript
    assert 'href="/flows/orders"' in page


def test_search_shows_an_operator_no_admin_page(world: World) -> None:
    operator = sign_in(create_app(state=world.state, loops=lambda s: []), "ann", "operator")
    labels = [i["label"] for i in operator.get("/search.json?q=user").json()["items"]]
    assert "Users" not in labels
    admin = sign_in(create_app(state=world.state, loops=lambda s: []), "boss", "admin")
    assert "Users" in [i["label"] for i in admin.get("/search.json?q=user").json()["items"]]


# A list page: tiles, search, the column chooser, the "only active" switch, and the rows it opens to.


def test_flows_list_has_tiles_a_search_a_column_chooser_and_the_active_switch(
    client: TestClient, world: World
) -> None:
    world.run()
    html = client.get("/flows").text
    for tile in ("Active</h3>", "Paused</h3>", "Failing</h3>"):
        assert tile in html
    assert 'class="spark"' in html and 'class="charts-toggle"' in html  # the charts, shown on asking
    assert 'details class="columns" data-columns-for="flows"' in html
    for column in ("owner", "tags", "schedule", "last_run", "started", "code"):
        assert f'data-col-toggle="{column}"' in html and f'data-col="{column}"' in html
    assert "data-select-all" in html and 'form="bulk"' in html and 'aria-controls="child-1"' in html
    assert "orders" in client.get("/flows?q=ORD").text.split("<tbody>")[-1]  # any case
    assert "No flows match" in client.get("/flows?q=nothing").text
    world.state.runtime.pause_flow("orders", actor="op")
    assert "No flows match" in client.get("/flows?active=1").text
    fragment = client.get("/flows?q=ord", headers={"HX-Request": "true"}).text
    assert fragment.lstrip().startswith('<div id="live" hx-get="/flows?q=ord"')  # the poll keeps the search


def test_runs_list_has_tiles_search_and_the_active_switch(client: TestClient, world: World) -> None:
    world.run()
    html = client.get("/runs").text
    for tile in ("Started today</h3>", "Running</h3>", "Failed today</h3>", "Queued</h3>"):
        assert tile in html
    run_id = world.store.runs.list_runs(limit=1)[0].run_id
    assert run_id[:8] in client.get(f"/runs?q={run_id[:5]}").text
    assert "No runs match" in client.get("/runs?q=nothing-like-it").text
    assert "No runs match" in client.get("/runs?active=1").text  # the run is over
    queued = world.state.runtime.start_run("orders")
    assert queued[:8] in client.get("/runs?active=1").text


def test_a_row_opens_to_its_routes_and_runs_or_its_tasks(client: TestClient, world: World) -> None:
    world.run()
    flow = client.get("/flows/orders/row")
    assert flow.status_code == 200 and "<html" not in flow.text
    assert 'href="/routes/warehouse/deliveries"' in flow.text and "Recent runs" in flow.text
    run_id = world.store.runs.list_runs(limit=1)[0].run_id
    run = client.get(f"/runs/{run_id}/row").text
    assert "extract" in run and "/tasks/" in run and "The whole run" in run
    assert client.get("/flows/nope/row").status_code == 404
    assert client.get("/runs/nope/row").status_code == 404


def test_every_list_page_has_tiles_a_search_and_a_footer(client: TestClient, world: World) -> None:
    world.run()
    for path in (
        "/datasets",
        "/deliveries",
        "/failures",
        "/quarantine",
        "/connections",
        "/datasets/orders/partitions",
    ):
        html = client.get(path).text
        assert 'class="tiles"' in html, path
        assert 'name="q"' in html and 'hx-select="#live"' in html, path
    assert "Showing 1 dataset." in client.get("/datasets").text
    assert 'href="/routes/warehouse/deliveries"' in client.get("/deliveries").text
    assert "No routes match" in client.get("/deliveries?q=nothing").text


# Bulk actions and who may do them.


def test_flows_are_paused_and_resumed_in_bulk(client: TestClient, world: World, csrf: Csrf) -> None:
    paused = client.post("/flows/bulk", data={"csrf_token": csrf(), "action": "pause", "name": ["orders"]})
    assert paused.status_code == 303 and paused.headers["location"] == "/flows?done=paused&n=1"
    assert world.state.runtime.is_flow_paused("orders")
    assert "1 flow paused." in client.get(paused.headers["location"]).text
    resumed = client.post("/flows/bulk", data={"csrf_token": csrf(), "action": "resume", "name": ["orders"]})
    assert resumed.status_code == 303 and not world.state.runtime.is_flow_paused("orders")
    audit = [a.action for a in world.store.audit.list_audit()]
    assert "pause_flow" in audit and "resume_flow" in audit  # each through the flow page's own action


def test_a_bulk_action_needs_csrf_a_known_action_and_rows(
    client: TestClient, world: World, csrf: Csrf
) -> None:
    forged = client.post("/flows/bulk", data={"action": "pause", "name": ["orders"]})
    assert forged.status_code == 403 and not world.state.runtime.is_flow_paused("orders")
    odd = client.post("/flows/bulk", data={"csrf_token": csrf(), "action": "delete", "name": ["orders"]})
    assert odd.status_code == 422 and "Choose run, pause or resume" in odd.text
    none = client.post("/flows/bulk", data={"csrf_token": csrf(), "action": "pause"})
    assert none.status_code == 422 and "Tick between 1 and" in none.text
    unknown = client.post(
        "/flows/bulk", data={"csrf_token": csrf(), "action": "pause", "name": ["orders", "nope"]}
    )
    assert unknown.status_code == 409 and "there is no flow named &#39;nope&#39;" in unknown.text
    assert client.post("/runs/bulk", data={"action": "cancel", "run_id": ["x"]}).status_code == 403


def test_a_token_that_may_only_publish_can_do_no_bulk_action(world: World) -> None:
    publisher = _bearer(world, "publish")
    refused = publisher.post("/flows/bulk", data={"action": "pause", "name": ["orders"]})
    assert refused.status_code == 403 and not world.state.runtime.is_flow_paused("orders")
    run_id = world.state.runtime.start_run("orders")
    cancel = publisher.post("/runs/bulk", data={"action": "cancel", "run_id": [run_id], "confirm": "yes"})
    assert cancel.status_code == 403 and world.store.runs.get_run(run_id).status == "queued"


def test_runs_are_cancelled_in_bulk_after_a_second_step(client: TestClient, world: World, csrf: Csrf) -> None:
    run_id = world.state.runtime.start_run("orders")
    asked = client.post("/runs/bulk", data={"csrf_token": csrf(), "action": "cancel", "run_id": [run_id]})
    assert asked.status_code == 200 and "Check before you confirm" in asked.text
    assert world.store.runs.get_run(run_id).status == "queued"  # only asked
    done = client.post(
        "/runs/bulk", data={"csrf_token": csrf(), "action": "cancel", "run_id": [run_id], "confirm": "yes"}
    )
    assert done.status_code == 303 and done.headers["location"] == "/runs?done=cancelled&n=1"
    assert world.store.runs.get_run(run_id).status == "cancelled"
    again = client.post(
        "/runs/bulk", data={"csrf_token": csrf(), "action": "cancel", "run_id": [run_id], "confirm": "yes"}
    )
    assert again.status_code == 409 and "already over" in again.text


def test_actions_a_role_cannot_do_are_not_shown(world: World) -> None:
    operator = sign_in(create_app(state=world.state, loops=lambda s: []), "ann", "operator")
    flows = operator.get("/flows").text
    assert 'value="pause"' in flows and 'value="run"' in flows  # an operator runs and pauses
    assert 'href="/users"' not in flows
    secrets = operator.get("/secrets").text
    assert "Replace" not in secrets.split("<main")[1]  # only an admin replaces a secret


# Secrets: names and when they were set, never a value.


def test_the_secrets_page_shows_when_and_by_whom_and_never_a_value(world: World) -> None:
    world.state.vault = Vault(world.store.secrets, "dev", MasterKey.parse(new_master_key()))
    admin = sign_in(create_app(state=world.state, loops=lambda s: []), "boss", "admin")
    Environment(world.store, world.clock, "dev", world.state.vault).set_secret(
        ADMIN, "zakya/client_secret", "s3cr3t"
    )
    page = admin.get("/secrets").text
    assert "zakya/client_secret" in page and "set, updated" in page and ">root<" in page
    assert "s3cr3t" not in page and 'action="/secrets/zakya/client_secret"' in page
    token = re.search(r'name="csrf_token" value="([^"]+)"', page)
    assert token
    replaced = admin.post(
        "/secrets/zakya/client_secret",
        data={"csrf_token": token.group(1), "value": "n3w", "back": "/secrets"},
    )
    assert replaced.status_code == 303 and replaced.headers["location"] == "/secrets"
    elsewhere = admin.post(
        "/secrets/zakya/client_secret",
        data={"csrf_token": token.group(1), "value": "n3w", "back": "//evil.example"},
    )
    assert elsewhere.headers["location"] == "/connections"  # only back to a page of ours
    assert json.dumps(admin.get("/search.json?q=zakya").json()).count("s3cr3t") == 0

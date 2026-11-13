import re
from collections.abc import Callable

import httpx2 as httpx  # what Starlette's TestClient answers with once httpx2 is installed (mcp brings it)
from fastapi.testclient import TestClient

from feather_connect.core.errors import PermanentError
from feather_server.services.reads import (
    DatasetSummary,
    FlowDetail,
    FlowSummary,
    RouteSummary,
)
from feather_server.testing import SECRET, World
from feather_server.web.flow_graph import flow_graph


def started_run(client: TestClient, response: httpx.Response) -> str:
    """The run that the redirect after a run starts names."""
    location = response.headers["location"]
    match = re.search(r"started=([0-9a-zA-Z_-]+)", location)
    assert match, location
    return match.group(1)


def test_flows_list_shows_status_last_run_freshness_owner_tags_sha(client: TestClient, world: World) -> None:
    assert "Never run" in client.get("/flows").text
    world.run()
    html = client.get("/flows").text
    assert '<th scope="row"><a href="/flows/orders">orders</a></th>' in html
    assert "Succeeded" in html and "just now" in html  # last run, and how fresh the data is
    assert ">ops<" in html and "retail" in html  # owner and tags
    assert world.state.project.code_sha[:8] in html
    assert "every 15 min" in html  # what starts it


def test_filters_live_in_the_query_string(client: TestClient, world: World) -> None:
    world.run()
    assert "orders" in client.get("/flows?status=succeeded").text
    assert "orders" not in client.get("/flows?status=failed").text.split("<tbody>")[-1]
    assert "No flows match" in client.get("/flows?owner=nobody").text
    assert "orders" in client.get("/flows?owner=ops&tag=retail").text
    assert "No flows match" in client.get("/flows?tag=finance").text
    page = client.get("/flows?owner=ops").text
    assert '<form class="filters" method="get" action="/flows" ' in page  # works without JavaScript
    world.sink("warehouse").fail_next(PermanentError("no such table"))
    world.run()
    assert "orders" in client.get("/flows?status=failed").text.split("<tbody>")[-1]
    world.state.runtime.pause_flow("orders", actor="op")
    assert "orders" in client.get("/flows?status=paused").text.split("<tbody>")[-1]


def test_the_flows_list_refreshes_itself(client: TestClient, world: World) -> None:
    html = client.get("/flows?owner=ops").text
    assert 'hx-get="/flows?owner=ops"' in html and 'hx-trigger="every 10s"' in html
    fragment = client.get("/flows?owner=ops", headers={"HX-Request": "true"}).text
    assert "<html" not in fragment and "orders" in fragment


def test_detail_shows_graph_config_layers_runs_versions(client: TestClient, world: World) -> None:
    run_id = world.run()
    html = client.get("/flows/orders").text
    assert "<svg" in html and "<title>" in html  # the graph, with titles for what it draws
    for title in ("Dataset orders", "Route warehouse", "Route gofrugal", "Destination gofrugal"):
        assert f"<title>{title}" in html
    assert "env/dev.yaml" in html and "feather.yaml" in html  # each value with the layer that set it
    assert "flows.orders.triggers.every" in html
    assert run_id[:8] in html and "Succeeded" in html
    assert "18 Oct 2026" in html  # versions, in business words
    assert "Sealed" in html


def test_the_runs_of_a_flow_refresh_themselves(client: TestClient, world: World) -> None:
    html = client.get("/flows/orders").text
    assert 'hx-get="/flows/orders"' in html and 'hx-trigger="every 10s"' in html
    run_id = world.run()
    fragment = client.get("/flows/orders", headers={"HX-Request": "true"}).text
    assert "<html" not in fragment and run_id[:8] in fragment and "<svg" not in fragment


def test_config_never_shows_a_secret(client: TestClient, world: World) -> None:
    html = client.get("/flows/orders").text
    assert SECRET not in html
    assert "connections.warehouse.auth.token" in html and "***" in html


def test_an_unknown_flow_is_a_not_found_page(client: TestClient) -> None:
    response = client.get("/flows/nothing")
    assert response.status_code == 404 and "No flow named" in response.text


def test_run_now_starts_a_run_and_shows_a_link(
    client: TestClient, world: World, csrf: Callable[[], str]
) -> None:
    response = client.post("/flows/orders/run", data={"csrf_token": csrf(), "reason": "month end"})
    assert response.status_code == 303
    run_id = started_run(client, response)
    assert [r.run_id for r in world.store.runs.list_runs(flow="orders")] == [run_id]
    page = client.get(response.headers["location"]).text
    assert f'href="/runs/{run_id}"' in page
    (row,) = [a for a in world.store.audit.list_audit(10) if a.action == "run_flow"]
    assert (row.actor, row.request["reason"]) == ("op", "month end")


def test_run_now_needs_csrf_and_a_session(client: TestClient, world: World) -> None:
    assert client.post("/flows/orders/run", data={}).status_code == 403
    assert world.store.runs.list_runs() == []
    anonymous = TestClient(client.app, follow_redirects=False)
    assert anonymous.post("/flows/orders/run", data={}).status_code in (303, 403)


def test_pause_and_resume_need_csrf_and_record_a_reason(
    client: TestClient, world: World, csrf: Callable[[], str]
) -> None:
    assert client.post("/flows/orders/pause", data={"reason": "x"}).status_code == 403
    assert not world.state.runtime.is_flow_paused("orders")
    response = client.post("/flows/orders/pause", data={"csrf_token": csrf(), "reason": "bad data upstream"})
    assert response.status_code == 303
    assert world.state.runtime.is_flow_paused("orders")
    (paused,) = [o for o in world.store.overrides.list_all() if o.target == "flow:orders"]
    assert paused.reason == "bad data upstream" and paused.created_by == "op"
    page = client.get("/flows/orders").text
    assert "Paused" in page and "Resume" in page
    blocked = client.post("/flows/orders/run", data={"csrf_token": csrf()})
    assert blocked.status_code == 409 and "paused" in blocked.text
    assert client.post("/flows/orders/resume", data={"csrf_token": csrf()}).status_code == 303
    assert not world.state.runtime.is_flow_paused("orders")
    actions = [a.action for a in world.store.audit.list_audit(10)]
    assert "pause_flow" in actions and "resume_flow" in actions


def test_backfill_preview_shows_the_impact_before_confirming(
    client: TestClient, world: World, csrf: Callable[[], str]
) -> None:
    form = {"csrf_token": csrf(), "start": "2026-10-10", "end": "2026-10-12", "max_concurrent": "2"}
    preview = client.post("/flows/orders/backfill/preview", data=form)
    assert preview.status_code == 200
    assert "Check before you confirm" in preview.text and "10 Oct 2026" in preview.text
    assert re.search(r"<dt>Partitions</dt><dd>3</dd>", preview.text)
    assert world.store.runs.list_runs() == []  # nothing has started
    token = re.search(r'name="token" value="([^"]+)"', preview.text)
    assert token
    confirmed = client.post(
        "/flows/orders/backfill/confirm",
        data={"csrf_token": csrf(), "token": token.group(1), "reason": "catch up"},
    )
    assert confirmed.status_code == 303
    run_id = started_run(client, confirmed)
    assert world.store.runs.get_run(run_id).params["max_concurrent"] == 2
    again = client.post(
        "/flows/orders/backfill/confirm", data={"csrf_token": csrf(), "token": token.group(1)}
    )
    assert again.status_code == 409 and "used already" in again.text  # the token works once


def test_backfill_refuses_a_bad_range_and_a_made_up_token(
    client: TestClient, world: World, csrf: Callable[[], str]
) -> None:
    bad = {"csrf_token": csrf(), "start": "2026-10-12", "end": "2026-10-10", "max_concurrent": "2"}
    response = client.post("/flows/orders/backfill/preview", data=bad)
    assert response.status_code == 422 and "start must be on or before end" in response.text
    nonsense = {**bad, "start": "tomorrow"}
    assert client.post("/flows/orders/backfill/preview", data=nonsense).status_code == 422
    forged = client.post("/flows/orders/backfill/confirm", data={"csrf_token": csrf(), "token": "x.y"})
    assert forged.status_code == 422 and world.store.runs.list_runs() == []


def test_two_shas_for_one_flow_show_a_warning(client: TestClient, world: World) -> None:
    world.run()
    assert "different code" not in client.get("/flows/orders").text
    world.store.workers.heartbeat("w-old", {}, "an-older-sha", 4)
    html = client.get("/flows/orders").text
    assert "different code" in html and "an-older-sha"[:8] in html


def test_the_graph_escapes_names() -> None:
    summary = FlowSummary(
        "<b>f</b>",
        None,
        (),
        (),
        True,
        False,
        (DatasetSummary("<script>d", "c", "replace", "normal"),),
        (RouteSummary('r"><x', "<i>dest", True),),
        None,
    )
    svg = flow_graph(FlowDetail(summary, (), ()))
    assert "<script>" not in svg and "<i>" not in svg and '"><x' not in svg
    assert "&lt;script&gt;d" in svg and svg.startswith("<svg") and "<title>" in svg

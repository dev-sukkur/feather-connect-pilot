import re
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.rows import DictRow

from feather_connect.core.errors import PermanentError
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_server.app import AppState, create_app
from feather_server.settings import Settings
from feather_server.testing import World, day, sign_in

Csrf = Callable[[], str]


def cell(html: str, label: str, route: str) -> str:
    """The text of the cell of the row `label` under the column `route`."""
    head = re.search(r"<thead>(.*?)</thead>", html, re.DOTALL)
    assert head
    columns = [
        re.sub(r"<[^>]+>", "", c).strip()
        for c in re.findall(r"<th[^>]*>(.*?)</th>", head.group(1), re.DOTALL)
    ]
    row = next(r for r in re.findall(r"<tr>(.*?)</tr>", html.split("<tbody>")[1], re.DOTALL) if label in r)
    cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", cells[columns.index(route)])).strip()


def test_grid_shows_a_cell_per_partition_and_route_with_business_labels(
    client: TestClient, world: World
) -> None:
    world.run()
    html = client.get("/datasets/orders/partitions").text
    assert (
        "18 Oct 2026" in html and "17 Oct 2026" in html and "date=2026-10-18" in html
    )  # the key is in the tooltip
    assert html.index("18 Oct 2026") < html.index("17 Oct 2026")  # newest first
    assert cell(html, "18 Oct 2026", "warehouse").startswith("✓ Applied")
    assert cell(html, "18 Oct 2026", "gofrugal").startswith("✓ Applied")
    assert '<th scope="col">warehouse</th>' in html and 'class="table-wrap scroll"' in html
    assert "<caption>" in html  # a table says what it is
    assert client.get("/datasets/nothing/partitions").status_code == 404


def test_a_cell_says_what_it_is_in_words_not_only_colour(client: TestClient, world: World) -> None:
    from feather_connect.core.errors import PermanentError

    world.sink("warehouse").fail_next(PermanentError("no such table"))
    world.run([day(17)])
    html = client.get("/datasets/orders/partitions").text
    assert cell(html, "17 Oct 2026", "warehouse").startswith("✕ Failed")
    assert "Applied" in cell(html, "17 Oct 2026", "gofrugal")


def test_cell_links_drill_down_to_deliveries(client: TestClient, world: World) -> None:
    world.run()
    html = client.get("/datasets/orders/partitions").text
    assert 'href="/routes/warehouse/deliveries?partition=date%3D2026-10-18"' in html
    assert 'href="/routes/gofrugal/deliveries?partition=date%3D2026-10-17"' in html


def test_the_grid_is_paginated_by_cursor_and_filters_by_date(client: TestClient, world: World) -> None:
    world.run()
    assert "Next page" not in client.get("/datasets/orders/partitions").text  # 2 partitions fit on a page
    since = client.get("/datasets/orders/partitions?since=2026-10-19").text
    assert "No partitions sealed since 2026-10-19" in since
    assert client.get("/datasets/orders/partitions?since=soon").status_code == 422
    after = client.get(f"/datasets/orders/partitions?cursor={quote(day(18))}").text
    assert "date=2026-10-17" in after and "date=2026-10-18" not in after


def test_rerun_selected_partitions_with_preview(client: TestClient, world: World, csrf: Csrf) -> None:
    world.run()
    runs = len(world.store.runs.list_runs())
    chosen = {"csrf_token": csrf(), "partition": [day(17), day(18)]}
    preview = client.post("/datasets/orders/partitions/rerun/preview", data=chosen)
    assert preview.status_code == 200 and "Check before you confirm" in preview.text
    assert "<dt>Partitions</dt><dd>2</dd>" in preview.text
    assert len(world.store.runs.list_runs()) == runs  # a preview starts nothing
    token = re.search(r'name="token" value="([^"]+)"', preview.text)
    assert token
    confirmed = client.post(
        "/datasets/orders/partitions/rerun/confirm",
        data={"csrf_token": csrf(), "token": token.group(1), "reason": "source fixed"},
    )
    assert confirmed.status_code == 303 and confirmed.headers["location"].startswith("/flows/orders?started=")
    newest = world.store.runs.list_runs(flow="orders", limit=1)[0]
    assert sorted(newest.params["partitions"]) == [day(17), day(18)] and newest.reason == "source fixed"
    assert newest.trigger == "rerun"


def test_rerun_needs_a_selection_a_token_and_csrf(client: TestClient, world: World, csrf: Csrf) -> None:
    nothing = client.post("/datasets/orders/partitions/rerun/preview", data={"csrf_token": csrf()})
    assert nothing.status_code == 422 and "choose at least one partition" in nothing.text
    assert (
        client.post("/datasets/orders/partitions/rerun/preview", data={"partition": day(17)}).status_code
        == 403
    )
    forged = client.post(
        "/datasets/orders/partitions/rerun/confirm", data={"csrf_token": csrf(), "token": "a.b"}
    )
    assert forged.status_code == 422 and world.store.runs.list_runs() == []


def test_verify_reports_differences_and_changes_nothing(client: TestClient, world: World, csrf: Csrf) -> None:
    world.run()
    source = world.state.runtime.dataset("orders").source
    rows = [{"id": 171, "date": "2026-10-17"}, {"id": 173, "date": "2026-10-17"}]  # 172 is gone, 173 is new
    source.load(rows + [{"id": 181, "date": "2026-10-18"}, {"id": 182, "date": "2026-10-18"}])  # type: ignore[attr-defined]
    runs, versions = len(world.store.runs.list_runs()), world.store.versions.list_versions("orders")
    url = f"/datasets/orders/partitions/{quote(day(17), safe='')}/verify"
    found = client.post(url, data={"csrf_token": csrf()})
    assert found.status_code == 200
    assert (
        "1 records would be added or changed and 1 deleted" in found.text
        and "Nothing was changed" in found.text
    )
    assert (
        len(world.store.runs.list_runs()) == runs and world.store.versions.list_versions("orders") == versions
    )
    clean = client.post(
        f"/datasets/orders/partitions/{quote(day(18), safe='')}/verify", data={"csrf_token": csrf()}
    )
    assert "matches its source" in clean.text
    audited = [a.result for a in world.store.audit.list_audit(20) if a.action == "verify_partition"]
    assert audited == [
        "ok: 0 upserts and 0 deletes differ",
        "ok: 1 upserts and 1 deletes differ",
    ]  # newest first
    assert client.post(url, data={}).status_code == 403
    assert (
        client.post("/datasets/orders/partitions/nonsense/verify", data={"csrf_token": csrf()}).status_code
        == 422
    )


def test_replay_to_one_route_leaves_others_alone(client: TestClient, world: World, csrf: Csrf) -> None:
    world.run()
    preview = client.post(
        f"/routes/warehouse/partitions/{quote(day(17), safe='')}/replay/preview",
        data={"csrf_token": csrf(), "dataset": "orders"},
    )
    assert preview.status_code == 200 and "<dt>Deliveries</dt><dd>1</dd>" in preview.text
    assert "Replay to warehouse" in preview.text
    token = re.search(r'name="token" value="([^"]+)"', preview.text)
    action = re.search(r'<form method="post" action="([^"]*replay/confirm[^"]*)"', preview.text)
    assert token and action
    done = client.post(
        action.group(1).replace("&amp;", "&"),
        data={"csrf_token": csrf(), "token": token.group(1), "reason": "x"},
    )
    assert done.status_code == 303 and "replayed=1" in done.headers["location"]
    world.engine.run_until_idle()
    replays = {(d.route, d.partition): d.replays for d in world.store.ledger.list_deliveries(limit=50)}
    assert replays[("warehouse", day(17))] == 1
    assert replays[("gofrugal", day(17))] == 0 and replays[("warehouse", day(18))] == 0
    assert "Replay started for 1 delivery" in client.get(done.headers["location"]).text


def grid_preview(client: TestClient, csrf: Csrf, partition: str) -> tuple[str, str]:
    preview = client.post(
        f"/routes/warehouse/partitions/{quote(partition, safe='')}/replay/preview",
        data={"csrf_token": csrf(), "dataset": "orders"},
    )
    assert preview.status_code == 200, preview.text
    assert "<dt>Deliveries</dt><dd>1</dd>" in preview.text  # one version, however many came before it
    token = re.search(r'name="token" value="([^"]+)"', preview.text)
    action = re.search(r'<form method="post" action="([^"]*replay/confirm[^"]*)"', preview.text)
    assert token and action
    return action.group(1).replace("&amp;", "&"), token.group(1)


def new_version_of_day_17(world: World, value: str) -> str:
    source: Any = world.state.runtime.dataset("orders").source
    source.load([{**dict(row), "note": value} for row in source._rows])
    world.run([day(17)])
    latest = world.store.versions.latest_sealed("orders", day(17))
    assert latest is not None
    return latest.version_id


def test_grid_replay_takes_only_the_latest_version_of_a_partition(
    client: TestClient, world: World, csrf: Csrf
) -> None:
    world.run()
    first = world.store.versions.latest_sealed("orders", day(17))
    assert first is not None
    second = new_version_of_day_17(world, "second")
    assert second != first.version_id
    action, token = grid_preview(client, csrf, day(17))
    done = client.post(action, data={"csrf_token": csrf(), "token": token})
    assert done.status_code == 303 and "replayed=1" in done.headers["location"]
    world.engine.run_until_idle()
    assert world.store.ledger.get("warehouse", first.version_id).replays == 0
    assert world.store.ledger.get("warehouse", second).replays == 1


def test_grid_preview_survives_a_new_version_and_replays_the_latest(
    client: TestClient, world: World, csrf: Csrf
) -> None:
    world.run()
    before = world.store.versions.latest_sealed("orders", day(17))
    assert before is not None
    action, token = grid_preview(client, csrf, day(17))
    newer = new_version_of_day_17(world, "landed meanwhile")
    done = client.post(action, data={"csrf_token": csrf(), "token": token})
    assert done.status_code == 303 and "replayed=1" in done.headers["location"]
    world.engine.run_until_idle()
    assert world.store.ledger.get("warehouse", before.version_id).replays == 0  # no older data goes out
    assert world.store.ledger.get("warehouse", newer).replays == 1


def test_a_confirm_that_moves_nothing_says_why_and_not_success(
    client: TestClient, world: World, csrf: Csrf
) -> None:
    world.run()
    action, token = grid_preview(client, csrf, day(17))
    world.sink("warehouse").fail_next(PermanentError("no such table"))
    newer = new_version_of_day_17(world, "failed meanwhile")
    assert world.store.ledger.get("warehouse", newer).status == "failed"
    done = client.post(action, data={"csrf_token": csrf(), "token": token})
    assert done.status_code == 409 and newer in done.text and "Replay started" not in done.text


def test_grid_replay_of_an_aged_out_version_is_refused_at_the_preview(
    client: TestClient, world: World, csrf: Csrf
) -> None:
    world.run()
    latest = world.store.versions.latest_sealed("orders", day(17))
    assert latest is not None
    world.state.spool.expire_chunks(latest.version_id, "data")
    response = client.post(
        f"/routes/warehouse/partitions/{quote(day(17), safe='')}/replay/preview",
        data={"csrf_token": csrf(), "dataset": "orders"},
    )
    assert response.status_code == 422 and "aged out" in response.text
    assert world.store.ledger.get("warehouse", latest.version_id).status == "applied"


def test_replay_of_a_partition_that_was_never_delivered_is_refused(
    client: TestClient, world: World, csrf: Csrf
) -> None:
    world.run()
    response = client.post(
        f"/routes/warehouse/partitions/{quote('date=2026-01-01', safe='')}/replay/preview",
        data={"csrf_token": csrf(), "dataset": "orders"},
    )
    assert response.status_code == 422 and "matches nothing" in response.text


def test_the_grid_escapes_what_it_shows(client: TestClient, world: World) -> None:
    response = client.get("/datasets/orders/partitions?since=<script>alert(1)</script>")
    assert response.status_code == 422 and "<script>alert" not in response.text


# Review Focus 4: a grid of tens of thousands of partitions, on Postgres.

BIG = 50_000


@pytest.fixture
def seed_grid(world: World, pg_url: str, pg: psycopg.Connection[DictRow], pg_connect: Any) -> TestClient:
    """A signed-in browser on a Postgres store that holds `BIG` sealed partitions of `orders`, each delivered to both
    routes."""
    store, spool = pg_connect(pg_url, clock=world.clock)
    engine = LocalEngine(world.clock)
    project = world.state.project
    runtime = Runtime(project.flows, store, spool, engine, code_sha="sha", clock=world.clock)
    state = AppState(
        Settings(secret_key="test-secret-key", engine="local"),
        project,
        store,
        spool,
        engine,
        runtime,
        clock=world.clock,
    )
    pg.execute(
        """insert into feather.version (tenant_id, version_id, dataset, partition, run_id, complete, upserts, deletes,
               content_hash, code_sha, status, seal_seq, sealed_at)
           select 'default', 'v' || n, 'orders', 'date=' || to_char(date '1900-01-01' + n, 'YYYY-MM-DD'), 'r', true,
                  2, 0, 'h', 'sha', 'sealed', nextval('feather.seal_seq'), now()
           from generate_series(0, %s - 1) n""",
        (BIG,),
    )
    pg.execute(
        """insert into feather.delivery (tenant_id, route, version_id, dataset, partition, seal_seq, status)
           select 'default', r.route, v.version_id, v.dataset, v.partition, v.seal_seq, 'applied'
           from feather.version v cross join (values ('warehouse'), ('gofrugal')) r(route)"""
    )
    return sign_in(create_app(state=state, loops=lambda s: []))


@pytest.mark.postgres
def test_50_000_partitions_page_renders_quickly(seed_grid: TestClient) -> None:  # Review Focus 4
    started = time.perf_counter()
    response = seed_grid.get("/datasets/orders/partitions")
    seconds = time.perf_counter() - started
    assert response.status_code == 200
    assert seconds < 2, f"the first page took {seconds:.2f} s"
    assert response.text.count('name="partition"') == 100 and 'rel="next"' in response.text
    cursor = re.search(r'rel="next" href="([^"]+)"', response.text)
    assert cursor
    started = time.perf_counter()
    second = seed_grid.get(cursor.group(1).replace("&amp;", "&"))
    assert time.perf_counter() - started < 2 and second.text.count('name="partition"') == 100
    first_keys = re.findall(r'name="partition" value="([^"]+)"', response.text)
    second_keys = re.findall(r'name="partition" value="([^"]+)"', second.text)
    assert second_keys[0] < first_keys[-1] and not set(first_keys) & set(second_keys)

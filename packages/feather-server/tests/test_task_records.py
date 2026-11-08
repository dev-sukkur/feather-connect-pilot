"""The Records panel of a run's timeline: an extract task's records with what extraction turned down, an apply task's
records with what its route made of each, a page at a time; on the dashboard and in the API."""

import re
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from feather_connect.connectors import memory
from feather_connect.runtime.extract import version_id_of
from feather_connect.storage.port import ResponseRow, TaskRow
from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.services import task_records
from feather_server.services.task_records import NoRecords, TaskRecordReader
from feather_server.testing import World, day

API = "/api/v1"


def _source(world: World) -> memory.Source:
    source = world.state.runtime.dataset("orders").source
    assert isinstance(source, memory.Source)
    return source


def _task(world: World, run_id: str, kind: str, route: str | None = None) -> TaskRow:
    return next(
        t
        for t in world.store.runs.list_tasks(run_id)
        if t.kind == kind and (route is None or t.route == route)
    )


def _url(run_id: str, task: TaskRow) -> str:
    return f"/runs/{run_id}/tasks/{task.task_id}/records"


def _api(world: World) -> TestClient:
    token = issue_token(world.store, "ci", timedelta(days=1), clock=world.clock)
    return TestClient(
        create_app(state=world.state, loops=lambda s: []), headers={"Authorization": f"Bearer {token}"}
    )


# The service.


def test_an_extract_tasks_records_say_what_changed(world: World) -> None:
    first = world.run([day(18)])
    reader = TaskRecordReader(world.state)
    found = reader.read(first, _task(world, first, "extract").task_id)
    assert [(r.sighting.key, r.op) for r in found.records] == [("181", "insert"), ("182", "insert")]
    assert found.records[0].payload == {"id": 181, "date": "2026-10-18"} and found.records[0].in_spool
    assert not found.not_indexed and found.next_cursor is None
    _source(world).load(
        [{"id": 181, "date": "2026-10-18", "note": "paid"}, {"id": 182, "date": "2026-10-18"}]
    )
    second = world.run([day(18)])
    again = reader.read(second, _task(world, second, "extract").task_id)
    # A replace reads the whole slice again: 181 changed, and 182 is there as it was.
    assert [(r.sighting.key, r.op) for r in again.records] == [("181", "update"), ("182", "unchanged")]


def test_a_task_of_another_run_or_without_records_is_refused(world: World) -> None:
    run_id = world.run([day(18)])
    reader = TaskRecordReader(world.state)
    extract = _task(world, run_id, "extract")
    with pytest.raises(KeyError):
        reader.read("another-run", extract.task_id)
    with pytest.raises(KeyError):
        reader.read(run_id, "no-such-task")
    plan = next((t for t in world.store.runs.list_tasks(run_id) if t.kind not in ("extract", "apply")), None)
    if plan is not None:
        with pytest.raises(NoRecords):
            reader.read(run_id, plan.task_id)


# The dashboard.


def test_the_run_page_links_each_extract_and_apply_to_its_records(client: TestClient, world: World) -> None:
    run_id = world.run([day(18)])
    html = client.get(f"/runs/{run_id}").text
    for task in (_task(world, run_id, "extract"), _task(world, run_id, "apply", "warehouse")):
        url = _url(run_id, task)
        assert f'href="{url}" hx-get="{url}" hx-target="#records-panel"' in html
    assert 'id="records-panel"' in html


def test_the_extract_panel(client: TestClient, world: World) -> None:
    run_id = world.run([day(18)])
    task = _task(world, run_id, "extract")
    version = world.store.versions.latest_sealed("orders", day(18))
    assert version is not None
    world.store.quarantine.add(None, version.version_id, "18x", "date is not a date", "orders", day(18))
    page = client.get(_url(run_id, task))
    assert page.status_code == 200
    html = page.text
    assert "<h1>Records of a extract task</h1>" not in html  # said properly
    assert '<th scope="row"><code>181</code></th>' in html and "insert" in html
    assert "&#34;date&#34;: &#34;2026-10-18&#34;" in html  # the payload, as escaped pretty JSON
    assert 'href="/trace?dataset=orders&amp;key=181"' in html and "Full trace" in html
    assert f'href="/versions/{version.version_id}"' in html
    assert "Turned down when read" in html and "date is not a date" in html and "18x" in html
    panel = client.get(_url(run_id, task), headers={"HX-Request": "true"}).text  # the panel alone, for HTMX
    assert "<html" not in panel and 'id="h-task-records"' in panel


def test_the_apply_panel_shows_the_route_result_and_answer(client: TestClient, world: World) -> None:
    world.sink("warehouse").reject("182", "customer is missing")
    run_id = world.run([day(18)])
    task = _task(world, run_id, "apply", "warehouse")
    version = world.store.versions.latest_sealed("orders", day(18))
    assert version is not None
    world.store.responses.record(
        [ResponseRow("warehouse", version.version_id, 0, "181", 201, {"erp_id": "ERP-7"})]
    )
    html = client.get(_url(run_id, task)).text
    assert "Records sent to warehouse" in html
    row_181 = re.search(r"<code>181</code></th>(.*?)</tr>", html, re.DOTALL)
    row_182 = re.search(r"<code>182</code></th>(.*?)</tr>", html, re.DOTALL)
    assert row_181 and row_182
    assert "Applied" in row_181.group(1) and "201" in row_181.group(1) and "ERP-7" in row_181.group(1)
    assert "Rejected" in row_182.group(1) and "customer is missing" in row_182.group(1)


def test_a_record_whose_chunk_retention_removed(client: TestClient, world: World) -> None:
    run_id = world.run([day(18)])
    version = world.store.versions.latest_sealed("orders", day(18))
    assert version is not None
    world.state.spool.expire_version(version.version_id)
    html = client.get(_url(run_id, _task(world, run_id, "extract"))).text
    assert "<code>181</code>" in html and "No longer in the spool: retention removed its chunk." in html


def test_a_version_sealed_before_the_index(client: TestClient, world: World) -> None:
    run_id = world.run([day(18)])
    world.store.versions._d.keys.clear()  # type: ignore[attr-defined]  # as a version sealed before 0013 is
    html = client.get(_url(run_id, _task(world, run_id, "extract"))).text
    assert "Records of this version were not indexed (sealed before tracing was added)." in html


def test_paging_and_search(client: TestClient, world: World, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(task_records, "PAGE", 1)
    run_id = world.run([day(18)])
    url = _url(run_id, _task(world, run_id, "extract"))
    first = client.get(url).text
    assert "<code>181</code>" in first and "<code>182</code>" not in first
    found = re.search(r'rel="next" href="([^"]+)"', first)
    assert found
    second = client.get(found.group(1).replace("&amp;", "&")).text
    assert "<code>182</code>" in second and "<code>181</code>" not in second and 'rel="next"' not in second
    assert "First page" in second
    searched = client.get(f"{url}?q=182").text
    assert "<code>182</code>" in searched and "<code>181</code>" not in searched
    assert "No record's key starts with" in client.get(f"{url}?q=9").text
    assert client.get(f"{url}?cursor=nonsense").status_code == 422


def test_a_payload_is_escaped(client: TestClient, world: World) -> None:
    _source(world).load([{"id": 181, "date": "2026-10-18", "note": "<script>alert(1)</script>"}])
    run_id = world.run([day(18)])
    html = client.get(_url(run_id, _task(world, run_id, "extract"))).text
    assert "<script>alert(1)</script>" not in html and "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_the_panel_needs_a_sign_in_and_a_real_task(client: TestClient, world: World) -> None:
    run_id = world.run([day(18)])
    url = _url(run_id, _task(world, run_id, "extract"))
    anonymous = TestClient(create_app(state=world.state, loops=lambda s: []), follow_redirects=False)
    answer = anonymous.get(url)
    assert answer.status_code in (302, 303) and answer.headers["location"].startswith("/login")
    assert client.get(f"/runs/{run_id}/tasks/no-such-task/records").status_code == 404
    assert client.get(url.replace(run_id, "another-run")).status_code == 404


# The API.


def test_the_api_gives_the_same_a_page_at_a_time(world: World) -> None:
    world.sink("warehouse").reject("182", "customer is missing")
    run_id = world.run([day(18)])
    version = world.store.versions.latest_sealed("orders", day(18))
    assert version is not None
    world.store.responses.record(
        [ResponseRow("warehouse", version.version_id, 0, "181", 201, {"erp_id": "ERP-7"})]
    )
    api = _api(world)
    apply = _task(world, run_id, "apply", "warehouse")
    body = api.get(f"{API}{_url(run_id, apply)}?limit=1").json()
    assert (body["kind"], body["dataset"], body["route"]) == ("apply", "orders", "warehouse")
    assert body["version_ids"] == [version.version_id] and body["not_indexed"] is False
    [item] = body["items"]
    assert item["key"] == "181" and item["op"] == "insert" and item["result"] == "applied"
    assert item["response"] == {"status": 201, "fields": {"erp_id": "ERP-7"}}
    assert item["payload"] == {"id": 181, "date": "2026-10-18"} and item["in_spool"] is True
    rest = api.get(f"{API}{_url(run_id, apply)}", params={"limit": 1, "cursor": body["next_cursor"]}).json()
    assert [(i["key"], i["result"], i["error"]) for i in rest["items"]] == [
        ("182", "rejected", "customer is missing")
    ]
    assert rest["next_cursor"] is None
    extract = _task(world, run_id, "extract")
    assert [
        i["key"] for i in api.get(f"{API}{_url(run_id, extract)}", params={"q": "18"}).json()["items"]
    ] == [
        "181",
        "182",
    ]
    assert version_id_of(run_id, "orders", day(18)) == version.version_id


def test_the_api_errors(world: World) -> None:
    run_id = world.run([day(18)])
    api = _api(world)
    url = f"{API}{_url(run_id, _task(world, run_id, 'extract'))}"
    assert api.get(f"{API}/runs/{run_id}/tasks/nope/records").status_code == 404
    assert api.get(url, params={"cursor": "nonsense"}).status_code == 422
    assert api.get(url, params={"limit": 0}).status_code == 422
    assert TestClient(create_app(state=world.state, loops=lambda s: [])).get(url).status_code == 401

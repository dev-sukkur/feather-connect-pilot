"""Tracing a record: every version that held its key, what each route did with it, and what the operators did about it,
as one timeline on the dashboard (`/trace`) and in the API (`/datasets/{d}/records/{key}/trace`)."""

from dataclasses import dataclass
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from feather_connect.connectors import memory
from feather_connect.core.errors import PermanentError
from feather_connect.storage.port import ResponseRow
from feather_server.app import create_app
from feather_server.auth import Actor, issue_token
from feather_server.services.actions import DeliverySelection
from feather_server.testing import World, day

ANN = Actor("human", "ann")
API = "/api/v1"


@dataclass
class Story:
    first: str  # the version that read 181 first: the warehouse rejected it and gofrugal failed, then was retried
    second: str  # the version that read it again, changed
    run_first: str
    run_second: str


def _source(world: World) -> memory.Source:
    source = world.state.runtime.dataset("orders").source
    assert isinstance(source, memory.Source)
    return source


def tell(world: World) -> Story:
    """Record 181 of 18 October: read, rejected by the warehouse, failed at gofrugal and retried, then read again."""
    world.sink("warehouse").reject("181", "customer is missing")
    world.sink("gofrugal").fail_next(PermanentError("gofrugal is down"))
    run_first = world.run([day(18)])
    first = world.store.versions.latest_sealed("orders", day(18))
    assert first is not None
    world.store.responses.record(  # what an HTTP sink keeps of a rejection: the status and the body it sent
        [ResponseRow("warehouse", first.version_id, 0, "181", 422, {}, sent={"id": 181, "customer": None})]
    )
    world.clock.advance(timedelta(minutes=5))
    retry = world.actions.preview_deliveries(ANN, "retry", DeliverySelection(route="gofrugal"))
    world.actions.apply_deliveries(ANN, retry.token, reason="gofrugal is back")
    world.engine.run_until_idle()
    world.clock.advance(timedelta(minutes=5))
    rows = [
        {"id": 181, "date": "2026-10-18", "note": "paid"},
        {"id": 182, "date": "2026-10-18", "note": None},
    ]
    _source(world).load(rows)
    run_second = world.run([day(18)])
    second = world.store.versions.latest_sealed("orders", day(18))
    assert second is not None and second.version_id != first.version_id
    return Story(first.version_id, second.version_id, run_first, run_second)


def test_the_trace_tells_the_whole_story_in_order(client: TestClient, world: World) -> None:
    story = tell(world)
    page = client.get("/trace?dataset=orders&key=181")
    assert page.status_code == 200
    html = page.text
    at = html.index
    assert at(story.first) < at(story.second)  # oldest first
    assert at("insert") < at("update")  # the first sighting inserts, the next updates
    assert "note" in html and "paid" in html  # the diff of the changed field
    # The first version, route by route: the warehouse's rejection with the body it sent, gofrugal's failure and retry.
    assert "customer is missing" in html and "422" in html and "&#34;customer&#34;: null" in html
    assert "gofrugal is down" in html and "gofrugal is back" in html and "ann" in html
    assert at("gofrugal is down") < at("gofrugal is back") < at(story.second)
    # Links to the run, the version, each delivery and the task log of the read.
    for link in (
        f'href="/runs/{story.run_first}"',
        f'href="/versions/{story.first}"',
        f'href="/versions/{story.second}"',
        f'href="/routes/warehouse/deliveries/{story.first}"',
        f'href="/routes/gofrugal/deliveries/{story.first}"',
        f'href="/routes/gofrugal/deliveries/{story.second}"',
    ):
        assert link in html, link
    assert 'href="/tasks/' in html and "/logs?run=" in html
    assert "sealed before" in html  # the note on versions older than the index


def test_the_trace_api_answers_the_same_in_json(world: World) -> None:
    story = tell(world)
    token = issue_token(world.store, "ci", timedelta(days=1), clock=world.clock)
    api = TestClient(
        create_app(state=world.state, loops=lambda s: []), headers={"Authorization": f"Bearer {token}"}
    )
    body = api.get(f"{API}/datasets/orders/records/181/trace").json()
    assert body["dataset"] == "orders" and body["key"] == "181"
    first, second = body["versions"]
    assert [first["version_id"], second["version_id"]] == [story.first, story.second]
    assert (first["op"], second["op"]) == ("insert", "update")
    assert first["partition"] == day(18) and first["run_id"] == story.run_first and first["seal_seq"] >= 1
    assert first["record"]["id"] == 181 and first["changes"] == []
    assert {"field": "note", "before": None, "after": "paid"} in second["changes"]
    routes = {d["route"]: d for d in first["deliveries"]}
    assert set(routes) == {"warehouse", "gofrugal"}
    assert routes["warehouse"]["response"] == {
        "status": 422,
        "fields": {},
        "sent": {"id": 181, "customer": None},
    }
    assert [q["error"] for q in routes["warehouse"]["quarantine"]] == ["customer is missing"]
    gofrugal = routes["gofrugal"]
    assert gofrugal["status"] == "applied"
    assert any(a["error"] == "gofrugal is down" for a in gofrugal["attempts"])
    [action] = gofrugal["actions"]
    assert (action["action"], action["actor"], action["reason"]) == ("retry", "ann", "gofrugal is back")
    assert action["audit_id"]  # the audit row it came with
    assert body["indexed_only"] is True


def test_a_key_no_version_held(client: TestClient, world: World) -> None:
    world.run([day(18)])
    page = client.get("/trace?dataset=orders&key=no-such-key")
    assert page.status_code == 200 and "No sealed version holds" in page.text
    token = issue_token(world.store, "ci", timedelta(days=1), clock=world.clock)
    api = TestClient(
        create_app(state=world.state, loops=lambda s: []), headers={"Authorization": f"Bearer {token}"}
    )
    assert api.get(f"{API}/datasets/orders/records/no-such-key/trace").json()["versions"] == []


def test_an_unknown_dataset_is_not_found(client: TestClient, world: World) -> None:
    assert client.get("/trace?dataset=nope&key=1").status_code == 404
    token = issue_token(world.store, "ci", timedelta(days=1), clock=world.clock)
    api = TestClient(
        create_app(state=world.state, loops=lambda s: []), headers={"Authorization": f"Bearer {token}"}
    )
    assert api.get(f"{API}/datasets/nope/records/1/trace").status_code == 404


def test_the_search_form_without_a_key(client: TestClient, world: World) -> None:
    html = client.get("/trace").text
    assert '<select name="dataset"' in html and '<option value="orders"' in html and 'name="key"' in html
    assert 'href="/trace"' in client.get("/").text  # the sidebar leads to it


@pytest.mark.parametrize("path", ["/trace", "/trace?dataset=orders&key=181"])
def test_the_trace_needs_a_sign_in(world: World, path: str) -> None:
    browser = TestClient(create_app(state=world.state, loops=lambda s: []), follow_redirects=False)
    answer = browser.get(path)
    assert answer.status_code in (302, 303) and answer.headers["location"].startswith("/login")
    assert browser.get(f"{API}/datasets/orders/records/181/trace").status_code == 401


def test_pages_that_show_a_record_link_to_its_trace(client: TestClient, world: World) -> None:
    story = tell(world)
    link = 'href="/trace?dataset=orders&amp;key=181"'
    assert link in client.get(f"/versions/{story.first}").text  # the version's records
    assert link in client.get(f"/routes/warehouse/deliveries/{story.first}").text  # the destination's answers
    world.store.quarantine.add("warehouse", story.second, "181", "customer is missing again")
    assert link in client.get("/quarantine").text

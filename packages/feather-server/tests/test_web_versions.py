"""The version page: what a version holds, its records a page at a time, and the links that lead to it."""

import re

from fastapi.testclient import TestClient

from feather_connect.storage.port import Version
from feather_server.app import create_app
from feather_server.testing import World, day


def _sealed(world: World, partition: str) -> Version:
    world.run([partition])
    version = world.store.versions.latest_sealed("orders", partition)
    assert version is not None
    return version


def test_the_version_page_shows_its_facts_and_records(client: TestClient, world: World) -> None:
    version = _sealed(world, day(18))
    page = client.get(f"/versions/{version.version_id}")
    assert page.status_code == 200
    html = page.text
    assert "orders" in html and "18 Oct 2026" in html
    assert f'href="/runs/{version.run_id}"' in html  # the run that sealed it
    assert re.search(r"<dt>Upserts</dt><dd>2</dd>", html) and re.search(r"<dt>Deletes</dt><dd>0</dd>", html)
    assert '<th scope="col">id</th>' in html and '<th scope="col">date</th>' in html  # a column per field
    assert ">181<" in html and ">182<" in html
    assert "<details" in html and "Show JSON" in html  # each record's JSON, without JavaScript too
    assert 'id="record-filter"' in html  # the filter box
    assert "not masked" in html  # records are shown as read in M1


def test_the_records_are_paged_by_cursor(client: TestClient, world: World) -> None:
    version = _sealed(world, day(18))
    first = client.get(f"/versions/{version.version_id}?limit=1").text
    found = re.search(r'rel="next" href="([^"]+)"', first)
    assert found and ">181<" in first and ">182<" not in first
    second = client.get(found.group(1).replace("&amp;", "&")).text
    assert ">182<" in second and ">181<" not in second and 'rel="next"' not in second


def test_a_cursor_this_server_did_not_hand_out_is_refused(client: TestClient, world: World) -> None:
    version = _sealed(world, day(18))
    assert client.get(f"/versions/{version.version_id}?cursor=nonsense").status_code == 422


def test_a_missing_version_is_not_found(client: TestClient) -> None:
    page = client.get("/versions/no-such-version")
    assert page.status_code == 404 and "no-such-version" in page.text


def test_the_version_page_needs_a_sign_in(world: World) -> None:
    version = _sealed(world, day(18))
    browser = TestClient(create_app(state=world.state, loops=lambda s: []), follow_redirects=False)
    answer = browser.get(f"/versions/{version.version_id}")
    assert answer.status_code in (302, 303) and answer.headers["location"].startswith("/login")


def test_the_delivery_page_links_to_what_was_read(client: TestClient, world: World) -> None:
    version = _sealed(world, day(18))
    html = client.get(f"/routes/warehouse/deliveries/{version.version_id}").text
    link = f'href="/versions/{version.version_id}"'
    assert html.count(link) >= 2  # the Version line, and "What was read"
    assert "What was read" in html
    assert client.get(f"/versions/{version.version_id}").status_code == 200


def test_the_flow_page_and_the_partitions_grid_link_to_versions(client: TestClient, world: World) -> None:
    version = _sealed(world, day(18))
    link = f'href="/versions/{version.version_id}"'
    assert link in client.get("/flows/orders").text
    assert link in client.get("/datasets/orders/partitions").text

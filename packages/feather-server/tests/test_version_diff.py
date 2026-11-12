"""The diff of two versions of a partition (M2 plan 2 O5): the keys added, removed and changed, with the changed fields,
PII masked, on the dashboard, in the API and in the CLI."""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from feather_connect.connectors import memory
from feather_server.app import create_app
from feather_server.auth import issue_token
from feather_server.services.diff import diff_versions
from feather_server.services.pii import MASK
from feather_server.testing import World, day, sign_in

API = "/api/v1"


def _versions(world: World) -> tuple[str, str, str]:
    """Three versions of 18 October: 181 and 182 read; then 181 changed, 182 gone and 183 new; then nothing new."""
    world.run([day(18)])
    first = world.store.versions.latest_sealed("orders", day(18))
    source = world.state.runtime.dataset("orders").source
    assert isinstance(source, memory.Source) and first is not None
    world.clock.advance(timedelta(minutes=5))
    source.load([{"id": 181, "date": "2026-10-18", "note": "paid"}, {"id": 183, "date": "2026-10-18"}])
    world.run([day(18)])
    second = world.store.versions.latest_sealed("orders", day(18))
    world.clock.advance(timedelta(minutes=5))
    source.load([{"id": 181, "date": "2026-10-18", "note": "shipped"}, {"id": 183, "date": "2026-10-18"}])
    world.run([day(18)])
    third = world.store.versions.latest_sealed("orders", day(18))
    assert second is not None and third is not None
    return first.version_id, second.version_id, third.version_id


def test_a_diff_names_the_keys_added_removed_and_changed(world: World) -> None:
    first, second, third = _versions(world)
    found = diff_versions(world.state, second)
    assert found.before is not None and found.before.version_id == first
    assert (found.added, found.removed) == (("183",), ("182",))
    [change] = found.changed
    assert change.key == "181" and [(f.field, f.before, f.after) for f in change.fields] == [
        ("note", None, "paid")
    ]
    across = diff_versions(world.state, third, first)  # the changes of both versions netted
    assert (across.added, across.removed) == (("183",), ("182",))
    assert [(f.field, f.after) for f in across.changed[0].fields] == [("note", "shipped")]
    assert diff_versions(world.state, first).added == ("181", "182")  # the first version adds all it holds
    with pytest.raises(ValueError, match="sealed before it"):
        diff_versions(world.state, first, third)


def test_the_api_and_the_page_show_a_diff_with_pii_masked(world: World) -> None:
    object.__setattr__(world.state.runtime.dataset("orders"), "pii", ("note",))
    _, second, _ = _versions(world)
    token = issue_token(world.state.store, "op", timedelta(days=1), role="operator", clock=world.clock)
    app = create_app(state=world.state, loops=lambda s: [])
    api = TestClient(app, headers={"Authorization": f"Bearer {token}"})
    found = api.get(f"{API}/versions/{second}/diff").json()
    assert found["masked"] and found["counts"] == {"added": 1, "removed": 1, "changed": 1}
    assert found["changed"][0]["fields"] == [{"field": "note", "before": None, "after": MASK}]
    assert api.get(f"{API}/versions/{second}/diff", params={"against": second}).status_code == 422
    assert api.get(f"{API}/versions/nope/diff").status_code == 404
    browser = sign_in(app)
    page = browser.get(f"/versions/{second}/diff")
    assert page.status_code == 200 and "183" in page.text and "182" in page.text and MASK in page.text
    assert f'href="/versions/{second}/diff"' in browser.get(f"/versions/{second}").text

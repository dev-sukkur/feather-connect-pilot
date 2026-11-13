import html as htmllib
import re
import time
from collections.abc import Callable
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.rows import DictRow

from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.engine.local import LocalEngine
from feather_connect.logs import register_secrets
from feather_connect.runtime.runtime import Runtime
from feather_server.app import AppState, create_app
from feather_server.settings import Settings
from feather_server.testing import World, day, sign_in

Csrf = Callable[[], str]


def fail_warehouse(world: World, message: str, partition: str) -> None:
    world.sink("warehouse").fail_next(PermanentError(message))
    world.run([partition])


def hidden_fields(page: str, action: str, button: str | None = None) -> dict[str, str]:
    """What a browser would post from the form that goes to `action`: its hidden fields, and the chosen button."""
    for form in re.findall(r"<form[^>]*>.*?</form>", page, re.DOTALL):
        if f'action="{action}"' not in form or (button and f'value="{button}"' not in form):
            continue
        fields = {
            htmllib.unescape(name): htmllib.unescape(value)
            for name, value in re.findall(r'<input type="hidden" name="([^"]*)" value="([^"]*)"', form)
        }
        return {**fields, **({"action": button} if button else {})}
    raise AssertionError(f"no form posts to {action}")


def token_of(page: str) -> str:
    found = re.search(r'name="token" value="([^"]+)"', page)
    assert found, "no confirmation token on the page"
    return found.group(1)


def test_failures_are_grouped_by_error_class_and_message_per_route(client: TestClient, world: World) -> None:
    for partition in (day(15), day(16)):
        fail_warehouse(world, "no such table", partition)
    fail_warehouse(world, "quota exceeded", day(17))
    page = client.get("/failures").text
    body = page.split("<tbody>")[1]
    rows = re.findall(r"<tr>(.*?)</tr>", body, re.DOTALL)
    text = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", r)).strip() for r in rows]
    table = next(t for t in text if "no such table" in t)
    assert "warehouse" in table and "PermanentError" in table and " 2 " in f" {table} "
    assert any("quota exceeded" in t and " 1 " in f" {t} " for t in text)
    assert len(rows) == 2 and "gofrugal" not in body  # one row per route and error
    only = client.get("/failures?route=gofrugal").text
    assert "No failed deliveries" in only
    assert 'href="/routes/warehouse/deliveries?status=failed' in page  # to the deliveries behind a group


def test_group_shows_a_blocked_partition_and_what_it_blocks(client: TestClient, world: World) -> None:
    fail_warehouse(world, "no such table", day(17))
    world.run([day(17)])  # a later version of the same partition queues behind the failed one
    page = client.get("/failures").text
    assert "Blocks" in page and "17 Oct 2026" in page
    assert re.search(r"1 later (delivery|version)", page)
    assert page.index("17 Oct 2026") < page.index("1 later")


def test_failures_page_is_paged_by_cursor(client: TestClient, world: World) -> None:
    for n, partition in enumerate((day(14), day(15), day(16))):
        fail_warehouse(world, f"error {n}", partition)
    first = client.get("/failures?limit=2").text
    assert "Next page" in first
    link = re.search(r'rel="next" href="([^"]+)"', first)
    assert link
    second = client.get(htmllib.unescape(link.group(1))).text
    seen = [m for page in (first, second) for m in re.findall(r"error-text\">(error \d)", page)]
    assert (
        sorted(set(seen)) == ["error 0", "error 1", "error 2"] and len(seen) == 3
    )  # no repeats, none missed


def test_bulk_retry_previews_then_confirms(client: TestClient, world: World, csrf: Csrf) -> None:
    fail_warehouse(world, "no such table", day(17))
    fail_warehouse(world, "no such table", day(16))
    page = client.get("/failures").text
    form = {"csrf_token": csrf(), **hidden_fields(page, "/deliveries/preview", "retry")}
    preview = client.post("/deliveries/preview", data=form)
    assert preview.status_code == 200 and "Check before you confirm" in preview.text
    assert re.search(r"<dt>Deliveries</dt><dd>2</dd>", preview.text)
    assert {d.status for d in world.store.ledger.list_deliveries(route="warehouse")} == {"failed"}
    token = token_of(preview.text)
    done = client.post(
        "/deliveries/confirm",
        data={"csrf_token": csrf(), "token": token, "reason": "table is back", "from": "failures"},
    )
    assert done.status_code == 303 and done.headers["location"].startswith("/failures")
    world.engine.run_until_idle()
    assert {d.status for d in world.store.ledger.list_deliveries(route="warehouse")} == {"applied"}
    (row,) = [a for a in world.store.audit.list_audit(20) if a.action == "retry_deliveries"]
    assert (row.actor, row.request["reason"]) == ("op", "table is back")
    assert "2 deliveries" in client.get(done.headers["location"]).text
    again = client.post(
        "/deliveries/confirm", data={"csrf_token": csrf(), "token": token, "from": "failures"}
    )
    assert (
        again.status_code == 409 and "preview again" in again.text
    )  # a token works once; the state has moved on
    assert client.post("/deliveries/preview", data=form | {"csrf_token": "x"}).status_code == 403


def test_a_preview_of_nothing_is_refused(client: TestClient, world: World, csrf: Csrf) -> None:
    form = {"csrf_token": csrf(), "action": "retry", "route": "warehouse", "from": "failures"}
    refused = client.post("/deliveries/preview", data=form)
    assert refused.status_code == 422 and "matches nothing" in refused.text
    bad = client.post("/deliveries/preview", data=form | {"action": "explode"})
    assert bad.status_code == 422
    forged = client.post(
        "/deliveries/confirm", data={"csrf_token": csrf(), "token": "x.y", "from": "failures"}
    )
    assert forged.status_code == 422


def test_a_change_needs_a_reason_field_filled_when_protected(
    client: TestClient, world: World, csrf: Csrf, monkeypatch: pytest.MonkeyPatch
) -> None:
    fail_warehouse(world, "no such table", day(17))
    fail_warehouse(world, "other", day(16))
    page = client.get("/failures").text
    free = {"csrf_token": csrf(), **hidden_fields(page, "/deliveries/preview", "skip")}
    preview = client.post("/deliveries/preview", data=free)
    assert "required" not in preview.text.split('name="reason"')[1].split(">")[0]  # not protected: optional
    ok = client.post("/deliveries/confirm", data={"csrf_token": csrf(), "token": token_of(preview.text)})
    assert ok.status_code == 303  # no reason, and it works

    monkeypatch.setitem(world.state.project.settings, "protected", True)
    page = client.get("/failures").text
    form = {"csrf_token": csrf(), **hidden_fields(page, "/deliveries/preview", "skip")}
    preview = client.post("/deliveries/preview", data=form)
    assert "required" in preview.text.split('name="reason"')[1].split(">")[0]  # the field insists
    token = token_of(preview.text)
    blank = client.post("/deliveries/confirm", data={"csrf_token": csrf(), "token": token, "reason": "  "})
    assert blank.status_code == 422 and "reason" in blank.text.lower()
    assert {d.status for d in world.store.ledger.list_deliveries(route="warehouse")} >= {"failed"}
    # The refusal did not spend the token: with a reason it goes through.
    again = client.post(
        "/deliveries/confirm", data={"csrf_token": csrf(), "token": token, "reason": "bad data"}
    )
    # Protected, a skip is asked for, and waits for a second person (M3 plan 1 E3).
    assert again.status_code == 202 and "waits for a second person" in again.text
    assert {d.status for d in world.store.ledger.list_deliveries(route="warehouse")} >= {"failed"}
    [asked] = world.store.requests.list_requests("waiting")
    assert asked.action == "skip_deliveries" and asked.reason == "bad data"
    # Retrying needs one too, when protected: every change does (M3 plan 1 E3).
    fail_warehouse(world, "again", day(15))
    page = client.get("/failures").text
    retry = client.post(
        "/deliveries/preview",
        data={"csrf_token": csrf(), **hidden_fields(page, "/deliveries/preview", "retry")},
    )
    assert "required" in retry.text.split('name="reason"')[1].split(">")[0]


def test_replay_from_deliveries_page(client: TestClient, world: World, csrf: Csrf) -> None:
    world.run([day(17), day(18)])
    page = client.get("/routes/warehouse/deliveries?partition=date%3D2026-10-17").text
    assert "17 Oct 2026" in page and "18 Oct 2026" not in page  # the partition filter is honoured
    assert "Applied" in page and '<form method="post" action="/deliveries/preview"' in page
    fields = hidden_fields(page, "/deliveries/preview")
    assert fields["route"] == "warehouse" and fields["from"] == "deliveries"
    version = world.store.versions.latest_sealed("orders", day(17))
    assert version
    data = {"csrf_token": csrf(), **fields, "action": "replay", "version_id": version.version_id}
    preview = client.post("/deliveries/preview", data=data)
    assert preview.status_code == 200 and "Check before you confirm" in preview.text
    assert (
        re.search(r"<dt>Deliveries</dt><dd>1</dd>", preview.text)
        and "warehouse · 17 Oct 2026" in preview.text
    )
    nothing = client.post("/deliveries/preview", data={k: v for k, v in data.items() if k != "version_id"})
    assert (
        nothing.status_code == 422 and "Tick at least one" in nothing.text
    )  # never a whole route by accident
    done = client.post(
        "/deliveries/confirm",
        data={
            "csrf_token": csrf(),
            "token": token_of(preview.text),
            "from": "deliveries",
            "route": "warehouse",
        },
    )
    assert done.status_code == 303 and done.headers["location"].startswith("/routes/warehouse/deliveries")
    world.engine.run_until_idle()
    assert world.store.ledger.get("warehouse", version.version_id).replays == 1
    assert world.store.ledger.get("warehouse", version.version_id).status == "applied"
    assert client.get("/routes/nothing/deliveries").status_code == 404
    assert client.get("/routes/warehouse/deliveries?status=odd").status_code == 422


def test_deliveries_filter_by_status_and_page_by_cursor(client: TestClient, world: World) -> None:
    fail_warehouse(world, "no such table", day(17))
    world.run([day(16)])
    failed = client.get("/routes/warehouse/deliveries?status=failed").text
    assert "no such table" in failed and "16 Oct 2026" not in failed
    both = client.get("/routes/warehouse/deliveries?limit=1").text
    link = re.search(r'rel="next" href="([^"]+)"', both)
    assert link
    assert "16 Oct 2026" in client.get(htmllib.unescape(link.group(1))).text or "17 Oct 2026" in both


def test_quarantine_shows_reason_and_can_be_dismissed(client: TestClient, world: World, csrf: Csrf) -> None:
    world.run([day(17)])
    version = world.store.versions.latest_sealed("orders", day(17))
    assert version
    world.store.quarantine.add("warehouse", version.version_id, "171", "amount <b>must</b> be positive")
    assert "No records are in quarantine" in client.get("/quarantine?route=gofrugal").text
    page = client.get("/quarantine").text
    assert "amount &lt;b&gt;must&lt;/b&gt; be positive" in page and "<b>must</b>" not in page
    assert "171" in page and "warehouse" in page and "17 Oct 2026" in page
    fields = {"csrf_token": csrf(), **hidden_fields(page, "/quarantine/dismiss"), "reason": "fixed upstream"}
    assert client.post("/quarantine/dismiss", data=fields | {"csrf_token": "x"}).status_code == 403
    done = client.post("/quarantine/dismiss", data=fields)
    assert done.status_code == 303 and done.headers["location"].startswith("/quarantine")
    assert world.store.quarantine.list_open() == []
    (row,) = [a for a in world.store.audit.list_audit(10) if a.action == "dismiss_quarantine"]
    assert (row.actor, row.request["reason"]) == ("op", "fixed upstream")
    assert "dismissed" in client.get(done.headers["location"]).text.lower()
    again = client.post("/quarantine/dismiss", data=fields)
    assert again.status_code == 409


def test_error_messages_are_escaped(client: TestClient, world: World, csrf: Csrf) -> None:  # Review Focus 5
    nasty = '<script>alert("x")</script> & <img src=x onerror=alert(1)>'
    fail_warehouse(world, nasty, day(17))
    for url in (
        "/failures",
        "/routes/warehouse/deliveries",
        f"/runs/{world.store.runs.list_runs()[0].run_id}",
    ):
        page = client.get(url).text
        assert "<script>alert" not in page and "<img src=x" not in page, url
        assert "&lt;script&gt;alert(&#34;x&#34;)&lt;/script&gt; &amp; &lt;img" in page, url
    page = client.get("/failures").text
    preview = client.post(
        "/deliveries/preview",
        data={"csrf_token": csrf(), **hidden_fields(page, "/deliveries/preview", "retry")},
    )
    assert (
        preview.status_code == 200 and "<script>alert" not in preview.text
    )  # the value survives the round trip
    assert "<script>alert" not in client.get("/").text


def test_recover_a_failed_delivery_in_five_clicks_or_fewer(
    client: TestClient, world: World, csrf: Csrf
) -> None:  # NFR-11
    fail_warehouse(world, "no such table", day(17))
    requests = 0

    overview = client.get("/").text  # where the operator starts: not a click
    assert 'href="/failures"' in overview
    requests += 1  # click 1: the link to the failures
    failures = client.get("/failures").text
    form = {"csrf_token": csrf(), **hidden_fields(failures, "/deliveries/preview", "retry")}
    requests += 1  # click 2: Retry on the group
    preview = client.post("/deliveries/preview", data=form)
    requests += 1  # click 3: Confirm
    done = client.post("/deliveries/confirm", data={"csrf_token": csrf(), "token": token_of(preview.text)})
    assert done.status_code == 303
    world.engine.run_until_idle()
    assert {d.status for d in world.store.ledger.list_deliveries(route="warehouse")} == {"applied"}
    assert requests <= 5, f"{requests} clicks"


# Review Focus 4: tens of thousands of failures, on Postgres.

BIG = 50_000
KINDS = 500


@pytest.fixture
def seed_failures(world: World, pg_url: str, pg: psycopg.Connection[DictRow], pg_connect: Any) -> TestClient:
    """A signed-in browser on a Postgres store with `BIG` failed deliveries on `warehouse`, of `KINDS` different errors."""
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
        """insert into feather.delivery (tenant_id, route, version_id, dataset, partition, seal_seq, status, attempts,
               last_error)
           select 'default', 'warehouse', v.version_id, v.dataset, v.partition, v.seal_seq, 'failed', 1,
                  'error kind ' || lpad((v.seal_seq %% %s)::text, 4, '0')
           from feather.version v""",
        (KINDS,),
    )
    return sign_in(create_app(state=state, loops=lambda s: []))


@pytest.mark.postgres
def test_50_000_failures_are_paginated_and_fast(seed_failures: TestClient) -> None:
    for url in ("/failures", "/routes/warehouse/deliveries?status=failed", "/"):
        started = time.perf_counter()
        response = seed_failures.get(url)
        seconds = time.perf_counter() - started
        assert response.status_code == 200, url
        assert seconds < 2, f"{url} took {seconds:.2f} s"
    first = seed_failures.get("/failures").text
    assert first.count("error-text") <= 100  # a page, not all 500 groups
    link = re.search(r'rel="next" href="([^"]+)"', first)
    assert link
    second = seed_failures.get(htmllib.unescape(link.group(1))).text
    kinds = r"error-text\">(error kind \d{4})"
    assert set(re.findall(kinds, first)).isdisjoint(re.findall(kinds, second))
    deliveries = seed_failures.get("/routes/warehouse/deliveries?status=failed").text
    assert len(re.findall(r'name="version_id"', deliveries)) == 100
    assert 'rel="next"' in deliveries


def test_a_connector_error_never_shows_a_secret_on_the_pages(client: TestClient, world: World) -> None:  # F8
    secret = "f8-registered-api-key"
    register_secrets(secret)
    message = f"GET https://api/x?key={secret}: 500"
    world.sink("warehouse").fail_next(PermanentError(message))
    world.sink("gofrugal").fail_next(TransientError(message))
    run = world.run([day(17)])
    pages = [client.get(url).text for url in ("/failures", f"/runs/{run}", "/routes/warehouse/deliveries")]
    assert "key=***" in pages[0]
    assert not any(secret in page for page in pages)
    rows = world.store.ledger.list_deliveries()
    assert not any(secret in (d.last_error or "") for d in rows)
    assert any("key=***" in (d.last_error or "") for d in rows)
    attempts = [a for d in rows for a in world.store.ledger.attempts(d.route, d.version_id)]
    assert not any(secret in (a.error or "") for a in attempts)
    assert not any(secret in (t.error or "") for t in world.store.runs.list_tasks(run))
    assert secret not in (world.store.runs.get_run(run).reason or "")


@pytest.mark.parametrize(
    "path",
    [
        "/failures?cursor=not-a-cursor",
        "/routes/warehouse/deliveries?cursor=not-a-cursor",
        "/routes/warehouse/deliveries?cursor=x:y",
        "/api/v1/routes/warehouse/deliveries?cursor=bm90LWEtY3Vyc29y",
        "/runs?cursor=not-a-cursor",
        "/datasets/orders/partitions?cursor=not-a-cursor",
        "/quarantine?cursor=not-a-cursor",
    ],
)
def test_a_bad_cursor_is_not_a_500(client, path):  # M3: a refusal, or a page that ignores it
    assert client.get(path).status_code < 500


def test_a_non_ascii_confirmation_token_is_refused(client, csrf):  # M3
    answer = client.post(
        "/deliveries/confirm", data={"csrf_token": csrf(), "token": "abc.é", "action": "retry"}
    )
    assert answer.status_code != 500


def test_quarantine_pages_through_every_entry(client: TestClient, world: World, monkeypatch) -> None:  # M7
    from feather_server.web import routes

    monkeypatch.setattr(routes, "_QUARANTINE_SHOWN", 2)
    world.run([day(17)])
    version = world.store.versions.latest_sealed("orders", day(17))
    assert version
    for key in ("k1", "k2", "k3"):
        world.store.quarantine.add("warehouse", version.version_id, key, "bad")
    first = client.get("/quarantine").text
    assert "k1" in first and "k3" not in first and "1 to 2 of 3" in first
    link = re.search(r'href="(/quarantine\?[^"]*offset=2[^"]*)"', first)
    assert link, first
    second = client.get(htmllib.unescape(link.group(1))).text
    assert "k3" in second and "k1" not in second

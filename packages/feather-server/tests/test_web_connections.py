import time
from collections.abc import Callable

import pytest
from fastapi.testclient import TestClient

from feather_connect.core.connector import CheckResult
from feather_connect.core.context import Context
from feather_connect.logs import register_secrets
from feather_server.services import actions as actions_module
from feather_server.testing import SECRET, World

Csrf = Callable[[], str]


def set_shop_check(world: World, check: Callable[[Context], CheckResult]) -> None:
    """Give the shop's source this check, as a connector with a real one would have."""
    source = world.state.runtime.flow("orders").datasets[0].source
    source.__dict__["check"] = check


def test_lists_status_last_check_and_site(client: TestClient, world: World, csrf: Csrf) -> None:
    page = client.get("/connections").text
    for name in ("shop", "warehouse", "gofrugal"):
        assert f'<th scope="row">{name}</th>' in page
    assert "hq" in page and "shop.api" in page  # the site and the connector
    assert "Never checked" in page and "orders" in page  # nothing has checked yet, and the flow that uses it
    assert 'href="/flows/orders"' in page
    assert '<td data-col="budget">api</td>' in page  # the budget the warehouse spends from
    assert 'href="/connections"' in page.split("<main")[0]  # in the navigation
    client.post("/connections/warehouse/check", data={"csrf_token": csrf()})
    after = client.get("/connections").text
    assert after.count("Never checked") == 2 and "just now" in after  # only the warehouse was checked


def test_check_runs_and_shows_the_result(client: TestClient, world: World, csrf: Csrf) -> None:
    seen: list[Context] = []

    def check(ctx: Context) -> CheckResult:
        seen.append(ctx)
        return CheckResult(True, "reached hq")

    set_shop_check(world, check)
    assert client.post("/connections/shop/check", data={}).status_code == 403
    done = client.post("/connections/shop/check", data={"csrf_token": csrf()})
    assert done.status_code == 303 and done.headers["location"].startswith("/connections")
    assert len(seen) == 1
    page = client.get(done.headers["location"]).text
    assert "reached hq" in page and "OK" in page
    stored = world.store.overrides.get("connection:shop", "last_check")
    assert stored is not None
    assert stored["ok"] is True and stored["message"] == "reached hq" and stored["at"]
    (row,) = [a for a in world.store.audit.list_audit(10) if a.action == "check_connection"]
    assert (row.actor, row.target) == ("op", "connection:shop")
    assert client.post("/connections/nothing/check", data={"csrf_token": csrf()}).status_code == 404
    simulated = client.post("/connections/warehouse/check", data={"csrf_token": csrf()})
    assert "nothing was checked" in client.get(simulated.headers["location"]).text


def test_failed_check_shows_a_redacted_message(client: TestClient, world: World, csrf: Csrf) -> None:
    register_secrets(SECRET)

    def check(ctx: Context) -> CheckResult:
        raise ConnectionError(f"401 Unauthorized for token {SECRET} <b>x</b>")

    set_shop_check(world, check)
    done = client.post("/connections/shop/check", data={"csrf_token": csrf()})
    page = client.get(done.headers["location"]).text
    assert "Failed" in page and "401 Unauthorized" in page and "***" in page
    assert SECRET not in page and "<b>x</b>" not in page and "&lt;b&gt;x&lt;/b&gt;" in page
    assert SECRET not in str(world.store.overrides.get("connection:shop", "last_check"))
    assert all(SECRET not in str(a.result) for a in world.store.audit.list_audit(10))


def test_check_times_out_with_a_clear_message(
    client: TestClient, world: World, csrf: Csrf, monkeypatch: pytest.MonkeyPatch
) -> None:
    def slow(ctx: Context) -> CheckResult:
        time.sleep(1)
        return CheckResult(True, "too late")

    set_shop_check(world, slow)
    monkeypatch.setattr(actions_module, "CHECK_TIMEOUT_S", 0.05)
    started = time.perf_counter()
    done = client.post("/connections/shop/check", data={"csrf_token": csrf()})
    assert time.perf_counter() - started < 0.9  # it did not wait for the connector
    page = client.get(done.headers["location"]).text
    assert "did not answer within" in page and "too late" not in page
    assert actions_module.CHECK_TIMEOUT_S == 0.05
    monkeypatch.undo()
    assert actions_module.CHECK_TIMEOUT_S == 15


def test_no_secret_on_the_page(client: TestClient, world: World, csrf: Csrf) -> None:
    client.post("/connections/warehouse/check", data={"csrf_token": csrf()})
    page = client.get("/connections").text
    assert SECRET not in page and "s3cret" not in page
    assert "auth.token" in page or "token" in page  # the name of the secret setting may show, never its value

import hashlib
import re
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from feather_connect.core.errors import PermanentError
from feather_server import web
from feather_server.app import AppState, create_app
from feather_server.settings import Settings
from feather_server.testing import World
from feather_server.web.charts import meter, sparkline

HTMX_SHA256 = "e209dda5c8235479f3166defc7750e1dbcd5a5c1808b7792fc2e6733768fb447"  # htmx.org 2.0.4
STATIC = Path(web.__file__).parent / "static"


def fail(world: World, message: str = "no such table") -> None:
    world.sink("warehouse").fail_next(PermanentError(message))
    world.run()


def test_overview_needs_a_signed_in_user(world: World) -> None:
    anonymous = TestClient(create_app(state=world.state, loops=lambda s: []), follow_redirects=False)
    response = anonymous.get("/")
    assert response.status_code == 303 and response.headers["location"] == "/login"


def test_overview_shows_failing_flows_and_links_to_them(client: TestClient, world: World) -> None:
    fail(world)
    html = client.get("/").text
    assert 'href="/flows/orders"' in html
    assert "no such table" in html and "warehouse" in html  # the failure, with its route
    assert 'Failed deliveries</h3><p class="n">1<' in html


def test_overview_shows_stale_datasets_budgets_workers_and_engine_health(
    client: TestClient, world: World
) -> None:
    world.store.workers.heartbeat("worker-7", {"site": "hq"}, "sha", 8)
    html = client.get("/").text
    assert "orders" in html and "never read" in html.lower()  # no version sealed yet
    assert "api" in html and 'class="meter"' in html and "0 of 90" in html
    assert "worker-7" in html and "alive" in html
    assert "local" in html  # the engine
    world.run()
    world.clock.advance(timedelta(minutes=31))
    assert "ago" in client.get("/").text  # the dataset is late, and says since when


def test_every_page_sends_the_csp_header_and_has_no_inline_script(client: TestClient) -> None:
    response = client.get("/")
    policy = response.headers["content-security-policy"]
    assert "'unsafe-inline'" not in policy and "default-src 'self'" in policy
    assert "frame-ancestors 'none'" in policy and "script-src 'self'" in policy
    html = response.text
    assert "<script>" not in html and " onclick=" not in html and "style=" not in html
    assert re.findall(r"<script[^>]*>", html) == ['<script src="/static/htmx.min.js" defer>']
    assert "default-src 'self'" in client.get("/login").headers["content-security-policy"]


def test_user_text_is_escaped(client: TestClient, world: World) -> None:  # Review Focus 5
    fail(world, "<script>alert(1)</script>")
    html = client.get("/").text
    assert "<script>alert" not in html and "&lt;script&gt;alert" in html


def test_page_works_without_htmx(client: TestClient, world: World) -> None:
    fail(world)
    html = client.get("/").text  # a plain page load, as a browser with no JavaScript makes it
    assert html.lstrip().lower().startswith("<!doctype html>") and "<nav" in html
    assert "no such table" in html and '<a href="/flows/orders">' in html


def test_the_overview_refreshes_itself_and_an_htmx_request_gets_a_fragment(
    client: TestClient, world: World
) -> None:
    full = client.get("/").text
    assert 'hx-get="/"' in full and 'hx-trigger="every 10s"' in full
    fragment = client.get("/", headers={"HX-Request": "true"})
    assert "<html" not in fragment.text and "<nav" not in fragment.text
    assert 'hx-get="/"' in fragment.text  # it carries the refresh on, so the page keeps updating
    assert "Failed deliveries" in fragment.text


def test_the_overview_lists_recent_runs_as_they_arrive(client: TestClient, world: World) -> None:
    assert "No runs yet" in client.get("/").text
    run_id = world.run()
    assert run_id[:8] in client.get("/", headers={"HX-Request": "true"}).text


def test_status_is_never_colour_alone(client: TestClient, world: World) -> None:
    fail(world)
    html = client.get("/").text
    start = html.index("badge badge-failed")
    badge = html[start : html.index("</span>", html.index("</span>", start) + 1)]
    assert (
        'class="icon" aria-hidden="true"' in badge and "Failed" in badge
    )  # an icon and a word, not only a colour


def test_pages_have_a_skip_link_a_main_landmark_and_a_current_nav_item(client: TestClient) -> None:
    html = client.get("/").text
    assert 'class="skip" href="#main"' in html and '<main id="main"' in html
    assert re.search(r'<a href="/" aria-current="page">Overview</a>', html)
    assert '<html lang="en">' in html and "<title>Overview" in html


def test_static_files_are_served_and_the_vendored_htmx_is_pinned(client: TestClient) -> None:
    script = client.get("/static/htmx.min.js")
    assert script.status_code == 200 and "javascript" in script.headers["content-type"]
    assert hashlib.sha256((STATIC / "htmx.min.js").read_bytes()).hexdigest() == HTMX_SHA256
    assert "Zero-Clause BSD" in (STATIC / "LICENSE-htmx.txt").read_text(encoding="utf-8")
    css = client.get("/static/app.css")
    assert css.status_code == 200 and "text/css" in css.headers["content-type"]
    assert "prefers-color-scheme: dark" in css.text and "@import" not in css.text


def test_htmx_is_told_not_to_use_inline_styles_or_eval(client: TestClient) -> None:
    html = client.get("/").text
    assert '<meta name="htmx-config"' in html
    assert "includeIndicatorStyles" in html and "allowEval" in html


def test_the_csrf_token_is_on_every_page(client: TestClient, csrf: Callable[[], str]) -> None:
    assert len(csrf()) == 64


def test_create_app_wires_actions_and_reads(world: World) -> None:
    app = create_app(state=world.state, loops=lambda s: [])
    assert app.state.actions is not None and app.state.reads is not None


def test_create_app_without_a_secret_key_says_so(state: AppState) -> None:
    state.settings = Settings(secret_key="")
    with pytest.raises(ValueError, match="secret key"):
        create_app(state=state, loops=lambda s: [])


def test_sparkline_is_svg_built_from_numbers_only() -> None:
    svg = sparkline([1, 3, 2, 5])
    assert svg.startswith("<svg") and "<polyline" in svg and 'role="img"' in svg
    assert sparkline([]) == "" and "<polyline" in sparkline([4])  # one point is still drawn
    assert "nan" not in sparkline([1.0, float("nan"), 3.0]).lower()
    assert "<polyline" in sparkline([2, 2, 2])  # a flat line has no range to divide by


def test_meter_shows_use_against_the_limit_and_clamps() -> None:
    assert 'class="meter"' in meter(45, 90) and "45 of 90" in meter(45, 90)
    assert meter(100, 90).count("meter-over") == 1  # past the limit, drawn full and marked
    assert "0 of 0" in meter(0, 0)  # no limit does not divide by zero

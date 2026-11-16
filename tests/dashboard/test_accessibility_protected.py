"""axe-core on the pages only a protected environment shows (M3 plan 1 E3), in light and dark: the page asking for a
reason, and the Holds page with an action waiting for a second person (NFR-13).

The shared dashboard stack isn't protected, so these run the server's test world in this process, protected, on a
real port: the same templates and stylesheets, which is what axe checks.
"""

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from axe_playwright_python.sync_playwright import Axe
from playwright.sync_api import Browser, Page

from feather_connect.clock import FrozenClock
from feather_server.app import create_app
from feather_server.auth import hash_password
from feather_server.testing import PASSWORD, build_world, serve

pytestmark = [pytest.mark.timeout(300)]

WCAG_AA = {"runOnly": {"type": "tag", "values": ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"]}}


@pytest.fixture(scope="module")
def protected_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    mp = pytest.MonkeyPatch()
    mp.setenv("WH_TOKEN", "a-warehouse-token-for-tests")
    world = build_world(
        tmp_path_factory.mktemp("protected"), FrozenClock(datetime(2026, 11, 16, 9, tzinfo=UTC))
    )
    world.state.project.settings["protected"] = True
    world.state.store.users.create_user("ann", hash_password(PASSWORD), "admin")
    world.state.store.requests.add(
        "skip_deliveries", "warehouse", {"version_ids": ["v1"]}, "bob", "the supplier sent a duplicate file"
    )
    with serve(create_app(state=world.state, loops=lambda s: [])) as url:
        yield url
    mp.undo()


@pytest.fixture
def page_in(browser: Browser, protected_url: str) -> Iterator[Page]:
    context = browser.new_context()
    page = context.new_page()
    page.goto(f"{protected_url}/login")
    page.get_by_label("Username").fill("ann")
    page.get_by_label("Password").fill(PASSWORD)
    page.get_by_role("button", name="Sign in").click()
    page.wait_for_url(lambda found: not found.endswith("/login"))
    yield page
    context.close()


def _no_violations(page: Page) -> None:
    page.wait_for_load_state("networkidle")
    results = Axe().run(page, options=WCAG_AA)
    assert results.violations_count == 0, results.generate_report()


@pytest.mark.parametrize("scheme", ["light", "dark"])
def test_the_page_asking_for_a_reason_has_no_wcag_aa_violations(
    page_in: Page, protected_url: str, scheme: str
):
    page_in.emulate_media(color_scheme=scheme)  # pyright: ignore[reportArgumentType]
    page_in.goto(f"{protected_url}/flows/orders")
    with page_in.expect_navigation():
        page_in.evaluate("document.querySelector('form[action$=\"/pause\"]').submit()")  # no reason given
    assert "A reason is needed" in page_in.content()
    _no_violations(page_in)


@pytest.mark.parametrize("scheme", ["light", "dark"])
def test_the_holds_page_with_a_request_waiting_has_no_wcag_aa_violations(
    page_in: Page, protected_url: str, scheme: str
):
    page_in.emulate_media(color_scheme=scheme)  # pyright: ignore[reportArgumentType]
    page_in.goto(f"{protected_url}/holds")
    assert "the supplier sent a duplicate file" in page_in.content()
    _no_violations(page_in)

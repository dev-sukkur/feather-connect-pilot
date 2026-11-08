"""The dashboard in a real browser: the stack running tests/dashboard/project, seeded with three days of items, and a
Playwright `page` signed in to it.

    uv run playwright install chromium
    uv run pytest tests/dashboard -q

The lake takes every delivery and writes it to a capture file; `flaky` turns down each version once, so each day has a
failure to retry. A test that changes what the stack holds asks for `fresh_site`; the read-only ones share `site`.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from dashboard_site import PASSWORD, USER, Site, open_site
from playwright.sync_api import Browser, Page


@pytest.fixture(scope="module")
def site(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Site]:
    """A seeded stack shared by the tests of a module that only look."""
    yield from open_site(tmp_path_factory.mktemp("site"))


@pytest.fixture
def fresh_site(tmp_path: Path) -> Iterator[Site]:
    """A seeded stack of the test's own, for a test that changes what it holds."""
    yield from open_site(tmp_path / "site")


def sign_in(page: Page, url: str) -> None:
    page.goto(f"{url}/login")
    page.get_by_label("Username").fill(USER)
    page.get_by_label("Password").fill(PASSWORD)
    page.get_by_role("button", name="Sign in").click()
    page.wait_for_url(lambda found: not found.endswith("/login"))


@pytest.fixture(scope="module")
def session(browser: Browser, site: Site) -> Any:
    """The cookies of one sign-in to `site`, shared by its tests: sign-ins from one address are rate limited."""
    context = browser.new_context()
    sign_in(context.new_page(), site.url)
    state = context.storage_state()
    context.close()
    return state


def _watch(page: Page) -> list[str]:
    """Collect the errors the page's console shows, CSP violations among them."""
    found: list[str] = []
    page.on("console", lambda message: found.append(message.text) if message.type == "error" else None)
    page.on("pageerror", lambda error: found.append(str(error)))
    return found


@pytest.fixture
def signed_in_with_console(browser: Browser, session: Any) -> Iterator[tuple[Page, list[str]]]:
    context = browser.new_context(storage_state=session)
    page = context.new_page()
    yield page, _watch(page)
    context.close()


@pytest.fixture
def signed_in(signed_in_with_console: tuple[Page, list[str]]) -> Page:
    return signed_in_with_console[0]


@pytest.fixture
def console(signed_in_with_console: tuple[Page, list[str]]) -> list[str]:
    return signed_in_with_console[1]


@pytest.fixture
def signed_in_fresh(page: Page, fresh_site: Site) -> Page:
    sign_in(page, fresh_site.url)
    return page


@pytest.fixture
def no_script_page(browser: Browser, session: Any) -> Iterator[Page]:
    context = browser.new_context(java_script_enabled=False, storage_state=session)
    yield context.new_page()
    context.close()

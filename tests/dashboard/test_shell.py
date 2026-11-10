"""The dashboard's shell in a real browser: the command palette, the theme, the column chooser, opening a row, the
keyboard, and (with FEATHER_SCREENSHOTS=1) the screenshots of the plan's docs."""

import os
import re
from pathlib import Path

import pytest
from dashboard_site import Site
from playwright.sync_api import Page, expect

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(300)]

SHOTS = Path(__file__).resolve().parents[2] / "docs" / "04-plan" / "screenshots"


def test_the_palette_finds_a_flow_a_run_by_id_and_a_page(signed_in: Page, site: Site):
    page = signed_in
    page.goto(f"{site.url}/")
    page.keyboard.press("Control+k")
    palette = page.get_by_role("dialog", name="Search")
    expect(palette).to_be_visible()
    box = page.get_by_role("combobox")
    expect(box).to_be_focused()
    box.fill("dem")
    options = palette.get_by_role("option")
    expect(options.first).to_contain_text("demo")
    page.keyboard.press("Tab")  # the focus stays in the dialog
    expect(box).to_be_focused()
    page.keyboard.press("Enter")
    expect(page).to_have_url(f"{site.url}/flows/demo")

    run_id = site.stack.store.runs.list_runs(limit=1)[0].run_id
    page.keyboard.press("Control+k")
    page.get_by_role("combobox").fill(run_id[:6])
    expect(page.get_by_role("option", name=f"Run {run_id[:8]}")).to_be_visible()
    page.keyboard.press("Escape")
    expect(page.get_by_role("dialog", name="Search")).to_be_hidden()

    page.keyboard.press("Control+k")
    page.get_by_role("combobox").fill("quarant")
    page.get_by_role("option", name="Quarantine").click()
    expect(page.locator("h1")).to_have_text("Quarantine")


def test_the_theme_switch_is_kept_for_the_user(signed_in: Page, site: Site):
    page = signed_in
    page.emulate_media(color_scheme="dark")
    page.goto(f"{site.url}/runs")
    expect(page.locator("html")).not_to_have_attribute("data-theme", "light")
    page.get_by_role("button", name="Switch to the light theme").click()
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    page.reload()
    expect(page.locator("html")).to_have_attribute("data-theme", "light")  # set before the page draws
    background = page.evaluate("() => getComputedStyle(document.body).backgroundColor")
    assert background == "rgb(243, 245, 248)", background
    page.locator(".user-menu summary").click()
    page.get_by_role("button", name="Follow the system theme").click()
    expect(page.locator("html")).not_to_have_attribute("data-theme", "light")


def test_the_column_chooser_hides_a_column_and_remembers(signed_in: Page, site: Site):
    page = signed_in
    page.goto(f"{site.url}/flows")
    owner = page.locator("table[data-table=flows] th", has_text="Owner")
    expect(owner).to_be_visible()
    page.locator("details.columns summary").click()
    page.get_by_role("checkbox", name="Owner").uncheck()
    expect(owner).to_be_hidden()
    page.reload()
    expect(page.locator("table[data-table=flows] th", has_text="Owner")).to_be_hidden()
    page.locator("details.columns summary").click()
    page.get_by_role("checkbox", name="Owner").check()
    expect(page.locator("table[data-table=flows] th", has_text="Owner")).to_be_visible()


def test_the_palette_moves_with_the_arrows_says_how_many_and_gives_the_focus_back(
    signed_in: Page, site: Site
):
    page = signed_in
    page.goto(f"{site.url}/flows")
    top = page.locator("#global-search")
    top.click()
    box = page.locator("#palette-input")
    expect(box).to_be_focused()
    expect(box).to_have_attribute("aria-expanded", "true")  # every page is listed for an empty search
    expect(box).to_have_attribute("aria-activedescendant", "palette-option-0")
    expect(page.locator("#palette-said")).to_have_text(re.compile(r"^\d+ results$"))
    page.keyboard.press("ArrowDown")
    expect(box).to_have_attribute("aria-activedescendant", "palette-option-1")
    expect(page.locator("#palette-option-1")).to_have_attribute("aria-selected", "true")
    page.keyboard.press("ArrowUp")
    expect(box).to_have_attribute("aria-activedescendant", "palette-option-0")
    box.fill("nothing-is-called-this")
    expect(page.get_by_text("Nothing matches.")).to_be_visible()
    expect(box).to_have_attribute("aria-expanded", "false")
    expect(page.locator("#palette-said")).to_have_text("0 results")
    expect(box).not_to_have_attribute("aria-activedescendant", re.compile(".*"))
    page.keyboard.press("Escape")
    expect(page.get_by_role("dialog", name="Search")).to_be_hidden()
    expect(top).to_be_focused()  # back where it was opened from


def test_a_row_menu_closes_on_escape_and_gives_the_focus_back(signed_in: Page, site: Site):
    page = signed_in
    page.goto(f"{site.url}/flows")
    more = page.locator("details.row-menu summary").first
    more.click()
    expect(page.locator("details.row-menu[open]")).to_have_count(1)
    page.keyboard.press("Escape")
    expect(page.locator("details.row-menu[open]")).to_have_count(0)
    expect(more).to_be_focused()


def test_a_row_opened_after_the_session_ends_goes_to_sign_in(signed_in: Page, site: Site):
    page = signed_in
    page.goto(f"{site.url}/flows")
    page.context.clear_cookies()
    page.get_by_role("button", name="Show the routes and runs of demo").click()
    expect(page).to_have_url(f"{site.url}/login")
    expect(page.locator("tr.child")).to_have_count(0)  # the sign-in page was not put into the table


def test_a_flow_row_opens_to_its_routes_and_runs_and_the_search_asks_the_server(signed_in: Page, site: Site):
    page = signed_in
    page.goto(f"{site.url}/flows")
    page.get_by_role("button", name="Show the routes and runs of demo").click()
    child = page.locator("tr.child").first
    expect(child.get_by_role("link", name="lake")).to_be_visible()
    expect(child.get_by_role("link", name="flaky")).to_be_visible()
    page.get_by_label("Search the flows").fill("nothing-like-this")
    expect(page.get_by_text("No flows match.")).to_be_visible()
    expect(page).to_have_url(re.compile(r"/flows\?q=nothing-like-this"))  # the address keeps the search


def test_ticking_rows_and_moving_with_the_keyboard(signed_in: Page, site: Site):
    page = signed_in
    page.goto(f"{site.url}/runs")
    cancel = page.get_by_role("button", name="Cancel", exact=True)
    expect(cancel).to_be_disabled()  # nothing ticked
    page.get_by_role("checkbox", name="Tick every run shown").check()
    expect(page.locator("[data-bulk-count]")).to_contain_text("ticked")
    expect(cancel).to_be_enabled()
    page.goto(f"{site.url}/routes/lake/deliveries")
    rows = page.locator("table[data-table=deliveries] > tbody > tr")
    assert rows.count() >= 2
    page.get_by_role("checkbox", name="Pick every delivery shown").check()
    expect(page.locator("table[data-table=deliveries] input[data-select]:checked")).to_have_count(
        rows.count()
    )
    first = rows.nth(0).locator("th a")
    first.focus()
    page.keyboard.press("ArrowDown")
    expect(rows.nth(1).locator("th a")).to_be_focused()
    page.keyboard.press("Home")
    expect(first).to_be_focused()


def test_the_status_bar_says_how_the_server_is(signed_in: Page, site: Site):
    page = signed_in
    page.goto(f"{site.url}/")
    bar = page.get_by_role("contentinfo")
    expect(bar).to_contain_text("Database reachable")
    expect(bar).to_contain_text("Environment")


@pytest.mark.skipif(
    not os.environ.get("FEATHER_SCREENSHOTS"), reason="set FEATHER_SCREENSHOTS=1 to take them"
)
@pytest.mark.parametrize("scheme", ["light", "dark"])
def test_take_the_screenshots(signed_in: Page, site: Site, scheme: str):
    page = signed_in
    page.set_viewport_size({"width": 1440, "height": 900})
    page.emulate_media(color_scheme=scheme)  # pyright: ignore[reportArgumentType]
    SHOTS.mkdir(parents=True, exist_ok=True)
    page.goto(f"{site.url}/")
    page.wait_for_load_state("networkidle")
    page.screenshot(path=str(SHOTS / f"overview-{scheme}.png"))
    page.goto(f"{site.url}/flows")
    page.locator(".charts-toggle").click()
    page.get_by_role("button", name="Show the routes and runs of demo").click()
    expect(page.locator("tr.child").first.get_by_role("link", name="lake")).to_be_visible()
    page.wait_for_load_state("networkidle")
    page.screenshot(path=str(SHOTS / f"flows-{scheme}.png"))
    page.locator(".charts-toggle").click()  # off again, for the next page
    page.goto(f"{site.url}/runs")
    page.wait_for_load_state("networkidle")
    page.screenshot(path=str(SHOTS / f"runs-{scheme}.png"))

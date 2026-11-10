"""What an operator does in the dashboard, in Chromium: read a day again, replay a delivery, retry a failure, with the
mouse and with the keyboard alone, and the read-only pages with JavaScript off. Two of these are M1 exit criteria."""

import pytest
from dashboard_site import DAYS, Site, wait_until
from playwright.sync_api import Page, expect

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(600)]

READ_ONLY = [
    "/",
    "/flows",
    "/flows/dashboard/demo",
    "/datasets/dashboard/demo.items/partitions",
    "/runs",
    "/failures",
    "/connections",
    "/datasets",
    "/deliveries",
    "/quarantine",
    "/secrets",
    "/users",
    "/search?q=demo",
    "/packages",
    "/packages/acme-invoices",
    "/packages/acme-invoices/flows/invoices",
    "/registries",
]
FIRST = f"day={DAYS[0]}"


def test_rerun_from_the_dashboard(signed_in_fresh: Page, fresh_site: Site):  # M1 exit criterion
    page, site = signed_in_fresh, fresh_site
    site.write_rows({DAYS[0]: 3, DAYS[1]: 2, DAYS[2]: 2})  # the source now has a third item on the first day
    page.goto(f"{site.url}/datasets/dashboard/demo.items/partitions")
    page.locator(f'input[name="partition"][value="{FIRST}"]').check()
    page.get_by_role("button", name="Read the selected partitions again").click()
    expect(page.get_by_role("heading", name="Check before you confirm")).to_be_visible()
    page.get_by_role("button", name="Read them again").click()

    def cell_updated() -> bool:
        page.goto(f"{site.url}/datasets/dashboard/demo.items/partitions")
        row = page.locator("tr", has=page.locator(f'input[value="{FIRST}"]'))
        return row.locator("td.num").first.inner_text().strip() == "3"

    wait_until(cell_updated, "the first day's cell to show three records")
    wait_until(
        lambda: len(site.deliveries("lake", "applied")) == len(DAYS) + 1, "the new version at the lake"
    )


def test_replay_from_the_dashboard(signed_in_fresh: Page, fresh_site: Site):  # M1 exit criterion
    page, site = signed_in_fresh, fresh_site
    before = sum(row["key"].startswith(DAYS[0]) for row in site.captured())
    page.goto(f"{site.url}/routes/dashboard/lake/deliveries?partition={FIRST}")
    page.locator('input[name="version_id"]').first.check()
    page.get_by_role("button", name="Replay picked").click()
    expect(page.get_by_role("heading", name="Check before you confirm")).to_be_visible()
    page.get_by_label("Reason").fill("the lake lost the day")
    page.get_by_role("button", name="Confirm").click()
    wait_until(
        lambda: sum(row["key"].startswith(DAYS[0]) for row in site.captured()) == 2 * before,
        "the lake to receive the first day again",
    )


def test_retry_after_a_failure(signed_in_fresh: Page, fresh_site: Site):
    page, site = signed_in_fresh, fresh_site
    page.goto(f"{site.url}/failures")
    page.get_by_role("button", name="Retry").first.click()
    expect(page.get_by_role("heading", name="Check before you confirm")).to_be_visible()
    page.get_by_label("Reason").fill("flaky took it on a second try")
    page.get_by_role("button", name="Confirm").click()
    wait_until(lambda: len(site.deliveries("flaky", "applied")) == len(DAYS), "the retried deliveries")
    assert site.deliveries("flaky", "failed") == []


def test_read_only_pages_work_with_javascript_disabled(no_script_page: Page, site: Site):
    for path in READ_ONLY:
        answer = no_script_page.goto(f"{site.url}{path}")
        assert answer is not None and answer.status == 200, path
        expect(no_script_page.locator("h1")).to_be_visible()
        expect(no_script_page.locator("main")).not_to_contain_text("Something went wrong")


def test_no_console_errors_or_csp_violations(signed_in: Page, site: Site, console: list[str]):
    for path in [*READ_ONLY, "/routes/dashboard/lake/deliveries", "/quarantine"]:
        signed_in.goto(f"{site.url}{path}")
        signed_in.wait_for_load_state("networkidle")
    assert console == []


def _tab_to(page: Page, text: str, limit: int = 80) -> None:
    """Press Tab until the focused control says `text`."""
    for _ in range(limit):
        page.keyboard.press("Tab")
        focused = page.evaluate("() => (document.activeElement && document.activeElement.textContent) || ''")
        if focused.strip().startswith(text):
            return
    raise AssertionError(f"Tab never reached a control saying {text!r}")


def test_keyboard_only_retry(signed_in_fresh: Page, fresh_site: Site):
    page, site = signed_in_fresh, fresh_site
    page.goto(f"{site.url}/failures")
    _tab_to(page, "Retry")
    page.keyboard.press("Enter")
    expect(page.get_by_role("heading", name="Check before you confirm")).to_be_visible()
    page.get_by_label("Reason").focus()
    page.keyboard.type("retried from the keyboard")
    _tab_to(page, "Confirm")
    page.keyboard.press("Enter")
    wait_until(lambda: len(site.deliveries("flaky", "applied")) == len(DAYS), "the retried deliveries")


@pytest.mark.parametrize("path", READ_ONLY)
def test_pages_fit_a_phone_without_scrolling_sideways(
    signed_in: Page, site: Site, path: str
):  # Stage 2 minor
    signed_in.set_viewport_size({"width": 360, "height": 740})
    signed_in.goto(f"{site.url}{path}")
    signed_in.get_by_role("button", name="Menu").click()  # the sidebar folds behind its button on a phone
    nav = signed_in.locator("nav").first
    box = nav.bounding_box()
    assert box is not None and box["x"] + box["width"] <= 360 + 0.5, f"the navigation overflows on {path}"
    wide = signed_in.evaluate("() => document.documentElement.scrollWidth")
    assert wide <= 360, f"{path} is {wide} px wide"


def test_from_a_delivery_to_what_was_read(signed_in: Page, site: Site):
    page = signed_in
    version = site.deliveries("lake", "applied")[0].version_id
    page.goto(f"{site.url}/routes/dashboard/lake/deliveries/{version}")
    page.get_by_role("link", name="What was read").click()
    expect(page).to_have_url(f"{site.url}/versions/{version}")
    rows = page.locator("#records tbody tr")
    expect(rows).to_have_count(2)
    first_key = rows.first.locator("th").inner_text().strip()
    page.get_by_label("Filter the records on this page").fill(first_key)
    expect(page.locator("#records tbody tr:visible")).to_have_count(1)
    rows.first.get_by_text("Show JSON").click()
    expect(rows.first.locator("pre")).to_contain_text(first_key)


def test_the_sidebar_opens_on_a_narrow_screen(signed_in: Page, site: Site):
    page = signed_in
    page.set_viewport_size({"width": 600, "height": 800})
    page.goto(f"{site.url}/")
    menu = page.get_by_role("button", name="Menu")
    expect(menu).to_have_attribute("aria-expanded", "false")
    expect(page.get_by_role("navigation", name="Main")).to_be_hidden()
    menu.click()
    expect(menu).to_have_attribute("aria-expanded", "true")
    expect(page.get_by_role("navigation", name="Main")).to_be_visible()
    page.get_by_role("navigation", name="Main").get_by_role("link", name="Runs").click()
    expect(page.locator("h1")).to_contain_text("Runs")
    page.set_viewport_size({"width": 1200, "height": 800})
    expect(page.get_by_role("button", name="Menu")).to_be_hidden()
    expect(page.get_by_role("navigation", name="Main")).to_be_visible()


def test_trace_a_record_from_a_failed_delivery(signed_in: Page, site: Site):
    page = signed_in
    version = site.deliveries("flaky", "failed")[0].version_id
    page.goto(f"{site.url}/routes/dashboard/flaky/deliveries/{version}")
    answers = page.locator("#h-responses ~ .table-wrap tbody tr")
    key = answers.first.locator("th code").inner_text().strip()
    answers.first.get_by_role("link", name=f"Trace {key}").click()
    expect(page.locator("h1")).to_contain_text(key)
    flaky = page.locator(".delivery", has=page.get_by_role("heading", name="flaky"))
    expect(flaky.first).to_contain_text("503")
    expect(flaky.first).to_contain_text("simulated delivery failure to flaky")
    sent = flaky.first.locator("details", has_text="The body it was sent with")
    expect(sent.locator("pre")).to_contain_text(f'"id": "{key}"')


@pytest.mark.parametrize("width", [1280, 360])
def test_open_a_tasks_records_from_the_run_page(signed_in: Page, site: Site, width: int):
    page = signed_in
    page.set_viewport_size({"width": width, "height": 800})
    path = site.records_path("apply", "flaky")
    run_path = path.split("/tasks/")[0]
    page.goto(f"{site.url}{run_path}")
    page.locator(f'a[href="{path}"]').click()
    panel = page.locator("#records-panel")
    expect(panel.get_by_role("heading", name="Records sent to dashboard/flaky")).to_be_visible()
    expect(page).to_have_url(f"{site.url}{run_path}")  # opened in place, not as a page of its own
    first = panel.locator("tbody tr").first
    expect(first).to_contain_text("503")
    first.get_by_text("Show").click()
    expect(first.locator("pre")).to_contain_text('"id"')
    expect(first.get_by_role("link", name="Full trace")).to_be_visible()
    wide = page.evaluate("() => document.documentElement.scrollWidth")
    assert wide <= width, f"the run page with its records is {wide} px wide: {_sticking_out(page, width)}"


def _sticking_out(page: Page, width: int) -> list[str]:
    """The innermost elements outside a table's scroll box whose right edge is past `width`, to say what to fix."""
    return page.evaluate(
        """(width) => [...document.querySelectorAll("body *")]
            .filter(e => !e.closest(".table-wrap") && e.getBoundingClientRect().right > width + 0.5)
            .filter(e => ![...e.children].some(c => c.getBoundingClientRect().right > width + 0.5))
            .slice(0, 10)
            .map(e => `${e.tagName.toLowerCase()}.${e.className} to ${Math.round(e.getBoundingClientRect().right)} px: `
                + (e.textContent || "").trim().slice(0, 60))""",
        width,
    )


def test_a_tasks_records_without_javascript(no_script_page: Page, site: Site):
    path = site.records_path("extract")
    answer = no_script_page.goto(f"{site.url}{path}")
    assert answer is not None and answer.status == 200
    expect(no_script_page.locator("h1")).to_have_text("Records of an extract task")
    expect(no_script_page.locator("#records-panel tbody tr")).to_have_count(2)


def test_keyboard_only_switch_of_a_packaged_flow(signed_in_fresh: Page, fresh_site: Site):  # task 9
    page, site = signed_in_fresh, fresh_site
    page.goto(f"{site.url}/packages/acme-invoices/flows/invoices")
    _tab_to(page, "Switch version")
    page.keyboard.press("Enter")
    dialog = page.get_by_role("dialog", name="Are you sure?")
    expect(dialog).to_be_visible()
    expect(dialog).to_contain_text("Switch acme-invoices/invoices")
    _tab_to(page, "Switch version")  # the dialog's own button, named for the action
    page.keyboard.press("Enter")
    expect(page.get_by_role("status").filter(has_text="Asked for")).to_be_visible()
    activating = site.stack.store.packages.activating()
    assert [(a.flow, a.version) for a in activating] == [("acme-invoices/invoices", "1.1.0")]

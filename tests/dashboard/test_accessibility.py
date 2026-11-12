"""axe-core on the main pages, in light and dark: no WCAG 2.1 A or AA violations (NFR-13)."""

import pytest
from axe_playwright_python.sync_playwright import Axe
from dashboard_site import Site
from playwright.sync_api import Page

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(300)]

PATHS = [
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
    "/flows?q=demo&active=1",
    "/versions/{version}",  # a version page, of the lake's first delivery
    "/routes/dashboard/lake/deliveries/{version}",
    "/trace",
    "/trace?dataset=demo.items&key=2026-11-01-0",  # a record's trace: a version, two routes, a failure
    "{records:extract}",  # a task's Records: what a read sealed
    "{records:apply:flaky}",  # and what a route answered, turned down
    "/packages",  # task 9: the Packages pages and the Registries page
    "/packages/acme-invoices",
    "/packages/acme-invoices/flows/invoices",
    "/registries",
    "/readiness",
    "/holds",  # M2 plan 2 A1
    "/budgets",  # M2 plan 2 O1
    "/audit",  # M2 plan 2 O3
    "/alerts",  # M2 plan 2 O6
]
WCAG_AA = {"runOnly": {"type": "tag", "values": ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"]}}


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("scheme", ["light", "dark"])
def test_axe_finds_no_wcag_aa_violations(signed_in: Page, site: Site, path: str, scheme: str):  # NFR-13
    signed_in.emulate_media(color_scheme=scheme)  # pyright: ignore[reportArgumentType]
    if "{version}" in path:
        path = path.format(version=site.deliveries("lake", "applied")[0].version_id)
    if path.startswith("{records:"):
        kind, _, route = path.strip("{}").removeprefix("records:").partition(":")
        path = site.records_path(kind, route or None)
    signed_in.goto(f"{site.url}{path}")
    signed_in.wait_for_load_state("networkidle")
    results = Axe().run(signed_in, options=WCAG_AA)
    assert results.violations_count == 0, results.generate_report()

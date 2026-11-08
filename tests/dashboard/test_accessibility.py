"""axe-core on the main pages, in light and dark: no WCAG 2.1 A or AA violations (NFR-13)."""

import pytest
from axe_playwright_python.sync_playwright import Axe
from dashboard_site import Site
from playwright.sync_api import Page

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(300)]

PATHS = [
    "/",
    "/flows",
    "/flows/demo",
    "/datasets/demo.items/partitions",
    "/runs",
    "/failures",
    "/connections",
]
WCAG_AA = {"runOnly": {"type": "tag", "values": ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"]}}


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("scheme", ["light", "dark"])
def test_axe_finds_no_wcag_aa_violations(signed_in: Page, site: Site, path: str, scheme: str):  # NFR-13
    signed_in.emulate_media(color_scheme=scheme)  # pyright: ignore[reportArgumentType]
    signed_in.goto(f"{site.url}{path}")
    signed_in.wait_for_load_state("networkidle")
    results = Axe().run(signed_in, options=WCAG_AA)
    assert results.violations_count == 0, results.generate_report()

"""The exit run: a Zakya organisation's invoices into MotherDuck, twice, the second run taking only what changed.

`test_zakya_invoices_reach_motherduck` runs it against a real Zakya test organisation. It is opt-in: marked `live`, and
skipped unless every variable below is set.

    ZAKYA_LIVE_ORGANIZATION_ID   the test organisation's id
    ZAKYA_LIVE_DOMAIN            com, in or eu (com if unset)
    ZAKYA_LIVE_CLIENT_ID         a Zoho API console "self client" of the organisation's region
    ZAKYA_LIVE_CLIENT_SECRET
    ZAKYA_LIVE_REFRESH_TOKEN     made with the scopes ZohoInventory.invoices.READ and
                                 ZohoInventory.inventoryadjustments.READ
    ZAKYA_LIVE_DAILY_CAP         the organisation's daily call cap (discovery Q4; 1000 if unset)
    MOTHERDUCK_TOKEN
    ZAKYA_LIVE_MOTHERDUCK_DB     such as md:feather_live; the run writes the table zakya_invoices_live

    uv run pytest packages/connectors/feather-zakya/tests/test_live.py -m live -v -s

It also checks, reading only, that `/inventoryadjustments` filters by `reference_number`, which `zakya.Stock` counts
on to post an adjustment once: a reference no adjustment has finds none, and one an adjustment has finds that one.

The same run against the simulator and a DuckDB file, `test_the_exit_run_works_on_the_simulator`, runs every time.
"""

import json
import logging
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb as duck
import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import (
    duckdb,  # pyright: ignore[reportAttributeAccessIssue]
    zakya,  # pyright: ignore[reportAttributeAccessIssue]
)
from feather_connect.connectors.zakya.testing import ZakyaSim, running  # pyright: ignore[reportMissingImports]
from feather_connect.core.context import Context
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import BudgetSpec, parse_limits
from feather_connect.storage.spool import MemorySpool

NEEDED = (
    "ZAKYA_LIVE_ORGANIZATION_ID",
    "ZAKYA_LIVE_CLIENT_ID",
    "ZAKYA_LIVE_CLIENT_SECRET",
    "ZAKYA_LIVE_REFRESH_TOKEN",
    "MOTHERDUCK_TOKEN",
    "ZAKYA_LIVE_MOTHERDUCK_DB",
)
TABLE = "zakya_invoices_live"


def _row(invoice: Mapping[str, Any], ctx: Context) -> dict[str, Any]:
    """An invoice as a flat row: its lines kept as JSON text."""
    return {**invoice, "line_items": json.dumps(invoice.get("line_items") or [], default=str)}


def reference_filter_works(zakya_config: Mapping[str, Any]) -> int:
    """Check that adjustments are filtered by `reference_number`, reading only. Returns how many adjustments the first
    page listed with a reference, so a caller can tell an organisation with none (only the first half checked)."""
    stock = zakya.Stock("zakya")
    ctx = Context(dict(zakya_config), logging.getLogger("zakya-live"), datetime.now(UTC), "UTC")
    assert stock._posted(ctx, "FC-no-such-reference-000") is None  # pyright: ignore[reportPrivateUsage]
    listed = stock.call(ctx, "GET", "/inventoryadjustments", params={"per_page": 25})
    held = [a for a in listed.get("inventory_adjustments") or [] if a.get("reference_number")]
    if held:
        reference = held[0]["reference_number"]
        found = stock.call(ctx, "GET", "/inventoryadjustments", params={"reference_number": reference})
        assert {a.get("reference_number") for a in found.get("inventory_adjustments") or []} == {reference}
    return len(held)


def exit_run(
    zakya_config: Mapping[str, Any], warehouse: Mapping[str, Any], daily_cap: int
) -> tuple[int, int, float]:
    """Run the invoices flow twice into `warehouse`'s TABLE. Returns the rows and distinct invoices it holds, and the
    calls the day's window counted."""
    sink = duckdb.Table("warehouse", table=TABLE, key_columns=["invoice_id"])
    flow = fc.Flow(
        "zakya-invoices-live",
        reads=zakya.dataset("invoices"),
        deliver=[fc.to(sink, map=_row)],
        triggers=[fc.every("1h")],
    )
    configs = {"zakya": dict(zakya_config), "warehouse": dict(warehouse)}
    clock = FrozenClock(datetime.now(UTC))  # virtual waits from the real now; HTTP and tokens use real time
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    account = f"zakya:{zakya_config['organization_id']}"
    store.budgets.define(
        BudgetSpec("zoho-api", parse_limits(["90/minute", f"{daily_cap}/day"]), account=account)
    )
    runtime = Runtime(
        [flow],
        store,
        spool,
        engine,
        code_sha="live",
        clock=clock,
        budgets={"zakya": "zoho-api"},
        config_of=lambda connection: configs[connection],
    )
    engine.register(runtime.tasks())
    for _ in range(2):  # the second by last_modified_time: only what changed since
        run = runtime.start_run(flow.name)
        engine.run_until_idle()
        assert store.runs.get_run(run).status == "succeeded"
    settings = {"motherduck_token": warehouse["token"]} if warehouse.get("token") else {}
    with duck.connect(str(warehouse["database"]), config=settings) as conn:
        row = conn.execute(f"select count(*), count(distinct invoice_id) from {TABLE}").fetchone()
    assert row is not None
    return int(row[0]), int(row[1]), store.budgets.usage("zoho-api")[1].used


def test_the_exit_run_works_on_the_simulator(tmp_path: Path):
    sim = ZakyaSim(start=7)
    with running(sim) as url:
        config = {
            "organization_id": sim.organization_id,
            "base_url": url,
            "accounts_url": url,
            "client_id": "1000.SIMCLIENT",
            "client_secret": "sim-client-secret",
            "refresh_token": "1000.sim-refresh-token",
        }
        rows, invoices, calls = exit_run(config, {"database": str(tmp_path / "warehouse.duckdb")}, 1000)
        sim.seed("inventoryadjustments", [{"inventory_adjustment_id": "a1", "reference_number": "FC-held"}])
        assert reference_filter_works(config) == 1
    assert rows == invoices == 7
    # A token, the list and each invoice; then the second run reads two minutes back, the same seven again, which
    # by_stamp drops as unchanged: the overlap costs calls, never a duplicate.
    assert round(calls) == 1 + (1 + 7) + (1 + 7)


@pytest.mark.live
@pytest.mark.timeout(1800)
@pytest.mark.skipif(
    not all(os.environ.get(n) for n in NEEDED), reason="the live Zakya run needs " + ", ".join(NEEDED)
)
def test_zakya_invoices_reach_motherduck():
    env = os.environ
    config = {
        "organization_id": env["ZAKYA_LIVE_ORGANIZATION_ID"],
        "domain": env.get("ZAKYA_LIVE_DOMAIN", "com"),
        "client_id": env["ZAKYA_LIVE_CLIENT_ID"],
        "client_secret": env["ZAKYA_LIVE_CLIENT_SECRET"],
        "refresh_token": env["ZAKYA_LIVE_REFRESH_TOKEN"],
    }
    warehouse = {"database": env["ZAKYA_LIVE_MOTHERDUCK_DB"], "token": env["MOTHERDUCK_TOKEN"]}
    cap = int(env.get("ZAKYA_LIVE_DAILY_CAP", "1000"))
    rows, invoices, calls = exit_run(config, warehouse, cap)
    print(f"\n{invoices} invoices in {TABLE}; {calls:.0f} of the day's {cap} calls used by two runs")
    assert rows == invoices > 0
    held = reference_filter_works(config)
    print(
        f"the reference_number filter works{'' if held else ' (no adjustment has a reference: half checked)'}"
    )

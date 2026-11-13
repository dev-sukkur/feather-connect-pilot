"""M3.1's flows through a real worker on Postgres (plan 1, F1-F3): UC-06's effective price as a derived dataset (H18),
a SQL transform that joins a reference (H16, H17), and a sink with an availability window (H19).

The clock is the wall clock here, so a future-dated price is dated a few seconds ahead with `{{ now }}`, and the
window opens at the next minute but one. The same cases run on a frozen clock in packages/feather-connect/tests/core
(test_sql.py, test_derived.py, test_windows.py), where days and daylight saving are moved through.
"""

import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest
from real_stack import scratch_database, server_url, start_worker, wait_until

pytestmark = pytest.mark.postgres

PACKAGE = "m31-flows"
LEAD = 20  # seconds: how far ahead the future-dated price is


@pytest.fixture
def pg_url() -> Iterator[str]:
    """A database of its own, so no other test's routes or reference datasets are in the catalogue."""
    with scratch_database(server_url()) as url:
        yield url


FLOWS = """
import feather_connect as fc
from feather_connect.connectors import simulation

root = Path(ROOT)  # the test's folder: the installed copy of this file runs from elsewhere


def source(connection, name):
    return simulation.Source(connection, fixture=root / "fixtures" / f"{name}.json")


def sink(connection, name):
    return simulation.Sink(connection, capture_to=root / "out" / f"{name}.jsonl")


# UC-06: prices with valid-from times, and the price in effect now, recomputed on each run.
price = fc.Dataset(
    "sap.price", source=source("sap", "prices"), key=("item", "valid_from"),
    strategy=fc.strategy.snapshot_diff(), keep_state=True,
)
prices = fc.Flow("prices", reads=price, deliver=[fc.to(sink("lake", "lake"), name="prices-lake")])
effective = fc.Dataset(
    "pricing.effective_price",
    source=fc.derived(
        "select item, price from {{ dataset('sap.price') }} "
        "where cast(valid_from as timestamptz) <= {{ now }} "
        "qualify row_number() over (partition by item order by valid_from desc) = 1"
    ),
    key="item",
    strategy=fc.strategy.snapshot_diff(),
)
to_pos = fc.Flow("effective-to-pos", reads=effective, deliver=[fc.to(sink("pos", "pos"), name="pos")])

# H16, H17: order lines enriched from the material master by a SQL transform.
material = fc.Dataset(
    "sap.material", source=source("sap", "materials"), key="material",
    strategy=fc.strategy.snapshot_diff(), keep_state=True,
)
materials = fc.Flow("materials", reads=material, deliver=[fc.to(sink("lake", "materials"), name="materials-lake")])
line = fc.Dataset("zakya.line", source=source("zakya", "lines"), key="line", strategy=fc.strategy.replace())
lines = fc.Flow(
    "lines",
    reads=line,
    deliver=[
        fc.to(
            sink("staging", "staging"),
            name="lines-staging",
            map=fc.sql(
                "select b.*, m.description from batch b left join {{ dataset('sap.material') }} m using (material)"
            ),
        )
    ],
)

# H19: a sink that takes writes only in its window.
order = fc.Dataset("orders", source=source("zakya", "orders"), key="id", strategy=fc.strategy.replace())
night = fc.Flow("night", reads=order, deliver=[fc.to(sink("night_sink", "night"), name="night-sink")])
"""


def hhmm(at: datetime) -> str:
    return at.strftime("%H:%M")


@pytest.fixture
def project(tmp_path: Path) -> Path:
    now = datetime.now(UTC)
    opens = (now + timedelta(minutes=2)).replace(second=0, microsecond=0)
    for name in ("flows", "env", "fixtures"):
        (tmp_path / name).mkdir()
    (tmp_path / "feather.yaml").write_text(
        dedent(
            f"""
            name: {PACKAGE}
            modules: [flows.m31]
            connections:
              sap: {{connector: simulation.source}}
              zakya: {{connector: simulation.source}}
              lake: {{connector: simulation.sink}}
              pos: {{connector: simulation.sink}}
              staging: {{connector: simulation.sink}}
              night_sink:
                connector: simulation.sink
                windows: {{tz: UTC, open: ["{hhmm(opens)}-{hhmm(opens + timedelta(minutes=30))}"], applies: [write]}}
            """
        )
    )
    (tmp_path / "env" / "dev.yaml").write_text("")
    (tmp_path / "flows" / "__init__.py").write_text("")
    (tmp_path / "flows" / "m31.py").write_text(
        f"from pathlib import Path\n\nROOT = {str(tmp_path)!r}\n" + FLOWS
    )
    soon = (now + timedelta(seconds=LEAD)).isoformat()
    fixtures = {
        "prices": [
            {"item": "A", "price": 100, "valid_from": "2026-01-01T00:00:00+00:00"},
            {"item": "A", "price": 120, "valid_from": soon},  # future-dated
            {"item": "B", "price": 50, "valid_from": "2026-01-01T00:00:00+00:00"},
        ],
        "materials": [{"material": "M1", "description": "Steel bar"}],
        "lines": [{"line": 1, "material": "M1"}, {"line": 2, "material": "M9"}],
        "orders": [{"id": 1}, {"id": 2}],
    }
    for name, rows in fixtures.items():
        write_fixture(tmp_path, name, rows)
    (tmp_path / "opens.txt").write_text(opens.isoformat())
    return tmp_path


def write_fixture(project: Path, name: str, rows: list[dict[str, Any]]) -> None:
    (project / "fixtures" / f"{name}.json").write_text(json.dumps(rows))


def captured(project: Path, name: str) -> list[dict[str, Any]]:
    out = project / "out" / f"{name}.jsonl"
    return [json.loads(line) for line in out.read_text().splitlines()] if out.exists() else []


def run(store: Any, flow: str, timeout: float = 120) -> str:
    """Queue a run of the package's `flow`, as the scheduler does, and wait for it to succeed."""
    version = store.packages.versions(PACKAGE)[-1]
    params = {"due_at": datetime.now(UTC).isoformat(), "package": PACKAGE, "version": version.version}
    run_id = store.runs.create_run(f"{PACKAGE}/{flow}", "manual", "test", params)
    wait_until(lambda: store.runs.get_run(run_id).status in {"succeeded", "failed"}, timeout=timeout)
    assert store.runs.get_run(run_id).status == "succeeded", store.runs.get_run(run_id)
    return run_id


@pytest.fixture
def stack(project: Path, pg_url: str, db, workers) -> Any:
    store, _ = db
    workers.append(start_worker(project, pg_url, "w-m31-" + uuid.uuid4().hex[:6]))
    wait_until(lambda: store.catalog.flow(f"{PACKAGE}/prices"), 90)
    return store


def test_uc06_today_s_effective_price_reaches_the_pos_and_only_the_change_follows(project, stack):  # H18
    run(stack, "prices")
    run(stack, "effective-to-pos")
    pos = {r["payload"]["item"]: r["payload"]["price"] for r in captured(project, "pos")}
    assert pos == {"A": 100, "B": 50}
    sent = len(captured(project, "pos"))

    wait_until(lambda: datetime.now(UTC) > datetime.fromisoformat(_soon(project)), LEAD + 10)
    run(stack, "effective-to-pos")  # nothing changed at SAP: the clock moved past A's new price
    later = captured(project, "pos")[sent:]
    assert [(r["op"], r["payload"]["item"], r["payload"]["price"]) for r in later] == [("upsert", "A", 120)]

    run(stack, "effective-to-pos")  # recomputed again: nothing new
    assert len(captured(project, "pos")) == sent + 1


def _soon(project: Path) -> str:
    rows = json.loads((project / "fixtures" / "prices.json").read_text())
    return max(r["valid_from"] for r in rows)


def test_a_sql_transform_joins_the_material_master_and_a_late_material_completes_the_line(
    project, stack
):  # H16, H17
    run(stack, "materials")
    run(stack, "lines")
    first = {r["payload"]["line"]: r["payload"]["description"] for r in captured(project, "staging")}
    assert first == {1: "Steel bar", 2: None}  # the left join keeps the line whose material isn't known yet

    write_fixture(
        project,
        "materials",
        [{"material": "M1", "description": "Steel bar"}, {"material": "M9", "description": "Bolt"}],
    )
    run(stack, "materials")
    run(stack, "lines")
    latest = {r["payload"]["line"]: r["payload"]["description"] for r in captured(project, "staging")}
    assert latest == {1: "Steel bar", 2: "Bolt"}  # the next delivery read the new reference version (H16)


def test_a_closed_window_holds_the_write_visibly_until_it_opens(project, stack):  # H19
    opens = datetime.fromisoformat((project / "opens.txt").read_text())
    version = stack.packages.versions(PACKAGE)[-1]
    params = {"due_at": datetime.now(UTC).isoformat(), "package": PACKAGE, "version": version.version}
    run_id = stack.runs.create_run(f"{PACKAGE}/night", "manual", "test", params)
    wait_until(lambda: stack.budgets.waits("window:night_sink"), 90)  # the wait is noted, with when it ends
    [wait] = stack.budgets.waits("window:night_sink")
    assert wait.until == opens and wait.run_id == run_id
    assert captured(project, "night") == []  # nothing written while closed
    wait_until(lambda: stack.runs.get_run(run_id).status == "succeeded", 240)
    assert datetime.now(UTC) >= opens
    assert sorted(r["payload"]["id"] for r in captured(project, "night")) == [1, 2]  # once each

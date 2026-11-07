"""`feather run --local [--sim]` and `feather test`."""

import json
from collections.abc import Callable
from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import Result

FEATHER_YAML = """
    name: shop
    modules: [flows.demo]
    settings: {timezone: UTC}
    connections:
      shop: {connector: postgres.query, dsn: "postgresql://nobody@127.0.0.1:9/none"}
      warehouse:
        connector: duckdb.table
        database: md:bronze
        auth: {token: "${secret:env:MOTHERDUCK_TOKEN}"}
      archive: {connector: memory.sink}
"""

FLOW = """
    import feather_connect as fc
    from feather_connect.connectors import duckdb, memory, postgres

    orders = fc.Dataset(
        "orders",
        source=postgres.Query("shop", table="orders"),
        key="order_id",
        strategy=fc.strategy.replace(),
        partition=fc.partition.by_day("day", tz="UTC"),
    )
    flow = fc.Flow(
        "demo",
        reads=orders,
        deliver=[
            fc.to(duckdb.Table("warehouse", table="bronze_orders", key_columns=["order_id"])),
            fc.to(memory.Sink("archive")),
        ],
        triggers=[fc.manual()],
    )
"""

ROWS = [
    {"order_id": "o1", "day": "2026-11-06", "total": 10},
    {"order_id": "o2", "day": "2026-11-07", "total": 12},
    {"order_id": "o3", "day": "2026-11-07", "total": 7},
]


@pytest.fixture
def demo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A project whose flow reads Postgres and writes MotherDuck, with fixtures for --sim."""
    files = {
        "feather.yaml": dedent(FEATHER_YAML),
        "env/dev.yaml": "",
        "flows/__init__.py": "",
        "flows/demo.py": dedent(FLOW),
        "fixtures/orders.json": json.dumps(ROWS),
        "tests/test_demo.py": "def test_ok():\n    assert True\n",
    }
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    return tmp_path


def test_run_local_sim_reports_per_route_counts(cli: Callable[..., Result], demo: Path) -> None:
    r = cli("run", "demo", "--local", "--sim", "--json")
    assert r.exit_code == 0, r.output
    doc = json.loads(r.stdout)
    assert doc["status"] == "succeeded" and doc["routes"]["warehouse"]["applied"] == 3
    assert doc["routes"]["archive"]["applied"] == 3 and doc["quarantine"] == {}


def test_run_local_without_sim_names_the_missing_credential(cli: Callable[..., Result], demo: Path) -> None:
    r = cli("run", "demo", "--local")
    assert r.exit_code != 0 and "warehouse" in r.stderr and "MOTHERDUCK_TOKEN" in r.stderr
    assert "--sim" in r.stderr
    r = cli("run", "demo", "--local", "--json")
    assert r.exit_code != 0 and "MOTHERDUCK_TOKEN" in json.loads(r.stdout)["error"]


def test_partition_option_runs_one_slice(cli: Callable[..., Result], demo: Path) -> None:
    r = cli("run", "demo", "--local", "--sim", "--partition", "day=2026-11-07", "--json")
    doc = json.loads(r.stdout)
    assert r.exit_code == 0 and doc["routes"]["warehouse"]["applied"] == 2
    assert doc["partitions"] == ["day=2026-11-07"]


def test_failed_local_run_exits_non_zero_with_the_reason(cli: Callable[..., Result], demo: Path) -> None:
    r = cli("run", "demo", "--local", "--sim", "--partition", "day=not-a-day", "--json")
    assert r.exit_code != 0 and "not-a-day" in json.loads(r.stdout)["error"]
    (demo / "fixtures" / "orders.json").unlink()
    r = cli("run", "demo", "--local", "--sim")
    assert r.exit_code != 0 and "fixtures/orders.json" in r.stderr


def test_unknown_flow_lists_the_flows(cli: Callable[..., Result], demo: Path) -> None:
    r = cli("run", "nope", "--local", "--sim", "--json")
    assert r.exit_code == 1 and "demo" in json.loads(r.stdout)["hint"]


def test_feather_test_passes_on_a_new_project_and_returns_json(
    cli: Callable[..., Result], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cli("new", "project", "fresh", "--dir", str(tmp_path)).exit_code == 0
    monkeypatch.chdir(tmp_path / "fresh")
    r = cli("test", "--json")
    assert r.exit_code == 0, r.output
    doc = json.loads(r.stdout)
    assert doc["ok"] and doc["passed"] == doc["tests"] >= 2 and doc["failed"] == 0


def test_feather_test_fails_with_pytests_code(cli: Callable[..., Result], demo: Path) -> None:
    (demo / "tests" / "test_demo.py").write_text("def test_no():\n    assert False\n", encoding="utf-8")
    r = cli("test", "--json")
    doc = json.loads(r.stdout)
    assert r.exit_code == 1 and doc["failed"] == 1 and not doc["ok"]

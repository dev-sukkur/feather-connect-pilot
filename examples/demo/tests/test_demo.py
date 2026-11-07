"""The demo project: every environment loads, qa runs on stand-ins, and dev points at the targets on this machine."""

import json
from pathlib import Path

from feather_connect.project import load_project, validate_project
from feather_connect.testing.harness import FlowHarness

ROOT = Path(__file__).resolve().parents[1]


def test_every_environment_loads_and_validates(monkeypatch):
    monkeypatch.setenv("FEATHER_DATABASE_URL", "postgresql://feather@127.0.0.1/feather")
    for env in ("dev", "qa", "uat", "prod"):
        project = load_project(ROOT, env)
        assert [p for p in validate_project(project) if p.severity == "error"] == [], env


def test_qa_runs_on_stand_ins_of_the_real_connectors():
    (flow,) = load_project(ROOT, "qa").flows
    assert all(getattr(route.sink, "stands_in", False) for route in flow.routes)


def test_dev_delivers_to_the_targets_on_this_machine():
    project = load_project(ROOT, "dev")
    connections = project.connections
    assert connections["warehouse"].connector == "postgres.table"
    assert connections["warehouse"].options["schema"] == "demo"
    assert connections["lake"].options["database"] == ".data/lake.duckdb"
    assert connections["erp"].options["base_url"] == "http://127.0.0.1:29001"
    assert [p.name for p in project.dev_processes] == ["demo-targets"]


def test_the_flow_delivers_the_fixtures():
    (flow,) = load_project(ROOT, "qa").flows
    rows = json.loads((ROOT / "fixtures" / "orders.json").read_text(encoding="utf-8"))
    harness = FlowHarness(flow)
    harness.source("orders").load(rows)
    harness.run()
    for route in flow.routes:
        delivered = harness.sink(route.name).rows
        assert sorted(r["order_id"] for r in delivered) == sorted(r["order_id"] for r in rows), route.name

"""The file-drop-to-db template: every environment loads, and dev runs the flow on its fixtures."""

import json
from pathlib import Path

from feather_connect.project import load_project, validate_project
from feather_connect.testing.harness import FlowHarness

ROOT = Path(__file__).resolve().parents[1]


def test_every_environment_loads_and_validates():
    for env in ("dev", "qa", "uat", "prod"):
        project = load_project(ROOT, env)
        assert [p for p in validate_project(project) if p.severity == "error"] == [], env


def test_dev_runs_on_stand_ins_of_the_real_connectors():
    (flow,) = load_project(ROOT, "dev").flows
    assert (
        getattr(flow.datasets[0].source, "stands_in", False)
        or flow.datasets[0].source.spec.name == "simulation.source"
    )
    assert all(getattr(route.sink, "stands_in", False) for route in flow.routes)


def test_the_flow_delivers_the_fixtures():
    (flow,) = load_project(ROOT, "dev").flows
    rows = json.loads((ROOT / "fixtures" / "invoices.json").read_text(encoding="utf-8"))
    harness = FlowHarness(flow)
    harness.source("invoices").load(rows)
    harness.run()
    for route in flow.routes:
        delivered = harness.sink(route.name).rows
        assert sorted(r["invoice_id"] for r in delivered) == sorted(r["invoice_id"] for r in rows), route.name

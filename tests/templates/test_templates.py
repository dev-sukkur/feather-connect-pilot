"""The five generic templates: their files, every environment, their own tests, and the fan-out demo's three
destinations."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from feather_connect.project import load_project, validate_project
from feather_connect.testing.harness import FlowHarness

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates"
TEMPLATES = ["db-to-warehouse", "api-to-warehouse", "file-drop-to-db", "webhook-to-api", "fan-out-demo"]
ENVS = ("dev", "qa", "uat", "prod")
REQUIRED = ["feather.yaml", *(f"env/{env}.yaml" for env in ENVS), "flows", "fixtures", "tests", "README.md"]


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_has_every_required_file(name: str):
    root = TEMPLATES_DIR / name
    assert [rel for rel in REQUIRED if not (root / rel).exists()] == []
    assert list((root / "fixtures").glob("*.json")) and list((root / "tests").glob("test_*.py"))


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_validates_for_every_environment(name: str):
    for env in ENVS:
        project = load_project(TEMPLATES_DIR / name, env)
        assert project.flows
        assert [p for p in validate_project(project) if p.severity == "error"] == [], env


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_flow_calls_the_library(name: str):  # ruling 12
    code = "".join(p.read_text(encoding="utf-8") for p in (TEMPLATES_DIR / name / "flows").glob("*.py"))
    assert "from feather_connect.templates.generic import" in code


@pytest.mark.parametrize("name", TEMPLATES)
def test_template_own_tests_pass(name: str):
    tests = TEMPLATES_DIR / name / "tests"
    done = subprocess.run(
        [sys.executable, "-m", "pytest", str(tests), "-q", "-p", "no:cacheprovider"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stdout[-2000:] + done.stderr[-2000:]


def test_fan_out_demo_reaches_three_destinations():
    root = TEMPLATES_DIR / "fan-out-demo"
    (flow,) = load_project(root, "prod").flows
    assert [r.sink.spec.name for r in flow.routes] == ["postgres.table", "duckdb.table", "http.call"]
    rows = json.loads((root / "fixtures" / "orders.json").read_text(encoding="utf-8"))
    harness = FlowHarness(flow)
    harness.source("orders").load(rows)
    harness.run()
    for route in ("warehouse", "lake", "erp"):
        assert sorted(r["order_id"] for r in harness.sink(route).rows) == sorted(r["order_id"] for r in rows)

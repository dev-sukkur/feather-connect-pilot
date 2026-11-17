"""The flows of flows/returns.py: the project validates, and they run on simulation data."""

import importlib
import json
from pathlib import Path

import feather_connect as fc
from feather_connect.project import load_project, validate_project
from feather_connect.testing.harness import FlowHarness

ROOT = Path(__file__).resolve().parents[1]
ENVS = [p.stem for p in (ROOT / "env").glob("*.yaml")]


def _flows(env: str) -> list[fc.Flow]:
    project = load_project(ROOT, env)
    module = importlib.import_module("flows.returns")
    names = {v.name for v in vars(module).values() if isinstance(v, fc.Flow)}
    return [f for f in project.flows if f.name in names]


def _fixture(name: str) -> list[dict] | None:
    for suffix in (".json", ".jsonl"):
        path = ROOT / "fixtures" / f"{name}{suffix}"
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            return (
                json.loads(text)
                if suffix == ".json"
                else [json.loads(line) for line in text.splitlines() if line]
            )
    return None


def test_every_environment_validates():
    for env in ENVS:
        problems = [p for p in validate_project(load_project(ROOT, env)) if p.severity == "error"]
        assert problems == [], env


def test_the_flows_run_in_dev():
    flows = _flows("dev")
    assert flows
    for flow in flows:
        if any(d.strategy.name == "push" for d in flow.datasets):
            continue  # events arrive through the ingress: the template's own tests show how to test that
        harness = FlowHarness(flow)
        for dataset in flow.datasets:
            rows = _fixture(dataset.name)
            if rows is not None:
                harness.source(dataset.name).load(rows)
        harness.run()
        failed = harness.ledger.list_deliveries(status="failed")
        assert failed == [], [d.last_error for d in failed]

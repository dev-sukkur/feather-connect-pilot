"""What the hidden acceptance tests share: the project under test, its flows and a run on fixture rows.

The runner copies this file into the project's `tests/` as `benchmark_accept.py`, beside the task's
`test_benchmark_acceptance.py`.
"""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import feather_connect as fc
from feather_connect.core.context import Context
from feather_connect.core.flow import RouteSpec
from feather_connect.core.records import make_key, row_hash
from feather_connect.project import Project, load_project
from feather_connect.testing.harness import FlowHarness

ROOT = Path(__file__).resolve().parents[1]


def project(env: str = "dev") -> Project:
    return load_project(ROOT, env)


def flow_reading(dataset: str, env: str = "dev") -> fc.Flow:
    """The one flow of the project that reads `dataset`."""
    found = [f for f in project(env).flows if any(d.name == dataset for d in f.datasets)]
    assert len(found) == 1, f"one flow should read the dataset {dataset!r}; found {[f.name for f in found]}"
    return found[0]


def dataset_of(flow: fc.Flow, name: str) -> fc.Dataset:
    return next(d for d in flow.datasets if d.name == name)


def fixture(name: str) -> list[dict[str, Any]]:
    return json.loads((ROOT / "fixtures" / f"{name}.json").read_text(encoding="utf-8"))


def delivered(flow: fc.Flow, dataset: str, rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Run `flow` in memory with `rows` as the dataset's whole contents; what each route received, by its name."""
    harness = FlowHarness(flow)
    harness.source(dataset).load(rows)
    harness.run()
    return {route.name: list(harness.sink(route.name).rows) for route in flow.routes}


def mapped(route: RouteSpec, rows: list[dict[str, Any]], key: list[str]) -> list[dict[str, Any]]:
    """`rows` through the route's map, as the runtime would send them (the rows themselves without one)."""
    if route.map is None:
        return [dict(r) for r in rows]
    ctx = Context({}, logging.getLogger("benchmark"), datetime(2026, 11, 16, tzinfo=UTC), "UTC")
    records = [fc.Record(make_key(r, tuple(key)), "upsert", r, row_hash(r)) for r in rows]
    return [dict(route.map(record, ctx)) for record in records]  # pyright: ignore[reportCallIssue, reportArgumentType, reportReturnType]


def key_of(dataset: fc.Dataset) -> list[str]:
    """The dataset's key columns, as a list, whether it names one or several."""
    return [dataset.key] if isinstance(dataset.key, str) else list(dataset.key)


def trigger_kinds(flow: fc.Flow) -> list[dict[str, Any]]:
    return flow.spec_dict()["triggers"]

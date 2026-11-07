"""`feather describe`: connectors, flows and datasets."""

import json
from collections.abc import Callable
from pathlib import Path

from typer.testing import Result


def test_describe_connector_returns_schema_and_capabilities(cli: Callable[..., Result]) -> None:
    r = cli("describe", "connector", "postgres.table", "--json")
    assert r.exit_code == 0, r.output
    doc = json.loads(r.stdout)
    assert doc["kind"] == "sink" and "upsert" in doc["capabilities"]["modes"]
    assert doc["config_schema"]["properties"]
    assert doc["maturity"] in {"experimental", "beta", "stable"}


def test_describe_unknown_connector_names_the_known_ones(cli: Callable[..., Result]) -> None:
    r = cli("describe", "connector", "nope.sink", "--json")
    assert r.exit_code == 1 and "simulation.source" in json.loads(r.stdout)["error"]


def test_describe_connectors_lists_them_all(cli: Callable[..., Result]) -> None:
    names = {c["name"] for c in json.loads(cli("describe", "connectors", "--json").stdout)}
    assert {"simulation.source", "postgres.table", "http.webhook", "duckdb.table"} <= names


def test_describe_flow_and_dataset(cli: Callable[..., Result], project: Path) -> None:
    flow = json.loads(cli("describe", "flow", "tiny", "--json").stdout)
    assert flow["name"] == "tiny" and flow["datasets"][0]["name"] == "orders"
    dataset = json.loads(cli("describe", "dataset", "orders", "--json").stdout)
    assert dataset["flow"] == "tiny" and dataset["connector"] == "simulation.source"
    missing = cli("describe", "flow", "nope", "--json")
    assert missing.exit_code == 1 and "tiny" in json.loads(missing.stdout)["hint"]


def test_help_lists_every_command(cli: Callable[..., Result]) -> None:
    out = cli("--help").stdout
    for name in ("validate", "describe", "server", "worker", "dev"):
        assert name in out

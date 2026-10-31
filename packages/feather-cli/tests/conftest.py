"""Fixtures for the CLI tests: a runner that keeps stdout and stderr apart, and small projects on disk."""

from collections.abc import Callable
from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner, Result

from feather_cli.main import app

FEATHER_YAML = """
    name: tiny
    modules: [flows.tiny]
    connections:
      sim_source: {connector: simulation.source}
      sim_sink: {connector: simulation.sink}
"""

FLOW = """
    import logging

    import feather_connect as fc
    from feather_connect.connectors import simulation

    logging.getLogger("tiny").warning("importing the tiny flow")

    orders = fc.Dataset(
        "orders",
        source=simulation.Source("sim_source", generator={"id": simulation.seq()}, rows_per_partition=3),
        key="id",
        strategy=fc.strategy.STRATEGY,
        partition=fc.partition.PARTITION,
    )
    tiny = fc.Flow(
        "tiny",
        reads=orders,
        deliver=[fc.to(simulation.Sink("SINK"), mode="MODE")],
        triggers=[fc.manual()],
    )
"""


def write_project(
    root: Path,
    *,
    strategy: str = "replace()",
    partition: str = "none()",
    mode: str = "upsert",
    sink: str = "sim_sink",
    extra_yaml: str = "",
) -> Path:
    flow = (
        dedent(FLOW)
        .replace("STRATEGY", strategy)
        .replace("PARTITION", partition)
        .replace("SINK", sink)
        .replace("MODE", mode)
    )
    files = {
        "feather.yaml": dedent(FEATHER_YAML) + dedent(extra_yaml),
        "flows/__init__.py": "",
        "flows/tiny.py": flow,
        "env/dev.yaml": "",
    }
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return root


@pytest.fixture
def make_project() -> Callable[..., Path]:
    return write_project


@pytest.fixture
def cli() -> Callable[..., Result]:
    runner = CliRunner()

    def run(*args: str, input: str | None = None) -> Result:
        return runner.invoke(app, list(args), input=input)

    return run


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A sound project, which is also the working directory."""
    monkeypatch.chdir(write_project(tmp_path))
    return tmp_path


@pytest.fixture
def bad_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A flow that replaces partitions from a source whose versions are not complete."""
    write_project(tmp_path, strategy="by_stamp('id')", mode="replace_partition")
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def warn_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A sound flow, and a budget that nothing uses."""
    write_project(tmp_path, extra_yaml="budgets:\n  idle: {limits: ['10/minute']}\n")
    monkeypatch.chdir(tmp_path)
    return tmp_path

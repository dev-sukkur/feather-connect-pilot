"""`feather new`: projects, flows from templates, and connectors, written whole or not at all."""

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import Result

from feather_cli.commands.new import available_templates
from feather_connect.project import load_project, validate_project

TEMPLATES = ["api-to-warehouse", "db-to-warehouse", "fan-out-demo", "file-drop-to-db", "webhook-to-api"]
LAYOUT = [
    "feather.yaml",
    "env/dev.yaml",
    "env/qa.yaml",
    "env/uat.yaml",
    "env/prod.yaml",
    "flows/__init__.py",
    "connectors/__init__.py",
    "fixtures/.gitkeep",
    "tests/test_hello.py",
    "AGENTS.md",
    "pyproject.toml",
]


def _tree(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def _pytest(project: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", "tests", "-q", "-p", "no:cacheprovider"],
        cwd=project,
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def made(cli: Callable[..., Result], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A new project, `shop`, which is the working directory."""
    r = cli("new", "project", "shop", "--dir", str(tmp_path), "--json")
    assert r.exit_code == 0, r.output
    monkeypatch.chdir(tmp_path / "shop")
    return tmp_path / "shop"


def test_new_project_has_the_design_layout(made: Path) -> None:
    files = _tree(made)
    assert [rel for rel in LAYOUT if rel not in {f.replace("\\", "/") for f in files}] == []
    agents = (made / "AGENTS.md").read_text(encoding="utf-8")
    for rule in ("Transforms are pure", "Budgets attach to connections", "Never put data in task arguments"):
        assert rule in agents
    assert "Every flow needs a test" in agents and "feather run" in agents
    assert [p for p in validate_project(load_project(made, "dev")) if p.severity == "error"] == []
    done = _pytest(made)
    assert done.returncode == 0, done.stdout[-2000:]


def test_templates_are_found() -> None:
    assert available_templates() == TEMPLATES


@pytest.mark.parametrize("template", TEMPLATES)
def test_new_flow_from_each_template_validates_and_its_tests_pass(
    cli: Callable[..., Result], made: Path, template: str
) -> None:
    r = cli("new", "flow", "copied", "--template", template, "--json")
    assert r.exit_code == 0, r.output
    code = (made / "flows" / "copied.py").read_text(encoding="utf-8")
    assert "def " in code and "feather_connect.templates" not in code  # ruling 12: a copy, not a call
    for env in ("dev", "qa", "uat", "prod"):
        problems = [p for p in validate_project(load_project(made, env)) if p.severity == "error"]
        assert problems == [], (env, problems)
    done = _pytest(made)
    assert done.returncode == 0, done.stdout[-3000:] + done.stderr[-2000:]


def test_new_blank_flow_validates_and_its_test_passes(cli: Callable[..., Result], made: Path) -> None:
    assert cli("new", "flow", "second").exit_code == 0
    assert "flows.second" in (made / "feather.yaml").read_text(encoding="utf-8")
    done = _pytest(made)
    assert done.returncode == 0, done.stdout[-2000:]


def test_new_connector_passes_the_conformance_kit_stubs(cli: Callable[..., Result], made: Path) -> None:
    r = cli("new", "connector", "acme_erp", "--json")
    assert r.exit_code == 0, r.output
    assert (made / "connectors" / "acme_erp.py").is_file()
    done = _pytest(made)
    assert done.returncode == 0, done.stdout[-3000:]
    assert "passed" in done.stdout


def test_existing_name_is_refused_and_nothing_changes(cli: Callable[..., Result], made: Path) -> None:
    before = _tree(made)
    r = cli("new", "flow", "hello", "--json")
    assert r.exit_code == 1 and "exists already" in json.loads(r.stdout)["error"]
    assert _tree(made) == before
    r = cli("new", "project", "shop", "--dir", str(made.parent), "--json")
    assert r.exit_code == 1 and _tree(made) == before


@pytest.mark.parametrize("name", ["Bad", "1st", "with space", "class", "a/b", "x" * 70])
def test_invalid_name_is_refused_with_no_partial_files(
    cli: Callable[..., Result], made: Path, name: str
) -> None:
    before = _tree(made)
    r = cli("new", "flow", name, "--json")
    assert r.exit_code == 1 and "not a valid name" in json.loads(r.stdout)["error"]
    assert _tree(made) == before
    r = cli("new", "project", name, "--dir", str(made), "--json")
    assert r.exit_code == 1 and _tree(made) == before


def test_unknown_template_lists_the_available_ones(cli: Callable[..., Result], made: Path) -> None:
    before = _tree(made)
    r = cli("new", "flow", "x", "--template", "nope", "--json")
    doc = json.loads(r.stdout)
    assert r.exit_code == 1 and "fan-out-demo" in doc["hint"]
    assert _tree(made) == before

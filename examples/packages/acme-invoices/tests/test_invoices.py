"""The package's own test: the project loads and validates."""

from pathlib import Path

from feather_connect.project import load_project, validate_project

ROOT = Path(__file__).resolve().parents[1]


def test_the_project_loads_and_validates():
    project = load_project(ROOT, "dev")
    assert [p for p in validate_project(project) if p.severity == "error"] == []

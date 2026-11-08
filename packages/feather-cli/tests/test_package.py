"""`feather package build`, `publish` and `search`, and `feather registry` (flow packages §4 and §10)."""

import json
import shutil
from pathlib import Path

import pytest

SAMPLE = Path(__file__).resolve().parents[3] / "examples" / "packages" / "acme-invoices"


@pytest.fixture
def sample(tmp_path: Path) -> Path:
    root = tmp_path / "acme-invoices"
    shutil.copytree(SAMPLE, root, ignore=shutil.ignore_patterns("dist", "__pycache__"))
    return root


def test_build_writes_the_same_wheel_twice(cli, sample: Path, tmp_path: Path):
    first = cli("package", "build", "--project", str(sample), "--json")
    second = cli("package", "build", "--project", str(sample), "--out", str(tmp_path / "again"), "--json")
    assert first.exit_code == second.exit_code == 0, first.output
    one, two = json.loads(first.stdout), json.loads(second.stdout)
    assert one["sha256"] == two["sha256"] and one["flows"] == ["invoices"]
    assert Path(one["wheel"]) == sample / "dist" / "acme_invoices-1.0.0-py3-none-any.whl"


def test_build_explains_a_project_without_a_version(cli, sample: Path):
    path = sample / "feather.yaml"
    path.write_text(path.read_text(encoding="utf-8").replace("version: 1.0.0\n", ""), encoding="utf-8")
    r = cli("package", "build", "--project", str(sample))
    assert r.exit_code == 1 and "version: is required" in r.stderr

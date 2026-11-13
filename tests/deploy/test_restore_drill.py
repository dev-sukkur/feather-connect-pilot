"""The restore drill of deploy/restore_drill.py (M3 plan 1 P2, NFR-04): a point-in-time restore of Feather's
database, in containers of its own, back within RTO. Runs nightly and for the release checklist."""

import importlib.util
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = [
    pytest.mark.docker,
    pytest.mark.timeout(1200),
    pytest.mark.skipif(shutil.which("docker") is None, reason="needs Docker"),
]


def test_a_point_in_time_restore_brings_back_every_run_and_undoes_the_mistake_within_rto() -> None:
    spec = importlib.util.spec_from_file_location("restore_drill", ROOT / "deploy" / "restore_drill.py")
    assert spec and spec.loader
    drill = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(drill)
    report = drill.drill()
    print(report)
    assert report["ok"], report
    assert report["runs_before_backup"] == 50 and report["runs_after_backup"] == 50
    assert report["rto_s"] <= 3600

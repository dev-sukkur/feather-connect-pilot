"""`feather test`: the project's pytest suite, the connectors' conformance kits among it."""

import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Annotated, Any

import typer

from feather_cli.options import Json, ProjectDir
from feather_cli.output import emit, fail

Extra = Annotated[list[str] | None, typer.Argument(help="More arguments for pytest.", show_default=False)]


def _counts(report: Path) -> dict[str, int]:
    try:
        root = ET.parse(report).getroot()
    except (ET.ParseError, OSError):
        return {}
    suites = [root] if root.tag == "testsuite" else list(root)
    total = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for suite in suites:
        for key in total:
            total[key] += int(suite.get(key, 0))
    return total


def test(project: ProjectDir = Path("."), json_mode: Json = False, extra: Extra = None) -> None:
    """Run the project's tests with pytest. Exits with pytest's code: 0 when every test passes."""
    if not (project / "feather.yaml").is_file():
        fail(
            f"no feather.yaml in {project.resolve()}",
            hint="run it from the project directory, or name that directory with --project",
            json_mode=json_mode,
        )
    if not (project / "tests").is_dir():
        fail(
            "the project has no tests/ folder",
            hint="every flow needs a test: see AGENTS.md",
            json_mode=json_mode,
        )
    with tempfile.TemporaryDirectory() as tmp:
        report = Path(tmp) / "report.xml"
        args = [sys.executable, "-m", "pytest", "tests", "-p", "no:cacheprovider", f"--junitxml={report}"]
        args += ["-q"] if not json_mode else ["-q", "--no-header"]
        args += list(extra or [])
        done = subprocess.run(
            args, cwd=project, check=False, capture_output=json_mode, text=True, stdin=subprocess.DEVNULL
        )
        counts = _counts(report)
    if json_mode:
        failed = counts.get("failures", 0) + counts.get("errors", 0)
        doc: dict[str, Any] = {
            "ok": done.returncode == 0,
            "exit_code": done.returncode,
            "tests": counts.get("tests", 0),
            "passed": counts.get("tests", 0) - failed - counts.get("skipped", 0),
            "failed": failed,
            "skipped": counts.get("skipped", 0),
            "output": (done.stdout or "")[-4000:],
        }
        emit(doc, json_mode=True, human=lambda _: None)
    if done.returncode != 0:
        raise typer.Exit(done.returncode)

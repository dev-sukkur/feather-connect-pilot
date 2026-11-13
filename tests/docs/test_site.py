"""The documentation site (M3 plan 1 L1, NFR-14): it builds with no warning, and its examples run.

- The ```python blocks of a page run as one module, as the connectors' and templates' READMEs do in their own tests.
- A ```bash block after a `<!-- test -->` line runs command by command, each `uv run feather ...` as the CLI here.
"""

import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from feather_connect.testing.readme import run_readme_examples

ROOT = Path(__file__).resolve().parents[2]
SITE = ROOT / "site-docs"
_TESTED = re.compile(r"<!-- test -->\s*```bash\n(.*?)```", re.DOTALL)


def test_the_site_builds_with_no_warning(tmp_path: Path) -> None:
    built = subprocess.run(
        [sys.executable, str(SITE / "build.py")], cwd=ROOT, capture_output=True, text=True, check=False
    )
    assert built.returncode == 0, built.stderr[-2000:]
    for page in ("connectors.md", "templates.md", "cli.md", "api.md", "openapi.json"):
        assert (SITE / "reference" / page).exists(), page
    site = subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--strict", "--site-dir", str(tmp_path / "site")],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    assert site.returncode == 0, site.stderr[-3000:]
    assert (tmp_path / "site" / "operating-guide" / "index.html").exists()
    assert (tmp_path / "site" / "runbooks" / "restore-drill" / "index.html").exists()


@pytest.mark.parametrize(
    "page", sorted(p.name for p in SITE.glob("*.md") if "```python" in p.read_text("utf-8"))
)
def test_a_pages_python_example_runs(page: str, tmp_path: Path) -> None:
    run_readme_examples(SITE / page, tmp_path)


@pytest.mark.timeout(600)
@pytest.mark.parametrize(
    "page", sorted(p.name for p in SITE.glob("*.md") if "<!-- test -->" in p.read_text("utf-8"))
)
def test_a_pages_tested_commands_run(page: str) -> None:
    for block in _TESTED.findall((SITE / page).read_text(encoding="utf-8")):
        for line in block.strip().splitlines():
            words = shlex.split(line, comments=True)
            if words[:2] == ["uv", "sync"]:
                continue  # the tests run in the synced environment already
            assert words[:3] == ["uv", "run", "feather"], f"{page}: only feather commands are tested: {line}"
            done = subprocess.run(
                [sys.executable, "-m", "feather_cli.main", *words[3:]],
                cwd=ROOT,
                capture_output=True,
                check=False,
                text=True,
                timeout=300,
            )
            assert done.returncode == 0, f"{page}: {line}\n{done.stdout[-1500:]}\n{done.stderr[-1500:]}"

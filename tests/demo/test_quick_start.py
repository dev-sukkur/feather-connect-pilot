"""The quick start in the root README: its commands run in order, as written.

`uv sync` is what made the environment this runs in, and `feather dev` runs until stopped (tests/dev runs it), so of
those two only that the command exists is checked here.
"""

import re
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def quick_start() -> list[list[str]]:
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    section = text[text.index("## Quick start") :]
    block = re.search(r"```sh\n(.*?)```", section, re.DOTALL)
    assert block, "the quick start has no sh block"
    return [shlex.split(line) for line in block.group(1).splitlines() if line.strip()]


def test_quick_start_in_the_readme_works():
    commands = quick_start()
    assert commands[0] == ["uv", "sync"] and commands[-1] == ["uv", "run", "feather", "dev"]
    for command in commands[1:-1]:
        assert command[:2] == ["uv", "run"] and command[2] == "feather", command
        done = subprocess.run(
            [sys.executable, "-m", "feather_cli.main", *command[3:]],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert done.returncode == 0, f"{shlex.join(command)}:\n{done.stdout}\n{done.stderr}"
    shown = subprocess.run(
        [sys.executable, "-m", "feather_cli.main", "dev", "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert shown.returncode == 0 and "demo project" in shown.stdout

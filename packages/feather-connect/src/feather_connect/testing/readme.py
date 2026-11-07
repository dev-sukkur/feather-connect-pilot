"""Run the Python examples of a README, so that every connector and template has one that works (NFR-14).

The ```python blocks of the file run in order as one module, from a file in `workdir`, and the names they define are
returned for the test to look at.
"""

import re
import runpy
from pathlib import Path
from typing import Any

__all__ = ["python_blocks", "run_readme_examples"]

_BLOCK = re.compile(r"```python\n(.*?)```", re.DOTALL)


def python_blocks(readme: Path) -> list[str]:
    """The ```python blocks of a Markdown file, in order."""
    return _BLOCK.findall(readme.read_text(encoding="utf-8"))


def run_readme_examples(readme: Path, workdir: Path) -> dict[str, Any]:
    """Run the README's Python blocks as one module and return its names. Raises if it has none."""
    blocks = python_blocks(readme)
    if not blocks:
        raise AssertionError(f"{readme} has no ```python example")
    example = workdir / f"{readme.parent.name}_readme_example.py"
    example.write_text("\n".join(blocks), encoding="utf-8")
    return runpy.run_path(str(example))

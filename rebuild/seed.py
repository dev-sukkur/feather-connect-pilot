"""Seed a new repository with the specification, staged as OpenSpec changes.

    python rebuild/seed.py ../feather-connect

`openspec/specs/` in this repository describes the pilot as it is. A new repository has built nothing yet, so its
`openspec/specs/` starts empty, and the same requirements go in as eight ordered changes, one per build stage: each
has a proposal, a design, a task list and the stage's requirements as `## ADDED Requirements`. As a stage is built,
`openspec archive <change>` moves its requirements into the new repository's `specs/`.

A stage is one file under `rebuild/stages/`, with its three documents after `=== proposal.md ===`,
`=== design.md ===` and `=== tasks.md ===`. The capabilities a stage brings are the ones its proposal lists under
"New Capabilities". Every capability must belong to exactly one stage.

It needs only the standard library, and writes nothing in this repository.
"""

import argparse
import re
import shutil
import sys
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPECS = HERE.parent / "openspec" / "specs"
STAGES = HERE / "stages"
DOCUMENTS = ("proposal.md", "design.md", "tasks.md")
_MARKER = re.compile(r"^=== (\S+) ===$", re.MULTILINE)
_CAPABILITY = re.compile(r"^- `([a-z0-9-]+)`:", re.MULTILINE)


def split_stage(text: str) -> dict[str, str]:
    """The documents of a stage file, by name."""
    parts = _MARKER.split(text)
    found = {name: body.strip() + "\n" for name, body in zip(parts[1::2], parts[2::2], strict=True)}
    if set(found) != set(DOCUMENTS):
        raise ValueError(f"a stage needs exactly {', '.join(DOCUMENTS)}; found {', '.join(found) or 'none'}")
    return found


def capabilities_of(proposal: str) -> list[str]:
    """The capabilities a proposal lists under "New Capabilities"."""
    section = proposal.split("### New Capabilities", 1)[1].split("### Modified Capabilities", 1)[0]
    return _CAPABILITY.findall(section)


def delta_of(spec: str) -> str:
    """A capability's spec as the delta that adds it: its purpose, then every requirement as ADDED."""
    purpose = spec.split("## Purpose", 1)[1].split("## Requirements", 1)[0].strip()
    requirements = spec.split("## Requirements", 1)[1].strip()
    return f"# Spec Delta\n\n## Purpose\n\n{purpose}\n\n## ADDED Requirements\n\n{requirements}\n"


def seed(target: Path) -> list[str]:
    """Write the seed into `target` and return the names of the changes, in order."""
    out = target / "openspec"
    if out.exists():
        raise FileExistsError(f"{out} exists already: seed an empty repository")
    stages = sorted(STAGES.glob("*.md"))
    known = {path.parent.name for path in SPECS.glob("*/spec.md")}
    placed: dict[str, str] = {}
    plan: list[tuple[str, dict[str, str], list[str]]] = []
    for stage in stages:
        documents = split_stage(stage.read_text(encoding="utf-8"))
        capabilities = capabilities_of(documents["proposal.md"])
        for capability in capabilities:
            if capability not in known:
                raise ValueError(f"{stage.name} names the capability {capability!r}, which has no spec")
            if capability in placed:
                raise ValueError(f"{capability!r} is in both {placed[capability]} and {stage.name}")
            placed[capability] = stage.name
        plan.append((stage.stem, documents, capabilities))
    if missing := sorted(known - placed.keys()):
        raise ValueError(f"no stage brings: {', '.join(missing)}")

    (out / "specs").mkdir(parents=True)
    (out / "specs" / ".gitkeep").touch()
    (out / "changes" / "archive").mkdir(parents=True)
    (out / "changes" / "archive" / ".gitkeep").touch()
    shutil.copyfile(SPECS.parent / "config.yaml", out / "config.yaml")
    shutil.copyfile(SPECS.parent / "traceability.md", out / "traceability.md")
    for name, documents, capabilities in plan:
        change = out / "changes" / name
        change.mkdir()
        (change / ".openspec.yaml").write_text(
            f"schema: spec-driven\ncreated: {date.today().isoformat()}\n", encoding="utf-8", newline="\n"
        )
        for document, body in documents.items():
            (change / document).write_text(body, encoding="utf-8", newline="\n")
        for capability in capabilities:
            spec = (SPECS / capability / "spec.md").read_text(encoding="utf-8")
            where = change / "specs" / capability
            where.mkdir(parents=True)
            (where / "spec.md").write_text(delta_of(spec), encoding="utf-8", newline="\n")
    return [name for name, _, _ in plan]


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("target", type=Path, help="the new repository's folder")
    args = parser.parse_args()
    try:
        changes = seed(args.target)
    except (FileExistsError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"seeded {args.target / 'openspec'} with {len(changes)} changes:")
    for name in changes:
        print(f"  {name}")
    print("next: cd there and run `openspec validate --all --strict`")
    return 0


if __name__ == "__main__":
    sys.exit(main())

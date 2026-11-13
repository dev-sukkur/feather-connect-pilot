"""Write the documentation site's generated pages (M3 plan 1 L1), before `mkdocs build`:

- reference/connectors/ and reference/templates/: each connector's and template's README, whose Python examples the
  tests run (NFR-14), with its links pointed at the repository;
- reference/cli.md: every command of `feather`, from the CLI itself;
- reference/api.md and reference/openapi.json: every route of the API, from the server's OpenAPI document;
- operating-guide/: the Operating Guide, as it is.

    uv run python site-docs/build.py && uv run mkdocs build --strict

What it writes is not kept in git (site-docs/.gitignore): the code is its source.
"""

import json
import re
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site-docs"
REF = SITE / "reference"
REPO = "https://github.com/dev-sukkur/feather-connect-pilot/blob/main"
_LINK = re.compile(r"\]\((?!https?://|#|mailto:)([^)\s]+)\)")


def _repo_links(text: str, source: Path) -> str:
    """Point the README's relative links at the repository, since only the README is on the site."""

    def to_repo(match: re.Match[str]) -> str:
        target, _, anchor = match.group(1).partition("#")
        path = (source.parent / target).resolve().relative_to(ROOT).as_posix()
        return f"]({REPO}/{path}{'#' + anchor if anchor else ''})"

    return _LINK.sub(to_repo, text)


def readmes(kind: str, folders: list[Path]) -> list[tuple[str, str]]:
    """Copy each README into reference/<kind>/ and return (name, first line of what it is)."""
    out = REF / kind
    out.mkdir(parents=True, exist_ok=True)
    found = []
    for folder in sorted(folders):
        readme = folder / "README.md"
        if not readme.exists():
            continue
        text = readme.read_text(encoding="utf-8")
        name = folder.name
        (out / f"{name}.md").write_text(_repo_links(text, readme), encoding="utf-8")
        summary = next(
            (line for line in text.splitlines()[1:] if line.strip() and not line.startswith("#")), ""
        )
        found.append((name, summary.strip()))
    return found


def index(kind: str, title: str, intro: str, found: list[tuple[str, str]]) -> None:
    lines = [f"# {title}", "", intro, "", "| | |", "|---|---|"]
    lines += [f"| [{name}]({kind}/{name}.md) | {summary} |" for name, summary in found]
    (REF / f"{kind}.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def cli() -> None:
    """Every command of `feather`, with its arguments and options."""
    import click
    import typer

    from feather_cli.main import app

    root = typer.main.get_command(app)
    lines = [
        "# CLI reference",
        "",
        "Every command of `feather`, as `feather <command> --help` shows it. Written from the CLI itself.",
        "",
    ]

    def visit(command: Any, path: list[str]) -> None:  # typer may hold a click of its own
        if isinstance(command, click.Group):
            if path:
                lines.extend([f"## `{' '.join(path)}`", "", (command.help or "").strip(), ""])
            for name in sorted(command.commands):
                visit(command.commands[name], [*path, name])
            return
        lines.extend(
            [f"{'###' if len(path) > 2 else '##'} `{' '.join(path)}`", "", (command.help or "").strip(), ""]
        )
        arguments = [p for p in command.params if isinstance(p, click.Argument)]
        options = [p for p in command.params if isinstance(p, click.Option) and p.name != "help"]
        if arguments:
            lines.append("Arguments: " + ", ".join(f"`{(p.name or '').upper()}`" for p in arguments))
            lines.append("")
        if options:
            lines.extend(["| Option | |", "|---|---|"])
            for option in options:
                names = ", ".join(f"`{o}`" for o in [*option.opts, *option.secondary_opts])
                text = (getattr(option, "help", None) or "").replace("|", "\\|").replace("\n", " ")
                lines.append(f"| {names} | {text} |")
            lines.append("")

    visit(root, ["feather"])
    (REF / "cli.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def api() -> None:
    """The API's routes, from the OpenAPI document of a server on a throwaway project."""
    from feather_connect.clock import FrozenClock
    from feather_server.app import create_app
    from feather_server.testing import build_world

    with tempfile.TemporaryDirectory() as tmp:
        world = build_world(Path(tmp), FrozenClock(datetime(2026, 1, 1, tzinfo=UTC)))
        spec: dict[str, Any] = create_app(state=world.state, loops=lambda s: []).openapi()
    (REF / "openapi.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")
    lines = [
        "# API reference",
        "",
        "The JSON API at `/api/v1`. Every call takes a bearer token (`feather server create-token`); a change takes a",
        "`reason`, and in a protected environment needs one. Errors are RFC 9457 problems. The whole contract is",
        "[openapi.json](openapi.json), from the server itself.",
        "",
        "| Method | Path | What it does |",
        "|---|---|---|",
    ]
    for path, operations in sorted(spec["paths"].items()):
        if not path.startswith("/api/"):
            continue
        for method, operation in operations.items():
            text = (operation.get("description") or operation.get("summary") or "").split("\n\n")[0]
            text = " ".join(text.split()).replace("|", "\\|")
            lines.append(f"| {method.upper()} | `{path}` | {text} |")
    (REF / "api.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def operating_guide() -> None:
    target = SITE / "operating-guide"
    shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(ROOT / "docs" / "04-plan" / "operating-guide", target)


def main() -> int:
    shutil.rmtree(REF, ignore_errors=True)
    REF.mkdir(parents=True)
    connectors = readmes("connectors", list((ROOT / "packages" / "connectors").iterdir()))
    index(
        "connectors",
        "Connectors",
        "Each connector is a package of its own (`pip install feather-<name>`). Its page says what it reads or "
        "writes, its settings and secrets, and has an example the tests run.",
        connectors,
    )
    templates = readmes("templates", list((ROOT / "templates").iterdir()))
    index(
        "templates",
        "Templates",
        "Built-in flows, as library functions (`from feather_connect.templates.generic import ...`); "
        "`feather new flow --template <name>` copies one into your project for full control.",
        templates,
    )
    cli()
    api()
    operating_guide()
    print(f"wrote {len(connectors)} connectors, {len(templates)} templates, the CLI, the API and the guide")
    return 0


if __name__ == "__main__":
    sys.exit(main())

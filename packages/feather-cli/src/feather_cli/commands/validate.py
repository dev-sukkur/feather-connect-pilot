"""`feather validate`: static checks on a project, without running anything."""

from dataclasses import asdict
from pathlib import Path
from typing import Any

import typer

from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit, fail
from feather_connect.core.flow import Problem
from feather_connect.project import ProjectError, load_project, validate_project


def validate(
    project: ProjectDir = Path("."),
    env: Env = "dev",
    json_mode: Json = False,
) -> None:
    """Check the project's flows, connections and budgets. Exits 1 if there is any error; warnings do not fail."""
    if not (project / "feather.yaml").is_file():
        fail(
            f"no feather.yaml in {project.resolve()}",
            hint="run it from the project directory, or name that directory with --project",
            json_mode=json_mode,
        )
    try:
        problems = validate_project(load_project(project, env))
    except ProjectError as error:
        problems = [
            Problem(
                "error",
                "project_invalid",
                str(error),
                "project",
                "fix the file and key the message names, then run validate again",
            )
        ]
    ok = not any(p.severity == "error" for p in problems)
    document = {"ok": ok, "env": env, "problems": [asdict(p) for p in problems]}
    emit(document, json_mode=json_mode, human=_print)
    if not ok:
        raise typer.Exit(1)


def _print(document: dict[str, Any]) -> None:
    problems = document["problems"]
    for p in problems:
        typer.echo(f"{p['severity']}: {p['path']}: {p['code']}: {p['message']}")
        typer.echo(f"  hint: {p['hint']}")
    errors = sum(p["severity"] == "error" for p in problems)
    warnings = len(problems) - errors
    if problems:
        typer.echo(f"{errors} error(s), {warnings} warning(s)")
    else:
        typer.echo("no problems")

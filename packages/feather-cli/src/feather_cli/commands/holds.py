"""`feather holds`: deliveries held for an approver, or by the schema policy, and the decision on them (M2 plan 2
stage A1). Each calls the server's API and is audited; a rejection needs a reason."""

from pathlib import Path
from typing import Annotated, Any

import typer

from feather_cli.commands import remote
from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit

holds_app = typer.Typer(help="Held deliveries: list, approve, reject.", no_args_is_help=True)

Route = Annotated[str, typer.Argument(help="The route, with its package: acme/sap.")]
Version = Annotated[str, typer.Argument(help="The version the delivery carries.")]
Reason = Annotated[str | None, typer.Option(help="Why, for the audit log.", show_default=False)]


def _listed(found: dict[str, Any]) -> None:
    rows = found.get("holds") or []
    if not rows:
        typer.echo("no held deliveries")
    for row in rows:
        typer.echo(
            f"{row['route']} {row['version_id']} [{row['partition']}]: {row.get('reason') or row['kind']}"
        )


@holds_app.command("list")
def list_(
    route: Annotated[str | None, typer.Option(help="Only this route.")] = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """The held deliveries, oldest first, with why each is held."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(lambda: api.holds(route), json_mode)
    emit(found, json_mode=json_mode, human=_listed)


def _decide(route: str, version: str, reason: str | None, approve: bool, where: tuple[Any, ...]) -> None:
    project, env, server, token, json_mode = where
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.decide_hold(route, version, approve, reason), json_mode)
    word = "approved: it applies now" if approve else "rejected: it is skipped"
    emit(done, json_mode=json_mode, human=lambda d: typer.echo(f"{d['route']} {d['version_id']}: {word}"))


@holds_app.command()
def approve(
    route: Route,
    version: Version,
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Approve a held delivery: it applies as it is."""
    _decide(route, version, reason, True, (project, env, server, token, json_mode))


@holds_app.command()
def reject(
    route: Route,
    version: Version,
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Reject a held delivery: it is skipped, and the versions behind it go ahead. A reason is required."""
    _decide(route, version, reason, False, (project, env, server, token, json_mode))

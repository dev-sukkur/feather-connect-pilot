"""`feather deliveries`: hands-on actions on one delivery. `compensate` reverses an applied delivery of a side-effect
route (M2 plan 2 stage A2), as a task, audited, with a reason."""

from pathlib import Path
from typing import Annotated

import typer

from feather_cli.commands import remote
from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit

deliveries_app = typer.Typer(help="One delivery: compensate it.", no_args_is_help=True)


@deliveries_app.command()
def compensate(
    route: Annotated[str, typer.Argument(help="The route, with its package: acme/sap.")],
    version: Annotated[str, typer.Argument(help="The version the delivery carries.")],
    reason: Annotated[
        str | None, typer.Option(help="Why, for the audit log. Required.", show_default=False)
    ] = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Reverse an applied delivery of a side-effect route: its sink reverses each chunk, then it is compensated."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.compensate(route, version, reason), json_mode)
    emit(
        done, json_mode=json_mode, human=lambda d: typer.echo(f"{d['route']} {d['version_id']}: compensating")
    )

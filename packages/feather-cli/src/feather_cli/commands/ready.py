"""`feather ready`: how the flows that wait for data stand, and the two hands-on actions (readiness design §8). Each
calls the server's API; firing and closing need an operator, and a reason."""

from pathlib import Path
from typing import Annotated, Any

import typer

from feather_cli.commands import remote
from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit

ready_app = typer.Typer(
    help="Flows that wait for data: status, close a partition, fire a period now.", no_args_is_help=True
)

Reason = Annotated[str | None, typer.Option(help="Why, for the audit log. Required.", show_default=False)]


def _said(found: dict[str, Any]) -> None:
    for sets in found.get("sets") or []:
        by = f" by {sets['by']}" if sets.get("by") else ""
        typer.echo(f"{sets['flow']}: {', '.join(sets['datasets'])}{by}, each {sets['each']}")
        for row in sets.get("periods") or []:
            where = f"{row['period']} {row['scope']}".strip()
            if row["state"] == "waiting":
                typer.echo(f"  {where}: waiting for {row.get('waiting_for') or 'its data'}")
            elif row["state"] in ("fired", "late"):
                typer.echo(f"  {where}: {row['state']} (run {row.get('run_id')}, {row.get('reason')})")
            else:
                typer.echo(f"  {where}: {row['state']}")


@ready_app.command()
def status(
    flow: Annotated[
        str | None, typer.Argument(help="One flow, with its package; all of them when left out.")
    ] = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Each readiness set with its latest periods and scopes: fired, late, ready or waiting, and for what."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(lambda: api.readiness(flow), json_mode)
    emit(found, json_mode=json_mode, human=_said)


@ready_app.command()
def close(
    dataset: Annotated[str, typer.Argument(help="The dataset, with its package: acme-invoices/invoices.")],
    partition: Annotated[str, typer.Option(help="The partition: store_id=1509/date=2026-11-08.")],
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Say a partition is complete: what waits for it may fire once its last version is sealed. Audited."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.close_partition(dataset, partition, reason), json_mode)
    emit(
        done,
        json_mode=json_mode,
        human=lambda d: typer.echo(f"{d['dataset']} {d['partition']}: closed by {d['closed_by']}"),
    )


@ready_app.command()
def fire(
    flow: Annotated[str, typer.Argument(help="The waiting flow, with its package.")],
    period: Annotated[
        str, typer.Option(help="The period: 2026-11-08 for a day, 2026-11-08T13:00 for an hour.")
    ] = "",
    scope: Annotated[str, typer.Option(help="The scope of a set split by a field: store_id=1509.")] = "",
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Fire a period now, ready or not. Audited; the period stays fired, and late data doesn't rerun it."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.fire_ready(flow, period, scope, reason), json_mode)
    emit(done, json_mode=json_mode, human=lambda d: typer.echo(f"queued run {d['run_id']}"))

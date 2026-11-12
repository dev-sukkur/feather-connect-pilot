"""`feather registry`: the Python package indexes an environment trusts besides its built-in registry (flow packages
§4.3). Each calls the server's API; adding, removing and testing need an admin. A registry's token is a secret
reference such as `${secret:env:ACME_REGISTRY_TOKEN}`, resolved on the server when the registry is called."""

from pathlib import Path
from typing import Annotated, Any

import typer

from feather_cli.commands import remote
from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit

Reason = Annotated[str | None, typer.Option(help="Why, for the audit log.", show_default=False)]

registry_app = typer.Typer(help="Add, list, test and remove package registries.", no_args_is_help=True)


@registry_app.command("add")
def add(
    name: Annotated[str, typer.Argument(help="What to call it, such as acme.")],
    index_url: Annotated[
        str, typer.Argument(help="Its simple index, such as https://pkgs.example.com/simple/.")
    ],
    registry_token: Annotated[
        str | None,
        typer.Option(
            "--token",
            help="The index's token, as a secret reference: '${secret:env:ACME_REGISTRY_TOKEN}'.",
            show_default=False,
        ),
    ] = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    api_token: Annotated[
        str | None,
        typer.Option(
            "--api-token", envvar="FEATHER_TOKEN", help="The Feather server's API token.", show_default=False
        ),
    ] = None,
    reason: Reason = None,
    json_mode: Json = False,
) -> None:
    """Trust another registry. Its token is a secret reference, never the token itself: --token names the index's
    token, as the other commands' --token names the server's, which this one takes as --api-token."""
    with remote.client(project, env, server, api_token, json_mode) as api:
        added = remote.call(lambda: api.add_registry(name, index_url, registry_token, reason), json_mode)
    emit(
        added,
        json_mode=json_mode,
        human=lambda d: typer.echo(f"added registry {d['name']}: {d['index_url']}"),
    )


@registry_app.command("list")
def list_registries(
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """The registries the environment trusts, the built-in one first."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(api.registries, json_mode)

    def human(doc: dict[str, Any]) -> None:
        for r in doc["items"]:
            kind = "built in" if r["built_in"] else f"token {r['token_ref'] or 'none'}"
            typer.echo(f"{r['name']}: {r['index_url']} ({kind})")

    emit({"items": found}, json_mode=json_mode, human=human)


@registry_app.command("remove")
def remove(
    name: str,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    reason: Reason = None,
    json_mode: Json = False,
) -> None:
    """Stop trusting a registry. Packages installed from it stay installed."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.remove_registry(name, reason), json_mode)
    emit(done, json_mode=json_mode, human=lambda d: typer.echo(f"removed registry {d['registry']}"))


@registry_app.command("test")
def test(
    name: str,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Read the registry's index with its token, from the server. Exits 1 when that fails."""
    with remote.client(project, env, server, token, json_mode) as api:
        result = remote.call(lambda: api.test_registry(name), json_mode)

    def human(doc: dict[str, Any]) -> None:
        if doc["ok"]:
            typer.echo(f"{doc['registry']}: ok, {doc['projects']} project(s)")
        else:
            typer.echo(f"{doc['registry']}: failed: {doc['message']}", err=True)

    emit(result, json_mode=json_mode, human=human)
    if not result["ok"]:
        raise typer.Exit(1)

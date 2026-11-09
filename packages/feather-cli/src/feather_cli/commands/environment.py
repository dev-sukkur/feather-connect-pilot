"""`feather connection`, `feather config`, `feather secret` and `feather flow`: the environment's own configuration and
secrets, and activating a package's flows (flow packages §6 and §8). Each calls the server's API; changing anything
needs an admin, and activating an operator.

A connection's settings are plain values, shown and changed by name (`feather connection set zakya
organization_id=60001`); a secret field is a reference under `auth`,
named for its connection and field (`auth.client_secret='${secret:db:zakya/client_secret}'`), whose value is set
with `feather secret set zakya/client_secret` and never shown again.
"""

import sys
from pathlib import Path
from typing import Annotated, Any

import typer
import yaml

from feather_cli.commands import remote
from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit, fail

connection_app = typer.Typer(
    help="The environment's own connections: list, show, set and import.", no_args_is_help=True
)
config_app = typer.Typer(help="The environment's own configuration.", no_args_is_help=True)
secret_app = typer.Typer(help="The environment's encrypted secrets: write-only.", no_args_is_help=True)
flow_app = typer.Typer(help="Activate and deactivate a package's flows.", no_args_is_help=True)

Reason = Annotated[str | None, typer.Option(help="Why, for the audit log.", show_default=False)]
_PLURAL = {"connections": "connection", "budgets": "budget", "settings": "setting"}


def _entries(path: Path, json_mode: bool, only: str | None = None) -> list[dict[str, Any]]:
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as error:
        fail(f"{path}: can't read it ({error})", json_mode=json_mode)
    if not isinstance(loaded, dict):
        fail(f"{path}: must be a mapping", json_mode=json_mode)
    found: list[dict[str, Any]] = []
    for key, kind in _PLURAL.items():
        section = loaded.get(key) or {}  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        if only is None or kind == only:
            found += [{"kind": kind, "name": str(n), "value": v} for n, v in dict(section).items()]  # pyright: ignore[reportUnknownArgumentType]
    return found


def _show(doc: dict[str, Any]) -> None:
    typer.echo(f"{doc['name']} ({doc['connector']}), changed {doc['changed_at']} by {doc['changed_by']}")
    for key, value in doc["settings"].items():
        typer.echo(f"  {key}: {value}")
    for key, secret in doc["secrets"].items():
        state = (
            f"set, updated {secret['updated_at']} by {secret['updated_by']}"
            if secret.get("set")
            else ("not set" if "set" in secret else secret["ref"])
        )
        typer.echo(f"  {key} (secret): {state}")


@connection_app.command("list")
def list_connections(
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """The environment's own connections, with their settings and whether each secret is set."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(api.connections, json_mode)

    def human(doc: dict[str, Any]) -> None:
        if not doc["environment"]:
            typer.echo(f"environment {doc['env']} has no connections of its own")
        for item in doc["environment"]:
            _show(item)

    emit(found, json_mode=json_mode, human=human)


@connection_app.command()
def show(
    name: Annotated[str, typer.Argument(help="The connection.")],
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """One connection's settings, and its secret fields' state."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(lambda: api.connection(name), json_mode)
    emit(found, json_mode=json_mode, human=_show)


@connection_app.command("set")
def set_connection(
    name: Annotated[str, typer.Argument(help="The connection.")],
    settings: Annotated[
        list[str],
        typer.Argument(help="key=value; key= removes it; auth.<field>='${secret:db:NAME}' for a secret."),
    ],
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Change some settings of a connection, or make one (give connector=…). Every change is audited with its old and
    new value."""
    changes: dict[str, Any] = {}
    for item in settings:
        key, sep, raw = item.partition("=")
        if not sep or not key.strip():
            fail(f"{item!r} is not key=value", hint="such as organization_id=60001", json_mode=json_mode)
        value = None if raw == "" else (raw if raw.startswith("${") else yaml.safe_load(raw))
        changes[key.strip()] = value
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(lambda: api.change_connection(name, changes, reason), json_mode)
    emit(found, json_mode=json_mode, human=_show)


def _import(path: Path, only: str | None, reason: str | None, project: Path, env: str, server: str | None,
            token: str | None, json_mode: bool) -> None:  # fmt: skip
    entries = _entries(path, json_mode, only)
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(
            lambda: api.import_environment(entries, reason or f"imported from {path.name}"), json_mode
        )

    def human(doc: dict[str, Any]) -> None:
        for item in doc["items"]:
            typer.echo(f"set {item['kind']} {item['name']}")
        typer.echo(f"{len(doc['items'])} entries in environment {doc['env']}")

    emit(done, json_mode=json_mode, human=human)


@connection_app.command("import")
def import_connections(
    path: Annotated[Path, typer.Argument(help="An M1 environment file, such as env/prod.yaml.")],
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Seed the environment's connections from an M1 file: its secrets stay references."""
    _import(path, "connection", reason, project, env, server, token, json_mode)


@config_app.command("import")
def import_config(
    path: Annotated[Path, typer.Argument(help="An M1 environment file, such as env/prod.yaml.")],
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Seed the environment's connections, budgets and settings from an M1 file, so a project carries over."""
    _import(path, None, reason, project, env, server, token, json_mode)


@secret_app.command("set")
def set_secret(
    name: Annotated[str, typer.Argument(help="The secret's name, as ${secret:db:NAME} refers to it.")],
    value_file: Annotated[
        Path | None,
        typer.Option("--from-file", help="Read the value from this file, not stdin.", show_default=False),
    ] = None,
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Set or replace a secret. The value is read from stdin (or --from-file), sealed on the server, and never shown
    again."""
    raw = value_file.read_text(encoding="utf-8") if value_file is not None else sys.stdin.read()
    value = raw.rstrip("\r\n")
    if not value:
        fail("no value was given", hint=f"echo -n VALUE | feather secret set {name}", json_mode=json_mode)
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.set_secret(name, value, reason), json_mode)
    emit(
        done,
        json_mode=json_mode,
        human=lambda d: typer.echo(f"{'replaced' if d['replaced'] else 'set'} secret {name}"),
    )


@secret_app.command("list")
def list_secrets(
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """The secrets' names, when and by whom each was set. Never a value."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(api.secrets, json_mode)

    def human(doc: dict[str, Any]) -> None:
        for item in doc["items"]:
            typer.echo(f"{item['name']}: updated {item['updated_at']} by {item['updated_by']}")
        if not doc["items"]:
            typer.echo("no secret is set")

    emit({"items": found}, json_mode=json_mode, human=human)


@secret_app.command("delete")
def delete_secret(
    name: Annotated[str, typer.Argument(help="The secret.")],
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Delete a secret. A connection that refers to it fails until it is set again."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.delete_secret(name), json_mode)
    emit(done, json_mode=json_mode, human=lambda d: typer.echo(f"deleted secret {name}"))


@secret_app.command("new-key")
def new_key(json_mode: Json = False) -> None:
    """Print a new master key for FEATHER_MASTER_KEY. Keep it out of the database and out of version control."""
    from feather_connect.vault import new_master_key

    emit({"key": new_master_key()}, json_mode=json_mode, human=lambda d: typer.echo(d["key"]))


@secret_app.command("rotate-key")
def rotate_key(
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Seal every secret again under the server's current master key and make it the active one. First restart
    every server and worker with the new key as FEATHER_MASTER_KEY and the old one as FEATHER_MASTER_KEY_PREVIOUS;
    after this, drop the old key."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.rotate_key(reason), json_mode)
    emit(
        done,
        json_mode=json_mode,
        human=lambda d: typer.echo(
            f"sealed {d['secrets']} secrets with key {d['key_id']}, now the active one: "
            "drop FEATHER_MASTER_KEY_PREVIOUS once every process has restarted"
        ),
    )


@flow_app.command()
def activate(
    flow: Annotated[str, typer.Argument(help="The flow, with its package: acme-invoices/invoices.")],
    version: Annotated[str, typer.Option(help="The installed version to activate.")],
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Put a flow of an installed version in service: its triggers fire from the next tick, with no restart."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.activate_flow(flow, version, reason), json_mode)
    emit(done, json_mode=json_mode, human=lambda d: typer.echo(f"{d['flow']} {d['version']}: {d['state']}"))


@flow_app.command()
def deactivate(
    flow: Annotated[str, typer.Argument(help="The flow, with its package: acme-invoices/invoices.")],
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Take a flow out of service: its triggers stop; runs in progress finish."""
    with remote.client(project, env, server, token, json_mode) as api:
        done = remote.call(lambda: api.deactivate_flow(flow, reason), json_mode)
    emit(done, json_mode=json_mode, human=lambda d: typer.echo(f"{d['flow']} {d['version']}: {d['state']}"))

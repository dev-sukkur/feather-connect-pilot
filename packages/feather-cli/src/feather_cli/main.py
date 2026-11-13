"""The `feather` command. Each command lives in `commands/` and takes `--env` and `--json`."""

import os
import sys
from typing import Annotated

import typer

from feather_cli.commands import (
    deliveries,
    describe,
    dev,
    environment,
    holds,
    mcp,
    new,
    package,
    processes,
    ready,
    registry,
    remote,
    run,
    validate,
)
from feather_cli.commands import test as test_command
from feather_connect.logs import configure_logging

app = typer.Typer(
    name="feather",
    help="Feather Connect: flows written as code.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_show_locals=False,  # a local variable may hold a secret
)


def _version(asked: bool) -> None:
    if asked:
        from importlib.metadata import version

        typer.echo(f"feather {version('feather-cli')}")
        raise typer.Exit


@app.callback()
def main(
    _: Annotated[
        bool,
        typer.Option("--version", callback=_version, is_eager=True, help="Print the version and exit."),
    ] = False,
) -> None:
    # Logs go to stderr, so that stdout holds only what a command prints (one JSON document with --json).
    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"), stream=sys.stderr)


app.command()(validate.validate)
app.add_typer(describe.describe_app, name="describe")
app.add_typer(new.new_app, name="new")
app.add_typer(package.package_app, name="package")
app.add_typer(registry.registry_app, name="registry")
app.add_typer(environment.connection_app, name="connection")
app.add_typer(environment.config_app, name="config")
app.add_typer(environment.secret_app, name="secret")
app.add_typer(environment.flow_app, name="flow")
app.add_typer(ready.ready_app, name="ready")
app.add_typer(holds.holds_app, name="holds")
app.add_typer(deliveries.deliveries_app, name="deliveries")
app.command()(run.run)
app.command("test")(test_command.test)
for command in (
    remote.status,
    remote.runs,
    remote.logs,
    remote.trace,
    remote.diff,
    remote.backfill,
    remote.replay,
):
    app.command()(command)
app.add_typer(processes.server_app, name="server")
app.command()(processes.worker)
app.command()(mcp.mcp)
app.command()(dev.dev)

if __name__ == "__main__":
    app()

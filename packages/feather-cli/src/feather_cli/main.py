"""The `feather` command. Each command lives in `commands/` and takes `--env` and `--json`."""

import os
import sys

import typer

from feather_cli.commands import describe, dev, new, processes, run, validate
from feather_cli.commands import test as test_command
from feather_connect.logs import configure_logging

app = typer.Typer(
    name="feather",
    help="Feather Connect: flows written as code.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_show_locals=False,  # a local variable may hold a secret
)


@app.callback()
def main() -> None:
    # Logs go to stderr, so that stdout holds only what a command prints (one JSON document with --json).
    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"), stream=sys.stderr)


app.command()(validate.validate)
app.add_typer(describe.describe_app, name="describe")
app.add_typer(new.new_app, name="new")
app.command()(run.run)
app.command("test")(test_command.test)
app.add_typer(processes.server_app, name="server")
app.command()(processes.worker)
app.command()(dev.dev)

if __name__ == "__main__":
    app()

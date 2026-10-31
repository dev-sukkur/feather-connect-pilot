"""How every command prints: one JSON document on stdout with `--json`, text otherwise, and errors on stderr.

Logs always go to stderr (`main.py` sets that up), so stdout is safe to parse.
"""

import json
from collections.abc import Callable
from typing import Any, NoReturn

import typer


def emit(data: Any, *, json_mode: bool, human: Callable[[Any], None]) -> None:
    """Print `data` as a single JSON document, or call `human(data)` to print it as text."""
    if json_mode:
        typer.echo(json.dumps(data, default=str))
    else:
        human(data)


def fail(message: str, *, code: int = 1, hint: str | None = None, json_mode: bool = False) -> NoReturn:
    """End the command with `message` and an optional `hint` saying what to do, and exit with `code`.

    With `--json` the error is a JSON document on stdout (`{"error": ..., "hint": ...}`), so that a caller parsing
    stdout still gets valid JSON; otherwise it is text on stderr.
    """
    if json_mode:
        typer.echo(json.dumps({"error": message, "hint": hint}))
    else:
        typer.echo(f"error: {message}", err=True)
        if hint:
            typer.echo(f"hint: {hint}", err=True)
    raise typer.Exit(code)

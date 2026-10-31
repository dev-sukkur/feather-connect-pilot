"""The options that every command takes."""

from pathlib import Path
from typing import Annotated

import typer

Env = Annotated[
    str, typer.Option("--env", envvar="FEATHER_ENV", help="The environment: env/<name>.yaml in the project.")
]
Json = Annotated[bool, typer.Option("--json", help="Print one JSON document on stdout.")]
ProjectDir = Annotated[
    Path,
    typer.Option(
        "--project", envvar="FEATHER_PROJECT", help="The project directory, which holds feather.yaml."
    ),
]

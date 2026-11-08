"""`feather package`: build a flow package from the project, publish it to a registry, and search a registry."""

from pathlib import Path
from typing import Annotated

import typer

from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit, fail
from feather_connect.packages.build import build_package
from feather_connect.packages.manifest import ManifestError

package_app = typer.Typer(help="Build, publish and find flow packages.", no_args_is_help=True)

OutDir = Annotated[
    Path | None,
    typer.Option("--out", help="Where the wheel goes: dist/ in the project by default.", show_default=False),
]


@package_app.command()
def build(
    project: ProjectDir = Path("."),
    env: Env = "dev",
    out: OutDir = None,
    json_mode: Json = False,
) -> None:
    """Build the project into a flow package's wheel. feather.yaml needs a name and a version; the project is checked
    as `feather validate` checks it. Building the same tree twice gives the same sha256."""
    try:
        built = build_package(project, out, env=env)
    except ManifestError as error:
        fail(str(error), hint="fix it, then build again", json_mode=json_mode)
    document = {
        "wheel": str(built.path),
        "sha256": built.sha256,
        "size": built.size,
        "name": built.manifest["name"],
        "version": built.manifest["version"],
        "flows": [f["name"] for f in built.manifest["flows"]],
    }
    emit(
        document, json_mode=json_mode, human=lambda d: typer.echo(f"built {d['wheel']}\nsha256 {d['sha256']}")
    )

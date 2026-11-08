"""`feather package`: build a flow package from the project, publish it to a registry, and search a registry."""

import hashlib
from pathlib import Path
from typing import Annotated, Any

import typer

from feather_cli.client import ApiClient, ApiError
from feather_cli.commands import remote
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


Url = Annotated[
    str | None,
    typer.Option(
        "--url",
        envvar="FEATHER_SERVER",
        help="The Feather server whose registry it goes to.",
        show_default=False,
    ),
]
PublishToken = Annotated[
    str | None,
    typer.Option(
        "--token",
        envvar=["FEATHER_PUBLISH_TOKEN", "FEATHER_TOKEN"],
        help="A token with the publish scope, or an admin's.",
        show_default=False,
    ),
]
Wheels = Annotated[
    list[Path] | None,
    typer.Argument(
        help="The wheels to publish: the project's dist/ wheel of its version by default.", show_default=False
    ),
]


def _project_wheel(project: Path, json_mode: bool) -> Path:
    """The wheel `feather package build` writes for the project's name and version."""
    import yaml

    from feather_connect.packages.build import wheel_name

    try:
        config = yaml.safe_load((project / "feather.yaml").read_text(encoding="utf-8")) or {}
        path = project / "dist" / wheel_name(str(config["name"]), str(config["version"]))
    except (OSError, KeyError, TypeError, yaml.YAMLError):
        fail(
            "no feather.yaml with a name and a version here",
            hint="name the wheel, or pass --project",
            json_mode=json_mode,
        )
    if not path.is_file():
        fail(f"there is no {path}", hint="run: feather package build", json_mode=json_mode)
    return path


@package_app.command()
def publish(
    wheels: Wheels = None,
    project: ProjectDir = Path("."),
    url: Url = None,
    token: PublishToken = None,
    json_mode: Json = False,
) -> None:
    """Upload flow package wheels to a Feather server's built-in registry. A version is published once: publishing
    it again is refused, so give each release a new version."""
    if not url:
        fail("no server to publish to", hint="pass --url https://feather-qa.example.com", json_mode=json_mode)
    if not token:
        fail("no token", hint="pass --token, or set FEATHER_PUBLISH_TOKEN", json_mode=json_mode)
    paths = list(wheels or [_project_wheel(project, json_mode)])
    published: list[dict[str, str]] = []
    with ApiClient(url, token) as api:
        for path in paths:
            if not path.is_file():
                fail(f"there is no {path}", json_mode=json_mode)
            try:
                api.publish(path)
            except ApiError as error:
                fail(str(error), hint=_publish_hint(error.status), json_mode=json_mode)
            published.append({"wheel": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    emit(
        {"url": url, "published": published},
        json_mode=json_mode,
        human=lambda d: typer.echo("\n".join(f"published {p['wheel']}" for p in d["published"])),
    )


def _publish_hint(status: int | None) -> str | None:
    return {
        401: "the token is not valid: create one with feather server create-token ci --scope publish",
        403: "the token may not publish: it needs the publish scope or an admin",
        409: "a version is never replaced: raise version: in feather.yaml and build again",
    }.get(status or 0)


@package_app.command()
def search(
    query: Annotated[str, typer.Argument(help="Part of the package's name.")] = "",
    registry: Annotated[str, typer.Option(help="The registry to search.")] = "feather",
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Find packages in one of the environment's registries, with their versions."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(lambda: api.search_packages(registry, query), json_mode)

    def human(doc: dict[str, Any]) -> None:
        if not doc["items"]:
            typer.echo("no package matches")
        for item in doc["items"]:
            versions = ", ".join(v + (" (yanked)" if v in item["yanked"] else "") for v in item["versions"])
            typer.echo(f"{item['name']}: {versions}")

    emit({"registry": registry, "items": found}, json_mode=json_mode, human=human)

"""`feather package`: build a flow package from the project, publish it to a registry, search a registry, fetch a
wheel from one, and install a version into an environment, list what is installed and show it."""

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

package_app = typer.Typer(help="Build, publish, find and install flow packages.", no_args_is_help=True)

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
        "warnings": list(built.warnings),
    }
    for warning in built.warnings:
        typer.echo(f"warning: {warning}", err=True)
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


Reason = Annotated[str | None, typer.Option(help="Why, for the audit log.", show_default=False)]
FromRegistry = Annotated[str, typer.Option("--registry", help="The registry to install from.")]


def _wanted(spec: str, json_mode: bool) -> tuple[str, str | None]:
    """`acme-invoices==1.4.0` or `acme-invoices`, as pip writes it."""
    name, _, version = spec.partition("==")
    if not name.strip() or (version == "" and "==" in spec):
        fail(
            f"{spec!r} is not a package",
            hint="write it as acme-invoices or acme-invoices==1.4.0",
            json_mode=json_mode,
        )
    return name.strip(), version.strip() or None


_INSTALL_HINTS = {
    "not_installed": "a version is installed once: install a new version",
    "fetch": "check the name and version with: feather package search",
    "not_yanked": "install another version, or pass --allow-yanked to take this one anyway",
    "lock": "build it again with feather package build, which locks every requirement with its hashes",
    "sha256": "the registry's record and the wheel disagree: ask the publisher to publish a new version",
    "manifest": "it is not a flow package this server can read: build it again with feather package build",
    "compatibility": "build it for this Feather Connect, or upgrade the server",
}


@package_app.command()
def install(
    spec: Annotated[str, typer.Argument(help="The package, with a version: acme-invoices==1.4.0.")],
    registry: FromRegistry = "feather",
    reason: Reason = None,
    allow_yanked: Annotated[bool, typer.Option(help="Take a version the registry has yanked.")] = False,
    pre: Annotated[bool, typer.Option(help="With no version, a pre-release may be the newest.")] = False,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Install a version of a package from one of the environment's registries: the newest not yanked when none is
    given, and not a pre-release unless --pre. The server checks it and records it preparing; a worker then builds
    its environment and load-checks it, and its flows start inactive: nothing runs until one is activated."""
    name, version = _wanted(spec, json_mode)
    with remote.client(project, env, server, token, json_mode) as api:

        def attempt() -> dict[str, Any]:
            try:
                return api.install_package(
                    name, version, registry, reason, allow_yanked=allow_yanked, pre=pre
                )
            except ApiError as error:
                hint = _INSTALL_HINTS.get(str(error.problem.get("check") or ""))
                if hint is None:
                    raise
                fail(str(error), hint=hint, json_mode=json_mode)

        installed = remote.call(attempt, json_mode)

    def human(doc: dict[str, Any]) -> None:
        typer.echo(f"installed {doc['name']} {doc['version']} from {doc['registry']}, sha256 {doc['sha256']}")
        for flow in doc["flows"]:
            typer.echo(f"  {flow}: inactive")
        typer.echo(
            f"it is {doc['state']}: a worker builds its environment and load-checks it "
            "(feather package show says when it is ready); then activate a flow to run it"
        )

    emit(installed, json_mode=json_mode, human=human)


@package_app.command("list")
def list_packages(
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """The packages installed in the environment, with their versions."""
    with remote.client(project, env, server, token, json_mode) as api:
        found = remote.call(api.packages, json_mode)

    def human(doc: dict[str, Any]) -> None:
        if not doc["items"]:
            typer.echo("no package is installed")
        for item in doc["items"]:
            versions = ", ".join(f"{v['version']} ({v['state']})" for v in item["versions"])
            typer.echo(f"{item['name']}: {versions}; {item['active_flows']} flows active")

    emit({"items": found}, json_mode=json_mode, human=human)


@package_app.command()
def show(
    spec: Annotated[str, typer.Argument(help="The package, or a version of it: acme-invoices==1.4.0.")],
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """An installed package's versions and flows, or one version's manifest, load check and flows."""
    name, version = _wanted(spec, json_mode)
    with remote.client(project, env, server, token, json_mode) as api:
        if version is None:
            found = remote.call(lambda: api.package(name), json_mode)
        else:
            found = remote.call(lambda: api.package_version(name, version), json_mode)

    def human(doc: dict[str, Any]) -> None:
        for v in doc.get("versions") or [doc]:
            typer.echo(
                f"{v['name']} {v['version']}: {v['state']}, from {v['registry']}, sha256 {v['sha256']}"
            )
        for flow in doc["flows"]:
            typer.echo(f"  {flow['flow']} {flow['version']}: {flow['state']}")

    emit(found, json_mode=json_mode, human=human)


@package_app.command()
def fetch(
    spec: Annotated[str, typer.Argument(help="The package and version: acme-invoices==1.4.0.")],
    registry: Annotated[str, typer.Option(help="The registry to fetch from.")] = "feather",
    out: OutDir = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: remote.Server = None,
    token: remote.Token = None,
    json_mode: Json = False,
) -> None:
    """Download a version's wheel from one of the environment's registries, through the server, which checks its
    sha256 and manifest as an install does. It needs an admin, as an install does, and goes to dist/ by default."""
    name, version = _wanted(spec, json_mode)
    if version is None:
        fail("fetch needs a version", hint="write it as acme-invoices==1.4.0", json_mode=json_mode)
    where = out or Path("dist")
    with remote.client(project, env, server, token, json_mode) as api:
        path, sha256 = remote.call(lambda: api.fetch_wheel(registry, name, version, where), json_mode)
    emit(
        {"wheel": str(path), "sha256": sha256, "registry": registry},
        json_mode=json_mode,
        human=lambda d: typer.echo(f"fetched {d['wheel']}\nsha256 {d['sha256']}"),
    )

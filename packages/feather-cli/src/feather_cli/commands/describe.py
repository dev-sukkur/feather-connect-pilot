"""`feather describe`: what an installed connector takes and can do, and what a project's flow or dataset is."""

from pathlib import Path
from typing import Annotated, Any

import typer

from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit, fail
from feather_connect.core.registry import ConnectorInfo, describe_connector, list_connectors
from feather_connect.project import Project, ProjectError, load_project

describe_app = typer.Typer(
    help="Describe a connector, or a flow or dataset of the project.", no_args_is_help=True
)

Name = Annotated[str, typer.Argument(help="The name to describe.")]


def _connector(info: ConnectorInfo) -> dict[str, Any]:
    spec = info.spec
    return {
        "name": info.name,
        "kind": info.kind,
        "maturity": info.maturity,
        "package": info.package,
        "capabilities": {
            "strategies": sorted(spec.strategies),
            "modes": sorted(spec.modes),
            "deletes": spec.deletes,
            "resumable": spec.resumable,
            "cost": dict(spec.cost),
        },
        "config_schema": info.config_schema,
    }


def _print_connector(doc: dict[str, Any]) -> None:
    caps = doc["capabilities"]
    typer.echo(f"{doc['name']} ({doc['kind']}, {doc['maturity']}) from {doc['package'] or 'this checkout'}")
    if caps["strategies"]:
        typer.echo(f"  strategies: {', '.join(caps['strategies'])}")
    if doc["kind"] == "sink":
        typer.echo(f"  modes: {', '.join(caps['modes'])}; deletes: {caps['deletes']}")
    if caps["cost"]:
        typer.echo(f"  cost: {', '.join(f'{k} {v}' for k, v in caps['cost'].items())}")
    settings = doc["config_schema"].get("properties", {})
    required = set(doc["config_schema"].get("required", ()))
    for name, field in settings.items():
        star = " (required)" if name in required else ""
        typer.echo(f"  {name}{star}: {field.get('description') or field.get('type', '')}")


@describe_app.command("connector")
def connector(name: Name, json_mode: Json = False) -> None:
    """The connector's settings as a JSON schema, and its capabilities."""
    try:
        found = describe_connector(name)
    except LookupError as error:
        fail(str(error), hint="`feather describe connectors` lists them", json_mode=json_mode)
    emit(_connector(found), json_mode=json_mode, human=_print_connector)


@describe_app.command("connectors")
def connectors(json_mode: Json = False) -> None:
    """Every installed connector."""
    found = [_connector(info) for info in list_connectors()]

    def human(docs: list[dict[str, Any]]) -> None:
        for doc in docs:
            typer.echo(f"{doc['name']:<24} {doc['kind']:<7} {doc['maturity']}")

    emit(found, json_mode=json_mode, human=human)


def _load(project: Path, env: str, json_mode: bool) -> Project:
    if not (project / "feather.yaml").is_file():
        fail(
            f"no feather.yaml in {project.resolve()}",
            hint="run it from the project directory, or name that directory with --project",
            json_mode=json_mode,
        )
    try:
        return load_project(project, env)
    except ProjectError as error:
        fail(str(error), hint="`feather validate` says more", json_mode=json_mode)


def _print_spec(doc: dict[str, Any]) -> None:
    for key, value in doc.items():
        typer.echo(f"{key}: {value}")


@describe_app.command("flow")
def flow(name: Name, project: ProjectDir = Path("."), env: Env = "dev", json_mode: Json = False) -> None:
    """The flow's datasets, routes and triggers, as the catalogue stores them."""
    found = _load(project, env, json_mode)
    for candidate in found.flows:
        if candidate.name == name:
            emit(candidate.spec_dict(), json_mode=json_mode, human=_print_spec)
            return
    fail(
        f"the project has no flow named {name!r}",
        hint=f"its flows are {', '.join(f.name for f in found.flows) or 'none'}",
        json_mode=json_mode,
    )


@describe_app.command("dataset")
def dataset(name: Name, project: ProjectDir = Path("."), env: Env = "dev", json_mode: Json = False) -> None:
    """The dataset as its flow's description has it, with the flow's name."""
    found = _load(project, env, json_mode)
    for candidate in found.flows:
        for spec in candidate.spec_dict()["datasets"]:
            if spec["name"] == name:
                emit({**spec, "flow": candidate.name}, json_mode=json_mode, human=_print_spec)
                return
    fail(
        f"the project has no dataset named {name!r}",
        hint="`feather describe flow <name>` lists them",
        json_mode=json_mode,
    )

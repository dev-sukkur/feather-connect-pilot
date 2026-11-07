"""`feather run`: run a flow here (`--local`), on simulation data if asked (`--sim`), or start a run on the server.

A local run uses the in-memory store and the local engine, in this process, against the connections the
environment names; `--sim` swaps each of them for a simulation stand-in that reads the project's `fixtures/`.
"""

import logging
from collections import Counter
from pathlib import Path
from typing import Annotated, Any

import typer

from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit, fail
from feather_connect.clock import SystemClock
from feather_connect.core.flow import Flow
from feather_connect.engine.local import LocalEngine
from feather_connect.project import Project, ProjectError, load_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool
from feather_connect.worker import config_of

log = logging.getLogger(__name__)

Local = Annotated[bool, typer.Option("--local", help="Run in this process, with in-memory storage.")]
Sim = Annotated[
    bool, typer.Option("--sim", help="Swap every connection for a simulation one, fed by fixtures/.")
]
Partitions = Annotated[
    list[str] | None,
    typer.Option(
        "--partition", help="Run only this partition, as key=value. Repeat it for more.", show_default=False
    ),
]
FlowName = Annotated[str, typer.Argument(help="The flow to run.")]


def _load(project: Path, env: str, sim: bool, json_mode: bool) -> Project:
    if not (project / "feather.yaml").is_file():
        fail(
            f"no feather.yaml in {project.resolve()}",
            hint="run it from the project directory, or name that directory with --project",
            json_mode=json_mode,
        )
    try:
        return load_project(project, env, simulate=sim)
    except ProjectError as error:
        fail(str(error), hint="`feather validate` says more", json_mode=json_mode)


def _flow(found: Project, name: str, json_mode: bool) -> Flow:
    for flow in found.flows:
        if flow.name == name:
            return flow
    fail(
        f"the project has no flow named {name!r}",
        hint=f"its flows are {', '.join(f.name for f in found.flows) or 'none'}",
        json_mode=json_mode,
    )


def _check_credentials(found: Project, flow: Flow, json_mode: bool) -> None:
    """Resolve the secrets of every connection the flow uses, and stop naming the first that is missing."""
    used = [d.source.connection for d in flow.datasets] + [r.sink.connection for r in flow.routes]
    missing: list[str] = []
    for name in dict.fromkeys(used):
        connection = found.connections.get(name)
        if connection is None:
            continue
        try:
            connection.resolve()
        except ProjectError as error:
            missing.append(f"connection {name!r} needs a credential: {error}")
    if missing:
        fail(
            "; ".join(missing),
            hint="set what it names, or run on simulation data with --sim",
            json_mode=json_mode,
        )


def run_local(found: Project, flow: Flow, partitions: list[str] | None) -> dict[str, Any]:
    """Run the flow to the end in this process and say how it went, per route. Raises `ValueError` for a partition
    that isn't one of the flow's."""
    for dataset in flow.datasets:
        for key in partitions or ():
            try:
                dataset.partition.label_of(key)
            except (ValueError, KeyError):
                raise ValueError(f"{key!r} is not a partition of dataset {dataset.name!r}") from None
    clock = SystemClock()
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine()
    for budget in found.budgets.values():
        store.budgets.define(budget)
    runtime = Runtime(
        found.flows,
        store,
        spool,
        engine,
        code_sha=found.code_sha,
        clock=clock,
        config_of=lambda connection: config_of(found, connection) if connection in found.connections else {},
        timezone=str(found.settings["timezone"]),
    )
    engine.register(runtime.tasks())
    run_id = runtime.start_run(flow.name, trigger="manual", actor="cli", partitions=partitions)
    engine.run_until_idle()
    run = store.runs.get_run(run_id)
    routes: dict[str, dict[str, Any]] = {}
    for route in flow.routes:
        deliveries = store.ledger.list_deliveries(route=route.name, limit=10_000)
        statuses = Counter(d.status for d in deliveries)
        routes[route.name] = {
            "deliveries": dict(statuses),
            "applied": sum(d.rows_applied for d in deliveries),
            "rejected": sum(d.rows_rejected for d in deliveries),
            "errors": sorted({d.last_error for d in deliveries if d.last_error}),
        }
    quarantine = Counter(row.route for row in store.quarantine.list_open())
    failed = any(r["deliveries"].get("failed") for r in routes.values())
    return {
        "run_id": run_id,
        "flow": flow.name,
        "status": "failed" if failed and run.status == "succeeded" else run.status,
        "reason": run.reason,
        "partitions": partitions,
        "routes": routes,
        "quarantine": dict(quarantine),
    }


def _print(doc: dict[str, Any]) -> None:
    typer.echo(f"run {doc['run_id']} of {doc['flow']}: {doc['status']}")
    if doc.get("reason"):
        typer.echo(f"  reason: {doc['reason']}")
    for name, route in doc.get("routes", {}).items():
        counts = ", ".join(f"{n} {s}" for s, n in sorted(route["deliveries"].items())) or "nothing delivered"
        typer.echo(f"  {name}: {route['applied']} applied, {route['rejected']} rejected ({counts})")
        for error in route["errors"]:
            typer.echo(f"    {error}")
    for name, n in doc.get("quarantine", {}).items():
        typer.echo(f"  quarantine on {name}: {n}")


def run(
    flow: FlowName,
    local: Local = False,
    sim: Sim = False,
    partition: Partitions = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: Annotated[
        str | None, typer.Option("--server", envvar="FEATHER_SERVER", show_default=False)
    ] = None,
    token: Annotated[str | None, typer.Option("--token", envvar="FEATHER_TOKEN", show_default=False)] = None,
    json_mode: Json = False,
) -> None:
    """Run a flow. `--local` runs it here; without it the server starts the run. Exits 1 if the run fails."""
    if not local:
        if sim:
            fail("--sim runs only with --local", hint="add --local", json_mode=json_mode)
        fail(
            "a run on the server needs the remote commands",
            hint="add --local to run it here",
            json_mode=json_mode,
        )
    found = _load(project, env, sim, json_mode)
    chosen = _flow(found, flow, json_mode)
    if not sim:
        _check_credentials(found, chosen, json_mode)
    try:
        doc = run_local(found, chosen, partition)
    except (KeyError, ValueError, ProjectError) as error:
        fail(str(error), hint="`feather validate` says more", json_mode=json_mode)
    emit(doc, json_mode=json_mode, human=_print)
    if doc["status"] != "succeeded":
        if not json_mode:
            typer.echo(f"error: the run ended {doc['status']}", err=True)
        raise typer.Exit(1)

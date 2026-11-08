"""Commands that call a server: `status`, `runs`, `logs`, `backfill`, `replay`, and `run` without `--local`.

The server's URL and token come from `--server` and `--token`, then `FEATHER_SERVER` and `FEATHER_TOKEN`, then the
`server:` entry of `env/<env>.yaml` (`url`, and `token` as a secret reference). A mass action shows its preview and
goes ahead only with `--yes` or when `yes` is typed at the prompt.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, NoReturn

import typer
import yaml

from feather_cli.client import ApiClient, ApiError
from feather_cli.options import Env, Json, ProjectDir
from feather_cli.output import emit, fail
from feather_connect.project import ProjectError, resolve_secret

Server = Annotated[
    str | None,
    typer.Option("--server", envvar="FEATHER_SERVER", help="The server's URL.", show_default=False),
]
Token = Annotated[
    str | None, typer.Option("--token", envvar="FEATHER_TOKEN", help="An API token.", show_default=False)
]
Yes = Annotated[bool, typer.Option("--yes", help="Go ahead without asking.")]
DryRun = Annotated[bool, typer.Option("--dry-run", help="Show the preview and change nothing.")]
Reason = Annotated[
    str | None, typer.Option("--reason", help="Why: kept in the audit row.", show_default=False)
]


def _from_env_file(project: Path, env: str) -> tuple[str | None, str | None]:
    path = project / "env" / f"{env}.yaml"
    if not path.is_file():
        return None, None
    try:
        found = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return None, None
    server = found.get("server") if isinstance(found, dict) else None
    if not isinstance(server, dict):
        return None, None
    url, token = server.get("url"), server.get("token")
    if isinstance(token, str) and token.startswith("${secret:"):
        token = resolve_secret(token, project)
    return (url if isinstance(url, str) else None), (token if isinstance(token, str) else None)


def client(project: Path, env: str, server: str | None, token: str | None, json_mode: bool) -> ApiClient:
    """The client for the server these settings name. Stops with a hint when none does."""
    if server is None or token is None:
        try:
            file_url, file_token = _from_env_file(project, env)
        except ProjectError as error:
            fail(str(error), hint="set the secret, or pass --token", json_mode=json_mode)
        same_server = server is None or (file_url is not None and server.rstrip("/") == file_url.rstrip("/"))
        if token is None and same_server:  # the file's token is for the file's server, and no other
            token = file_token
        server = server or file_url
    if not server:
        fail(
            "no server to call",
            hint=f"pass --server, set FEATHER_SERVER, or add server: {{url: ...}} to env/{env}.yaml",
            json_mode=json_mode,
        )
    return ApiClient(server, token)


def _failed(error: ApiError, json_mode: bool) -> NoReturn:
    hint = None
    if error.status == 401:
        hint = "pass a valid token with --token or FEATHER_TOKEN"
    elif error.status is None:
        hint = "check the URL, and that `feather server` is running there"
    elif error.problem.get("preview"):
        hint = f"preview first: {error.problem['preview']}"
    fail(str(error), hint=hint, json_mode=json_mode)


def call[T](fn: Callable[[], T], json_mode: bool) -> T:
    """Call the API, ending the command with the reason and a hint when it fails."""
    try:
        return fn()
    except ApiError as error:
        _failed(error, json_mode)


def _confirm(preview: dict[str, Any], yes: bool, dry_run: bool, json_mode: bool) -> bool:
    """Whether to go ahead after showing the preview."""
    impact = preview["impact"]
    if not json_mode:
        units = impact.get("estimated_budget_units")
        typer.echo(
            f"{preview['action']}: {impact['partitions']} partition(s), {impact['deliveries']} delivery(ies)"
            + (f", about {units} budget units" if units is not None else "")
        )
        for sample in impact.get("samples", []):
            typer.echo(f"  {sample}")
    if dry_run:
        return False
    if yes:
        return True
    if json_mode:
        fail(
            "a mass action needs --yes with --json",
            hint="add --yes, or --dry-run to only preview",
            json_mode=True,
        )
    return typer.prompt("Type yes to go ahead", default="no").strip().lower() == "yes"


# Reads.


def status(
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: Server = None,
    token: Token = None,
    json_mode: Json = False,
) -> None:
    """What is failing, what is running, and the workers."""
    with client(project, env, server, token, json_mode) as api:
        found = call(api.summary, json_mode)

    def human(doc: dict[str, Any]) -> None:
        counts = ", ".join(f"{n} {s}" for s, n in sorted(doc["deliveries"].items())) or "none"
        typer.echo(f"deliveries: {counts}")
        typer.echo(
            f"failed deliveries: {doc['failed_deliveries']}; open quarantine: {doc['open_quarantine']}"
        )
        typer.echo(f"runs: {doc['running_runs']} running, {doc['failed_runs']} failed")
        for name in doc["paused"]:
            typer.echo(f"paused: {name}")
        for worker in doc["workers"]:
            typer.echo(f"worker {worker['worker_id']}: {worker['state']}, last seen {worker['last_seen']}")

    emit(found, json_mode=json_mode, human=human)


def runs(
    flow: Annotated[str | None, typer.Option("--flow", show_default=False)] = None,
    status_: Annotated[str | None, typer.Option("--status", show_default=False)] = None,
    limit: int = 20,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: Server = None,
    token: Token = None,
    json_mode: Json = False,
) -> None:
    """The newest runs, filtered by flow and status."""
    with client(project, env, server, token, json_mode) as api:
        found = call(lambda: api.runs(flow=flow, status=status_, limit=limit), json_mode)

    def human(doc: dict[str, Any]) -> None:
        for r in doc["items"]:
            typer.echo(
                f"{r['run_id']}  {r['flow']:<24} {r['status']:<10} {r['trigger']:<8} {r['created_at']}"
            )
        if not doc["items"]:
            typer.echo("no runs")

    emit(found, json_mode=json_mode, human=human)


def logs(
    run_id: Annotated[str, typer.Argument(help="The run.")],
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: Server = None,
    token: Token = None,
    json_mode: Json = False,
) -> None:
    """The log lines of each task of a run, in task order."""
    tasks: list[dict[str, Any]] = []
    with client(project, env, server, token, json_mode) as api:
        cursor = None
        while True:
            page = call(lambda c=cursor: api.run_tasks(run_id, cursor=c), json_mode)
            for task in page["items"]:
                lines = call(lambda t=task: api.task_logs(t["task_id"]), json_mode)
                tasks.append(
                    {"task_id": task["task_id"], "kind": task["kind"], "ref": task["ref"], "lines": lines}
                )
            cursor = page["next_cursor"]
            if cursor is None:
                break

    def human(doc: dict[str, Any]) -> None:
        for task in doc["tasks"]:
            typer.echo(f"== {task['kind']} {task['ref']} ({task['task_id']})")
            for line in task["lines"]:
                typer.echo(line)

    emit({"run_id": run_id, "tasks": tasks}, json_mode=json_mode, human=human)


# Actions.


def start_run(
    flow: str,
    partitions: list[str] | None,
    *,
    project: Path,
    env: str,
    server: str | None,
    token: str | None,
    json_mode: bool,
) -> None:
    """Start a run of `flow` on the server: what `feather run` without `--local` does."""
    with client(project, env, server, token, json_mode) as api:
        found = call(lambda: api.start_run(flow, partitions=partitions), json_mode)
    emit(
        {"run_id": found["run_id"], "flow": flow, "status": "queued"},
        json_mode=json_mode,
        human=lambda d: typer.echo(
            f"started run {d['run_id']} of {flow}: `feather logs {d['run_id']}` follows it"
        ),
    )


def backfill(
    flow: Annotated[str, typer.Argument(help="The flow to backfill.")],
    start: Annotated[str, typer.Option("--from", help="The first day, as YYYY-MM-DD.")],
    end: Annotated[str, typer.Option("--to", help="The last day, as YYYY-MM-DD.")],
    max_concurrent: Annotated[int, typer.Option("--max-concurrent")] = 4,
    dry_run: DryRun = False,
    yes: Yes = False,
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: Server = None,
    token: Token = None,
    json_mode: Json = False,
) -> None:
    """Read the days from --from to --to again. Shows the impact first."""
    with client(project, env, server, token, json_mode) as api:
        preview = call(
            lambda: api.preview_backfill(flow, start, end, max_concurrent=max_concurrent), json_mode
        )
        if not _confirm(preview, yes, dry_run, json_mode):
            emit(
                {"done": False, "preview": preview},
                json_mode=json_mode,
                human=lambda _: typer.echo("nothing done"),
            )
            return
        found = call(lambda: api.backfill(flow, preview["confirmation_token"], reason=reason), json_mode)
    emit(
        {"done": True, "run_id": found["run_id"], "preview": preview},
        json_mode=json_mode,
        human=lambda d: typer.echo(f"started backfill run {d['run_id']}"),
    )


def replay(
    route: Annotated[str, typer.Argument(help="The route to deliver to again.")],
    version: Annotated[
        str | None, typer.Option("--version", help="A version to replay.", show_default=False)
    ] = None,
    partition: Annotated[
        str | None, typer.Option("--partition", help="A partition, as key=value.", show_default=False)
    ] = None,
    dry_run: DryRun = False,
    yes: Yes = False,
    reason: Reason = None,
    project: ProjectDir = Path("."),
    env: Env = "dev",
    server: Server = None,
    token: Token = None,
    json_mode: Json = False,
) -> None:
    """Deliver applied data to the route again: one version, or a partition's latest. Shows the impact first."""
    if (version is None) == (partition is None):
        fail(
            "give either --version or --partition",
            hint="`feather runs` and the dashboard show both",
            json_mode=json_mode,
        )
    selection: dict[str, Any] = {"route": route}
    if version is not None:
        selection["version_ids"] = [version]
    else:
        selection["partition"] = partition
    with client(project, env, server, token, json_mode) as api:
        preview = call(lambda: api.preview_deliveries("replay", selection), json_mode)
        if not _confirm(preview, yes, dry_run, json_mode):
            emit(
                {"done": False, "preview": preview},
                json_mode=json_mode,
                human=lambda _: typer.echo("nothing done"),
            )
            return
        found = call(
            lambda: api.apply_deliveries("replay", preview["confirmation_token"], reason=reason), json_mode
        )
    emit(
        {"done": True, "moved": found["moved"], "preview": preview},
        json_mode=json_mode,
        human=lambda d: typer.echo(f"replaying {len(d['moved'])} delivery(ies)"),
    )

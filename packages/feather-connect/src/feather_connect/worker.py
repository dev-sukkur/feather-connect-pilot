"""The worker process: loads a flow project, registers it in the catalogue, heartbeats and executes tasks.

    python -m feather_connect.worker PROJECT_ROOT ENV --database-url postgresql://...

On SIGTERM (on Windows also CTRL_BREAK_EVENT) it marks itself `draining`, stops taking work, waits up to 30 s for
the tasks that are running, and exits 0. A worker that is killed instead is found by the sweeper, which sees its
heartbeat stop (design 6.12).

The engine side is DBOS (`feather-engine-dbos`), which is imported when the worker starts and not before, so that
the SDK does not depend on it.
"""

import argparse
import logging
import os
import signal
import socket
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import FrameType
from typing import Any

from feather_connect.core.errors import PermanentError
from feather_connect.logs import configure_logging, install_task_logs, register_secrets
from feather_connect.project import Project, ProjectError, environment_layer, load_project, use_db_secrets
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate
from feather_connect.vault import Vault, master_key_from_env

log = logging.getLogger(__name__)

HEARTBEAT_S = 5.0
DRAIN_S = 30.0


def _check_connections(project: Project) -> None:
    """Fail at startup, not in a task, when a flow uses a connection the project does not define."""
    for flow in project.flows:
        used = {d.source.connection for d in flow.datasets} | {r.sink.connection for r in flow.routes}
        for name in sorted(used - project.connections.keys()):
            raise ProjectError(
                f"flow {flow.name!r} uses the connection {name!r}, which {project.env!r} does not define: "
                "add it under connections: in feather.yaml or env/<env>.yaml"
            )


def _heartbeats(
    store: Store, worker_id: str, labels: Mapping[str, str], sha: str, slots: int
) -> threading.Event:
    """Heartbeat every `HEARTBEAT_S` from a daemon thread, until the returned event is set."""
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(HEARTBEAT_S):
            try:
                store.workers.heartbeat(worker_id, labels, sha, slots)
            except Exception:  # a database that is away for a moment must not end the thread
                log.warning("heartbeat of %s failed", worker_id, exc_info=True)

    threading.Thread(target=beat, name="heartbeat", daemon=True).start()
    return stop


def config_of(project: Project, connection: str) -> Mapping[str, Any]:
    """Return the connection's settings, secrets resolved and registered, so that no log line can show one."""
    config = project.connections[connection]
    try:
        resolved = config.resolve()
    except ProjectError as error:  # no retry brings back a secret that isn't set
        raise PermanentError(f"connection {connection!r}: {error}") from None
    register_secrets(*(resolved[key] for key in config.secret_refs))
    return resolved


def run_worker(
    project_root: Path,
    env: str,
    *,
    database_url: str,
    worker_id: str | None = None,
    slots: int = 8,
    labels: Mapping[str, str] | None = None,
    poll_s: float = 0.5,
    stop: threading.Event | None = None,
) -> int:
    """Run a worker until it is told to stop, and return the exit code. `worker_id` defaults to
    `FEATHER_WORKER_ID`, then the host name.

    It stops on SIGTERM, SIGINT or SIGBREAK, or, when `stop` is given, when that event is set: a caller that runs
    the worker in a thread (`feather dev`) passes one, and no signal handler is installed.
    """
    from feather_engine_dbos import DbosEngine

    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"))
    worker_id = worker_id or os.environ.get("FEATHER_WORKER_ID") or socket.gethostname()
    labels = dict(labels or {})
    migrate(database_url)
    store, spool = connect(database_url)
    key = master_key_from_env()
    use_db_secrets(lambda name: Vault(store.secrets, env, key).open(name))
    project = load_project(project_root, env, environment=environment_layer(store.env_config.entries(env)))
    _check_connections(project)
    remove_task_logs = install_task_logs(store)
    engine: DbosEngine | None = None
    try:
        store.catalog.sync(project.name, project.code_sha, {f.name: f.spec_dict() for f in project.flows})
        for budget in project.budgets.values():
            store.budgets.define(budget)
        engine = DbosEngine(database_url)
        runtime = Runtime(
            project.flows,
            store,
            spool,
            engine,
            code_sha=project.code_sha,
            budgets={c.name: c.budget for c in project.connections.values() if c.budget is not None},
            config_of=lambda connection: config_of(project, connection),
            timezone=str(project.settings["timezone"]),
        )
        return _work(runtime, store, database_url, worker_id, labels, slots, poll_s, stop, project.code_sha)
    finally:  # a reload in `feather dev` runs this again in the same process
        remove_task_logs()
        if engine is not None:
            engine.close()
        store.close()


def _work(
    runtime: Runtime,
    store: Store,
    database_url: str,
    worker_id: str,
    labels: Mapping[str, str],
    slots: int,
    poll_s: float,
    stop: threading.Event | None,
    code_sha: str,
) -> int:
    """Execute tasks and heartbeat until told to stop, and drain."""
    from feather_engine_dbos import DbosWorkerRuntime

    told_to_stop = threading.Event() if stop is None else stop

    def on_signal(signum: int, frame: FrameType | None) -> None:
        told_to_stop.set()

    if stop is None:
        for name in ("SIGTERM", "SIGINT", "SIGBREAK"):  # SIGBREAK is CTRL_BREAK_EVENT, on Windows only
            if (number := getattr(signal, name, None)) is not None:
                signal.signal(number, on_signal)

    store.workers.heartbeat(worker_id, labels, code_sha, slots)
    # A heartbeat keeps a draining worker draining, so that the drain shows to the end. This is a new process, though:
    # a restarted container keeps its hostname, the default id, and its last process left the row draining.
    store.workers.mark(worker_id, "alive")
    handle = DbosWorkerRuntime(database_url, poll_s=poll_s, drain_s=DRAIN_S).run(
        runtime.tasks(), worker_id, labels, slots
    )
    stop_beating = _heartbeats(store, worker_id, labels, code_sha, slots)
    try:
        while not told_to_stop.wait(
            0.5
        ):  # a timeout, because a signal does not interrupt a plain wait on Windows
            pass
        store.workers.mark(worker_id, "draining")
        handle.stop()
    finally:
        stop_beating.set()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m feather_connect.worker", description=__doc__)
    parser.add_argument("project_root", type=Path)
    parser.add_argument("env")
    parser.add_argument("--database-url", default=os.environ.get("FEATHER_DATABASE_URL"))
    parser.add_argument("--worker-id")
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--label", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--poll-s", type=float, default=float(os.environ.get("FEATHER_ENGINE_POLL_S", "0.5")))
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("give --database-url or set FEATHER_DATABASE_URL")
    labels = dict(label.split("=", 1) for label in args.label)
    return run_worker(
        args.project_root,
        args.env,
        database_url=args.database_url,
        worker_id=args.worker_id,
        slots=args.slots,
        labels=labels,
        poll_s=args.poll_s,
    )


if __name__ == "__main__":
    raise SystemExit(main())

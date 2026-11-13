"""The worker loop that every package host runs (`feather_connect.host`): it heartbeats and executes tasks.

The project mode, where this module loaded a project folder and ran its flows, is retired (flow packages §13): a worker
is a supervisor (`feather_connect.supervisor`, `feather worker`) that runs a package host per installed version.

On SIGTERM (on Windows also CTRL_BREAK_EVENT) it marks itself `draining`, stops taking work, waits up to 30 s for
the tasks that are running, and exits 0. A worker that is killed instead is found by the sweeper, which sees its
heartbeat stop (design 6.12).

The engine side is DBOS (`feather-engine-dbos`), which is imported when the worker starts and not before, so that
the SDK does not depend on it.
"""

import logging
import signal
import threading
from collections.abc import Callable, Mapping
from types import FrameType
from typing import Any

from feather_connect.core.errors import PermanentError
from feather_connect.engine.port import TaskFn
from feather_connect.logs import register_secrets
from feather_connect.project import Project, ProjectError
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store

log = logging.getLogger(__name__)

HEARTBEAT_S = 5.0
DRAIN_S = 30.0


def check_connections(project: Project) -> None:
    """Fail at startup, not in a task, when a flow uses a connection the project does not define."""
    for flow in project.flows:
        used = {
            d.source.connection for d in flow.datasets if not getattr(d.source, "connectionless", False)
        } | {r.sink.connection for r in flow.routes}  # a derived dataset reads the store, not a connection
        for name in sorted(used - project.connections.keys()):
            raise ProjectError(
                f"flow {flow.name!r} uses the connection {name!r}, which {project.env!r} does not define: "
                "add it under connections: in feather.yaml or env/<env>.yaml"
            )


def heartbeats(
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


def work(
    runtime: Runtime,
    store: Store,
    database_url: str,
    worker_id: str,
    labels: Mapping[str, str],
    slots: int,
    poll_s: float,
    stop: threading.Event | None,
    code_sha: str,
    background: "Callable[[threading.Event], None] | None" = None,
    tasks: "Mapping[str, TaskFn] | None" = None,
    tenant: str = "default",
) -> int:
    """Execute tasks and heartbeat until told to stop, and drain. `background`, if given, runs on a thread of its own
    from when the worker takes tasks, and is given the event that says to stop. `tasks` replaces the runtime's."""
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
    handle = DbosWorkerRuntime(database_url, poll_s=poll_s, drain_s=DRAIN_S, tenant=tenant).run(
        runtime.tasks() if tasks is None else tasks, worker_id, labels, slots
    )
    stop_beating = heartbeats(store, worker_id, labels, code_sha, slots)
    if background is not None:
        threading.Thread(target=background, args=(told_to_stop,), name="background", daemon=True).start()
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

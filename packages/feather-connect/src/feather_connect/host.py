"""A package host (flow packages §7.1): the worker loop of M1, running one installed package version's flows.

    python -m feather_connect.host PACKAGE VERSION ENV --worker-id ID --cache DIR

The worker supervisor (`feather_connect.supervisor`) starts one per package version with work, with the Python of that
version's dependency environment, an environment holding no secret of the worker's (`FEATHER_DATABASE_URL` is the
host role's, `FEATHER_HOST_DATABASE_URL`, where one is set up), and a pipe to itself on stdin and stdout
(`packages.link`). The host:

- stops when that pipe closes, and on Linux when its parent dies (`PR_SET_PDEATHSIG`), so that it never outlives its
  supervisor; and holds a Postgres advisory lock on its id from before it takes work, so that a second host with the
  same id, started by a supervisor that replaced a dead one, refuses to start while the first still runs (§7.1);
- loads the version's stored wheel, its sha256 checked again (§11), from the database, never from a registry, and its
  flows with the package prefix, for the environment, as the load check did; then says `ready` on the pipe, which is
  when the supervisor moves the version's flows' triggers to it (§6), or `failed` with why it can't;
- has its connections' credentials resolved by the supervisor, over the pipe: it holds no master key;
- takes tasks only from the pool of its version (`pool_of`), and labels every task it submits with the pool of the
  version its run is tagged with (`PoolEngine`), so a task of a run started on 1.4.0 code never runs on 1.5.0 (§6);
  a plan or extract also holds an advisory lock on its concurrency key, so two versions never plan or read one
  partition at once (their pools' queues don't share keys);
- submits the plan of each queued run tagged with its version, oldest first, and again while it stays queued, since
  the plan's idempotency key is the run's and a second submit of one that exists changes nothing (`take_queued`); and
  runs the sweeper's resubmit pass for the runs of its version, which only a process with their code can do, and, while
  its version is the active one, the sealer of its push datasets, whose events only its code can parse (§7.2);
- heartbeats, retries, checkpoints and drains on SIGTERM (CTRL_BREAK_EVENT on Windows) as any worker does. A host that
  is killed hands its tasks on as a killed worker does: the supervisor starts it again under the same id, and DBOS
  takes its pending tasks back.
"""

import argparse
import logging
import os
import shutil
import signal
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any

from feather_connect.engine.port import Engine, TaskCall, TaskContext, TaskFn
from feather_connect.logs import configure_logging, install_task_logs
from feather_connect.packages.link import HostSide, LinkError
from feather_connect.project import (
    Project,
    ProjectError,
    environment_layer,
    load_project,
    prefixed,
    use_db_secrets,
    use_secret_broker,
)
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.sealer import seal_once
from feather_connect.runtime.sweeper import resubmit_lost
from feather_connect.storage.port import Run, Store, check_wheel
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.leader import KeyLocks, PostgresLeaderLock, advisory_key

__all__ = ["HOST_TAKEN", "PoolEngine", "keyed_tasks", "pool_of", "run_host", "take_queued"]

log = logging.getLogger(__name__)

TAKE_EVERY_S = 0.5  # how often the host looks for queued runs of its version
RESUBMIT_S = 30.0  # a run still queued this long after its plan was submitted is submitted again
SWEEP_EVERY_S = 15.0  # how often the host runs the sweeper's resubmit pass for its version
_TAKE_MOST = 50  # queued runs looked at each time, oldest first
HOST_TAKEN = 3  # the exit code of a host whose id another host holds


def pool_of(package: str, version: str) -> str:
    """The pool of a package version's tasks: only that version's hosts take them."""
    return f"pkg:{package}:{version}"


class PoolEngine(Engine):
    """An engine that sends each task to the pool of the package version its run is tagged with, and a task of no
    such run to `pool`. A host submits tasks of other versions' runs too: a delivery waits for the one before it on
    its route, and the apply that ends that one submits it, whichever version that apply ran on."""

    _KEPT = 10_000  # runs whose pool is remembered: a run's version never changes

    def __init__(self, engine: Engine, pool: str, store: Store | None = None) -> None:
        self._engine = engine
        self._pool = pool
        self._store = store
        self._pools: dict[str, str] = {}

    def pool_for(self, call: TaskCall) -> str:
        run_id = call.args.get("run_id")
        if self._store is None or not isinstance(run_id, str):
            return self._pool
        found = self._pools.get(run_id)
        if found is None:
            try:
                params = self._store.runs.get_run(run_id).params
            except KeyError:
                return self._pool
            package, version = params.get("package"), params.get("version")
            found = (
                pool_of(package, version)
                if isinstance(package, str) and isinstance(version, str)
                else self._pool
            )
            if len(self._pools) >= self._KEPT:
                self._pools.clear()
            self._pools[run_id] = found
        return found

    def submit(self, call: TaskCall) -> str:
        return self._engine.submit(replace(call, labels={**call.labels, "pool": self.pool_for(call)}))

    def cancel(self, ref: str) -> None:
        self._engine.cancel(ref)

    def signal(self, ref: str, key: str, payload: Mapping[str, Any]) -> None:
        self._engine.signal(ref, key, payload)

    def recover_worker(self, worker_id: str) -> list[str]:
        return self._engine.recover_worker(worker_id)

    def has_ended(self, ref: str) -> bool:
        return self._engine.has_ended(ref)

    def health(self) -> Mapping[str, Any]:
        return self._engine.health()


Hold = Callable[[str], Any]  # a context manager that holds a lock on a name


def keyed_tasks(tasks: Mapping[str, TaskFn], hold: Hold) -> dict[str, TaskFn]:
    """The tasks, with a plan holding a lock on its flow and an extract on its dataset's partition while it runs: the
    names of their concurrency keys, which the queues of one pool keep apart but not those of two."""

    def keyed(task: TaskFn, name_of: Callable[[Mapping[str, Any]], str]) -> TaskFn:
        def run(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any] | None:
            with hold(name_of(args)):
                return task(ctx, args)

        return run

    found = dict(tasks)
    if "feather.plan" in found:
        found["feather.plan"] = keyed(found["feather.plan"], lambda a: f"plan:{a['flow']}")
    if "feather.extract" in found:
        found["feather.extract"] = keyed(
            found["feather.extract"], lambda a: f"extract:{a['dataset']}:{a['partition']}"
        )
    return found


def take_queued(
    runtime: Runtime,
    package: str,
    version: str,
    submitted: dict[str, float],
    *,
    now: float | None = None,
    resubmit_s: float = RESUBMIT_S,
) -> list[str]:
    """Submit the plan of each queued run tagged with the version, oldest first, and return their ids. `submitted`
    holds when this host last submitted each one: a run still queued `resubmit_s` later is submitted again, which is
    safe, as the plan's idempotency key is the run's, and puts right a submit that a failure lost. A run of a paused
    flow is skipped, saying so."""
    store = runtime.store
    now = time.monotonic() if now is None else now
    flows = {f.name for f in runtime.flows()}
    started: list[str] = []
    queued = store.runs.queued_of(package, version, limit=_TAKE_MOST)
    for run in queued:
        last = submitted.get(run.run_id)
        if (last is not None and now - last < resubmit_s) or run.flow not in flows:
            continue
        if runtime.is_flow_paused(run.flow):
            store.runs.set_run_status(run.run_id, "skipped", "paused")
            continue
        runtime.submit_plan(run.flow, run.run_id)
        submitted[run.run_id] = now  # only once it went: one that failed is tried again on the next pass
        started.append(run.run_id)
    still = {r.run_id for r in queued}
    for run_id in [r for r in submitted if r not in still]:
        del submitted[run_id]  # out of the queue: taken
    return started


def _site(cache: Path, sha256: str, content: bytes) -> Path:
    """The wheel unpacked under the cache, by sha256, once."""
    from feather_connect.packages.check import unpack

    site = cache / "wheels" / sha256
    if (site / ".unpacked").is_file():
        return site
    work = cache / "wheels" / f".{sha256[:12]}-{uuid.uuid4().hex[:8]}"
    work.mkdir(parents=True)
    try:
        unpack(content, work)
        (work / ".unpacked").write_text(sha256, encoding="utf-8")
        try:
            work.rename(site)
        except OSError:
            if not (site / ".unpacked").is_file():
                raise
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return site


def _project(store: Store, package: str, version: str, env: str, cache: Path) -> tuple[Project, str, str]:
    """The version's project, loaded from its stored wheel, with its sha256 and normal version."""
    from feather_connect.packages.check import _find  # pyright: ignore[reportPrivateUsage]

    row = store.packages.version(package, version)
    if row is None:
        raise LookupError(f"{package} {version} is not installed")
    content = store.packages.wheel(row.sha256)
    if content is None:
        raise LookupError(f"the store has no wheel for {package} {version}")
    check_wheel(row, content)
    site = _site(cache, row.sha256, content)
    sys.path.insert(0, str(site))
    module = _find(site, package).load()
    where = getattr(module, "__file__", None)
    if not where:
        raise LookupError(f"the entry point of {package} is not a module with a file")
    project = load_project(
        Path(where).parent, env, environment=environment_layer(store.env_config.entries(env))
    )
    return project, row.sha256, row.version


def _brokered(link: HostSide) -> Callable[[str, str, str], str]:
    def resolve(connection: str, field: str, ref: str) -> str:
        try:
            answer = link.request("secret", connection=connection, field=field, ref=ref)
        except LinkError as error:
            raise ProjectError(f"secret {ref}: {error}") from None
        return str(answer["value"])

    return resolve


def run_host(
    package: str,
    version: str,
    env: str,
    *,
    database_url: str,
    worker_id: str,
    cache: Path,
    slots: int = 8,
    poll_s: float = 0.5,
    tenant: str = "default",
    stop: threading.Event | None = None,
    link: HostSide | None = None,
) -> int:
    """Run the host until told to stop, and return the exit code. With no `link` (a test in one process), the
    credentials are resolved here, from this process's environment."""
    from feather_connect.worker import check_connections, config_of, work
    from feather_engine_dbos import DbosEngine

    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"))
    told = stop or threading.Event()
    held = PostgresLeaderLock(database_url, advisory_key(tenant, f"host:{worker_id}"))
    if not held.try_acquire():
        why = f"another host runs as {worker_id}: this one doesn't start while it does"
        log.error(why)
        if link is not None:
            link.send("failed", error=why, taken=True)
        return HOST_TAKEN
    store, spool = connect(database_url, tenant=tenant)
    if link is not None:
        use_secret_broker(_brokered(link))
    else:
        from feather_connect.vault import Vault, keyring_from_env

        use_db_secrets(Vault(store.secrets, env, keyring_from_env()).open)
    remove_task_logs = install_task_logs(store)
    engine: DbosEngine | None = None
    try:
        try:
            project, sha, version = _project(store, package, version, env, cache)
            check_connections(project)
            for budget in project.budgets.values():
                store.budgets.define(budget)
            engine = DbosEngine(database_url)
        except Exception as error:
            if link is not None:
                link.send("failed", error=f"{type(error).__name__}: {error}"[:2000])
            raise
        pool = pool_of(package, version)
        runtime = Runtime(
            prefixed(project.flows, package),
            store,
            spool,
            PoolEngine(engine, pool, store),
            code_sha=sha,
            budgets={c.name: c.budget for c in project.connections.values() if c.budget is not None},
            config_of=lambda connection: config_of(project, connection),
            timezone=str(project.settings["timezone"]),
            prefix=package,
        )
        submitted: dict[str, float] = {}

        def tagged(run: Run) -> bool:
            return run.params.get("package") == package and run.params.get("version") == version

        def background(stopping: threading.Event) -> None:
            if link is not None:
                link.send("ready")  # taking work: the supervisor moves the flows' triggers here (§6)
            swept = time.monotonic()
            while not stopping.wait(TAKE_EVERY_S):
                if not held.try_acquire():  # its session went, and another host took the id meanwhile
                    log.error("host %s lost its lock on its id: it stops", worker_id)
                    told.set()
                    return
                try:
                    for run_id in take_queued(runtime, package, version, submitted):
                        log.info("took run %s of %s %s", run_id, package, version)
                    if any(a.package == package and a.version == version for a in store.packages.active()):
                        seal_once(
                            runtime, now=datetime.now(UTC)
                        )  # its push datasets: their parse is its code
                    if time.monotonic() - swept >= SWEEP_EVERY_S:
                        swept = time.monotonic()
                        resubmit_lost(store, runtime, now=datetime.now(UTC), owns=tagged)
                except Exception:  # a database that is away for a moment must not end the thread
                    log.warning("could not take queued runs of %s %s", package, version, exc_info=True)

        locks = KeyLocks(database_url, tenant)
        tasks = keyed_tasks(runtime.tasks(), locks.hold)
        labels = {"pool": pool, "package": package, "version": version}
        log.info("package host %s runs %s %s", worker_id, package, version)
        return work(
            runtime, store, database_url, worker_id, labels, slots, poll_s, told, sha, background, tasks
        )
    finally:
        use_secret_broker(None)
        remove_task_logs()
        if engine is not None:
            engine.close()
        store.close()
        held.release()


def _die_with_parent(parent: int | None) -> None:
    """On Linux, have the kernel send SIGTERM when the supervisor dies, and stop now if it died already."""
    if sys.platform == "linux":
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        pr_set_pdeathsig = 1
        libc.prctl(pr_set_pdeathsig, signal.SIGTERM, 0, 0, 0)
        if parent is not None and os.getppid() != parent:
            raise SystemExit("the supervisor that started this host is gone")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m feather_connect.host", description=__doc__)
    parser.add_argument("package")
    parser.add_argument("version")
    parser.add_argument("env")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--tenant", default="default")
    parser.add_argument("--poll-s", type=float, default=float(os.environ.get("FEATHER_ENGINE_POLL_S", "0.5")))
    parser.add_argument("--no-link", action="store_true", help="run with no supervisor pipe (tests)")
    args = parser.parse_args(argv)
    url = os.environ.get("FEATHER_DATABASE_URL")
    if not url:
        parser.error("set FEATHER_DATABASE_URL")
    parent = os.environ.get("FEATHER_SUPERVISOR_PID")
    _die_with_parent(int(parent) if parent and parent.isdigit() else None)
    stop = threading.Event()

    def on_signal(signum: int, frame: FrameType | None) -> None:
        stop.set()

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):  # SIGBREAK is CTRL_BREAK_EVENT, on Windows only
        if (number := getattr(signal, name, None)) is not None:
            signal.signal(number, on_signal)
    link = None if args.no_link else HostSide.attach(on_lost=stop.set)
    return run_host(
        args.package,
        args.version,
        args.env,
        database_url=url,
        worker_id=args.worker_id,
        cache=args.cache,
        slots=args.slots,
        poll_s=args.poll_s,
        tenant=args.tenant,
        stop=stop,
        link=link,
    )


if __name__ == "__main__":
    raise SystemExit(main())

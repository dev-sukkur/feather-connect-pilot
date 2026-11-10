"""A package host (flow packages §7.1): the worker loop of M1, running one installed package version's flows.

    python -m feather_connect.host PACKAGE VERSION ENV --worker-id ID --cache DIR

The worker supervisor (`feather_connect.supervisor`) starts one per active package version, with the Python of that
version's dependency environment, and `FEATHER_DATABASE_URL` set. The host:

- loads the version's stored wheel, its sha256 checked again (§11), from the database, never from a registry;
- loads its flows with the package prefix, for the environment, as the load check did;
- takes tasks only from the pool of its version (`pool_of`), and labels every task it submits with the pool of the
  version its run is tagged with (`PoolEngine`), so a task of a run started on 1.4.0 code never runs on 1.5.0 (§6);
- takes the queued runs that the scheduler wrote for its version (`params.package` and `params.version`) and submits
  each one's plan, once: the plan's idempotency key is the run's;
- heartbeats, retries, checkpoints and drains on SIGTERM (CTRL_BREAK_EVENT on Windows) as any worker does. A host that
  is killed hands its tasks on as a killed worker does: the supervisor starts it again under the same id, and DBOS
  takes its pending tasks back.
"""

import argparse
import logging
import os
import shutil
import sys
import threading
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from feather_connect.engine.port import Engine, TaskCall
from feather_connect.logs import configure_logging, install_task_logs
from feather_connect.project import Project, environment_layer, load_project, prefixed, use_db_secrets
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store, check_wheel
from feather_connect.storage.postgres.connect import connect
from feather_connect.vault import Vault, keyring_from_env

__all__ = ["PoolEngine", "pool_of", "run_host", "take_queued"]

log = logging.getLogger(__name__)

TAKE_EVERY_S = 0.5  # how often the host looks for queued runs of its version
_TAKE_MOST = 50  # queued runs of a flow looked at each time


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


def take_queued(runtime: Runtime, package: str, version: str, taken: set[str]) -> list[str]:
    """Submit the plan of every queued run of the version's flows that has none yet, and return their ids. A run of
    a paused flow is skipped, saying so. `taken` remembers the runs this host submitted."""
    store = runtime.store
    started: list[str] = []
    for flow in runtime.flows():
        for run in store.runs.list_runs(flow=flow.name, status="queued", limit=_TAKE_MOST):
            params = run.params
            if run.run_id in taken or params.get("package") != package or params.get("version") != version:
                continue
            taken.add(run.run_id)
            if store.runs.list_tasks(run.run_id, kinds=("plan",), limit=1):
                continue  # another host of the version took it
            if runtime.is_flow_paused(flow.name):
                store.runs.set_run_status(run.run_id, "skipped", "paused")
                continue
            runtime.submit_plan(flow.name, run.run_id)
            started.append(run.run_id)
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
    stop: threading.Event | None = None,
) -> int:
    """Run the host until told to stop, and return the exit code."""
    from feather_connect.worker import _check_connections, _work, config_of  # pyright: ignore[reportPrivateUsage]
    from feather_engine_dbos import DbosEngine

    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"))
    store, spool = connect(database_url)
    use_db_secrets(Vault(store.secrets, env, keyring_from_env()).open)
    remove_task_logs = install_task_logs(store)
    engine: DbosEngine | None = None
    try:
        project, sha, version = _project(store, package, version, env, cache)
        _check_connections(project)
        for budget in project.budgets.values():
            store.budgets.define(budget)
        engine = DbosEngine(database_url)
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
        )
        taken: set[str] = set()

        def take(stopping: threading.Event) -> None:
            while not stopping.wait(TAKE_EVERY_S):
                try:
                    for run_id in take_queued(runtime, package, version, taken):
                        log.info("took run %s of %s %s", run_id, package, version)
                except Exception:  # a database that is away for a moment must not end the thread
                    log.warning("could not take queued runs of %s %s", package, version, exc_info=True)

        labels = {"pool": pool, "package": package, "version": version}
        log.info("package host %s runs %s %s", worker_id, package, version)
        return _work(runtime, store, database_url, worker_id, labels, slots, poll_s, stop, sha, take)
    finally:
        remove_task_logs()
        if engine is not None:
            engine.close()
        store.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m feather_connect.host", description=__doc__)
    parser.add_argument("package")
    parser.add_argument("version")
    parser.add_argument("env")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--poll-s", type=float, default=float(os.environ.get("FEATHER_ENGINE_POLL_S", "0.5")))
    args = parser.parse_args(argv)
    url = os.environ.get("FEATHER_DATABASE_URL")
    if not url:
        parser.error("set FEATHER_DATABASE_URL")
    return run_host(
        args.package,
        args.version,
        args.env,
        database_url=url,
        worker_id=args.worker_id,
        cache=args.cache,
        slots=args.slots,
        poll_s=args.poll_s,
    )


if __name__ == "__main__":
    raise SystemExit(main())

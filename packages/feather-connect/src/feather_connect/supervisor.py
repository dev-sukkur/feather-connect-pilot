"""The worker supervisor (flow packages §7.1): it imports no flow code, and runs one package host per package version
that has work here.

    python -m feather_connect.supervisor ENV --database-url postgresql://... [--packages 'acme-*']

It wakes on NOTIFY when the catalogue changes, and every `POLL_S` seconds anyway, and then:

- **prepares** installed versions (on a thread of its own, one at a time): builds the version's dependency environment
  and load-checks it there, and records it `ready` or `failed` (`packages.prepare`);
- **starts a host** for each version that has an active flow, or queued or running runs tagged with it, in that
  version's environment (`feather_connect.host`), labelled with the version's pool;
- **drains a host** whose version has neither: SIGTERM (CTRL_BREAK_EVENT on Windows), and the host stops taking work,
  waits for its running tasks and exits. One that hasn't gone after `KILL_AFTER_S` is killed with its process tree;
- **restarts a host that died** while its version still has work, under the same id, after a back-off that doubles
  up to `BACKOFF_MAX_S`, so that DBOS gives it back its pending tasks. A host that dies `CRASH_LOOP` times in
  `CRASH_WINDOW_S` is logged as in a crash loop, and is still restarted at the longest back-off.

A version switch is these steps together: the new version's flows are active, so its host starts; the old version's
host keeps running while runs tagged with it are queued or running, so they finish on the code they started with,
and is drained when they are over. No run moves between versions, so none is lost or run twice.

Each host is a process tree of its own (a Job Object on Windows), killed with the supervisor's when it ends.
"""

import argparse
import fnmatch
import logging
import os
import signal
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import IO

from feather_connect.clock import Clock, SystemClock
from feather_connect.logs import configure_logging
from feather_connect.packages.check import load_check
from feather_connect.packages.envs import BUILD_TIMEOUT, MAX_BYTES, EnvError, Environments
from feather_connect.packages.prepare import Checker, lock_of, prepare_version
from feather_connect.packages.tree import ProcessTree
from feather_connect.storage.port import Store

__all__ = ["POLL_S", "Supervisor", "run_supervisor"]

log = logging.getLogger(__name__)

POLL_S = 2.0  # the poll that backs NOTIFY up (§7.1)
KILL_AFTER_S = 90.0  # a draining host that hasn't exited by then is killed: its drain waits 30 s for tasks
BACKOFF_MAX_S = 60.0
CRASH_LOOP = 5
CRASH_WINDOW_S = 300.0
_RUNS_LOOKED_AT = 500  # queued and running runs read each step, to find the versions that have work


@dataclass
class _Host:
    package: str
    version: str
    worker_id: str
    tree: ProcessTree
    started_at: float
    stopping_since: float | None = None


@dataclass
class _Crashes:
    times: list[float] = field(default_factory=list[float])
    next_start: float = 0.0


Spawn = Callable[[list[str], dict[str, str], str], ProcessTree]


class Supervisor:
    """Runs the package hosts of one environment on this worker. `packages` are the glob patterns of the packages it
    runs (all by default). `spawn` starts a host's process from its command, environment and id, for tests."""

    def __init__(
        self,
        store: Store,
        *,
        env: str,
        worker_id: str,
        database_url: str,
        envs: Environments,
        cache: Path,
        packages: Sequence[str] = ("*",),
        slots: int = 8,
        poll_s: float = 0.5,
        clock: Clock | None = None,
        checker: Checker = load_check,
        log_dir: Path | None = None,
        spawn: Spawn | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._env = env
        self._worker_id = worker_id
        self._url = database_url
        self._envs = envs
        self._cache = cache
        self._patterns = list(packages) or ["*"]
        self._slots = slots
        self._poll_s = poll_s
        self._clock: Clock = clock or SystemClock()
        self._checker = checker
        self._log_dir = log_dir
        self._spawn = spawn or self._start_process
        self._monotonic = monotonic
        self._hosts: dict[tuple[str, str], _Host] = {}
        self._crashes: dict[tuple[str, str], _Crashes] = {}
        self._lock = threading.Lock()

    def _ours(self, package: str) -> bool:
        return any(fnmatch.fnmatchcase(package, p) for p in self._patterns)

    # Preparing.

    def prepare_pending(self) -> list[str]:
        """Prepare every installed version not prepared yet, and return their names."""
        done: list[str] = []
        for row in self._store.packages.versions():
            if row.state not in ("installed", "preparing") or not self._ours(row.name):
                continue
            log.info("preparing %s %s", row.name, row.version)
            try:
                prepare_version(
                    self._store,
                    row,
                    env=self._env,
                    envs=self._envs,
                    worker_id=self._worker_id,
                    clock=self._clock,
                    checker=self._checker,
                )
            except Exception:
                log.exception("could not prepare %s %s", row.name, row.version)
                continue
            done.append(f"{row.name} {row.version}")
        return done

    # Hosts.

    def wanted(self) -> set[tuple[str, str]]:
        """The versions that need a host here: those with an active flow, and those with queued or running runs."""
        found = {(a.package, a.version) for a in self._store.packages.active()}
        for status in ("queued", "running"):
            for run in self._store.runs.list_runs(status=status, limit=_RUNS_LOOKED_AT):
                package, version = run.params.get("package"), run.params.get("version")
                if isinstance(package, str) and isinstance(version, str):
                    found.add((package, version))
        ready: set[tuple[str, str]] = set()
        for package, version in found:
            if not self._ours(package):
                continue
            row = self._store.packages.version(package, version)
            if row is not None and row.state == "ready":
                ready.add((package, row.version))
        return ready

    def hosts(self) -> dict[tuple[str, str], str]:
        """The hosts running, by version, with their worker ids."""
        with self._lock:
            return {
                key: host.worker_id for key, host in self._hosts.items() if host.tree.process.poll() is None
            }

    def step(self) -> None:
        """Bring the hosts in line with the work there is."""
        wanted = self.wanted()
        now = self._monotonic()
        with self._lock:
            for key, host in list(self._hosts.items()):
                if host.tree.process.poll() is not None:  # it ended: drained, or died
                    del self._hosts[key]
                    host.tree.kill()  # and whatever it started
                    if host.stopping_since is None:
                        self._crashed(key, host, now)
                elif key not in wanted and host.stopping_since is None:
                    log.info("draining the host of %s %s: it has no work", *key)
                    host.stopping_since = now
                    host.tree.terminate()
                elif host.stopping_since is not None and now - host.stopping_since > KILL_AFTER_S:
                    log.warning("killing the host of %s %s: it didn't drain in %g s", *key, KILL_AFTER_S)
                    host.tree.kill()
            for key in sorted(wanted - self._hosts.keys()):
                crashes = self._crashes.get(key)
                if crashes is not None and now < crashes.next_start:
                    continue
                try:
                    self._start(key, now)
                except Exception:
                    log.exception("could not start the host of %s %s", *key)
                    self._crashed(key, None, now)

    def _crashed(self, key: tuple[str, str], host: _Host | None, now: float) -> None:
        crashes = self._crashes.setdefault(key, _Crashes())
        crashes.times = [t for t in crashes.times if now - t < CRASH_WINDOW_S] + [now]
        delay = min(BACKOFF_MAX_S, 2.0 ** (len(crashes.times) - 1))
        crashes.next_start = now + delay
        code = None if host is None else host.tree.process.returncode
        if len(crashes.times) >= CRASH_LOOP:
            log.error(
                "the host of %s %s is in a crash loop: %d ends in %g s",
                *key,
                len(crashes.times),
                CRASH_WINDOW_S,
            )
        else:
            log.warning("the host of %s %s ended (exit %s): starting it again in %g s", *key, code, delay)

    def _start(self, key: tuple[str, str], now: float) -> None:
        package, version = key
        row = self._store.packages.version(package, version)
        content = None if row is None else self._store.packages.wheel(row.sha256)
        if row is None or content is None:
            raise LookupError(f"{package} {version} has no stored wheel")
        in_use = {h for h in map(self._lock_hash, self._hosts) if h is not None}
        try:
            python = self._envs.ensure(lock_of(content, row), row.lock_hash, keep=in_use)
        except EnvError as error:
            self._store.packages.record_env(
                self._worker_id, row.lock_hash, "failed", self._clock.now(), str(error)
            )
            raise
        self._store.packages.record_env(self._worker_id, row.lock_hash, "ready", self._clock.now())
        host_id = f"{self._worker_id}/{package}@{row.version}"
        command = [
            str(python),
            "-m",
            "feather_connect.host",
            package,
            row.version,
            self._env,
            "--worker-id",
            host_id,
            "--cache",
            str(self._cache),
            "--slots",
            str(self._slots),
            "--poll-s",
            str(self._poll_s),
        ]
        environ = {**os.environ, "FEATHER_DATABASE_URL": self._url}
        environ.pop("VIRTUAL_ENV", None)
        tree = self._spawn(command, environ, host_id)
        self._hosts[key] = _Host(package, row.version, host_id, tree, now)
        log.info("started the host %s", host_id)

    def _lock_hash(self, key: tuple[str, str]) -> str | None:
        row = self._store.packages.version(*key)
        return None if row is None else row.lock_hash

    def _start_process(self, command: list[str], environ: dict[str, str], host_id: str) -> ProcessTree:
        out: IO[bytes] | int | None = None
        if self._log_dir is not None:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            out = (self._log_dir / f"{host_id.replace('/', '_').replace(':', '_')}.log").open("ab")
        try:
            return ProcessTree(
                command, env=environ, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT
            )
        finally:
            if out is not None and not isinstance(out, int):
                out.close()  # the child has its own handle

    def stop_all(self, wait_s: float = KILL_AFTER_S) -> None:
        """Drain every host, and kill those still running after `wait_s`."""
        with self._lock:
            hosts = list(self._hosts.values())
            self._hosts.clear()
        for host in hosts:
            host.tree.terminate()
        deadline = self._monotonic() + wait_s
        for host in hosts:
            try:
                host.tree.process.wait(timeout=max(0.0, deadline - self._monotonic()))
            except subprocess.TimeoutExpired:
                log.warning("killing the host %s: it didn't drain", host.worker_id)
            host.tree.kill()

    def run(self, stop: threading.Event) -> int:
        """Prepare versions and run hosts until `stop` is set, then drain the hosts."""
        wake = threading.Event()
        unlisten = self._store.packages.listen(wake.set)

        def preparing() -> None:
            while not stop.is_set():
                try:
                    self.prepare_pending()
                except Exception:
                    log.exception("preparing versions failed")
                wake.wait(POLL_S)

        preparer = threading.Thread(target=preparing, name="prepare", daemon=True)
        preparer.start()
        try:
            while not stop.is_set():
                try:
                    self.step()
                except Exception:  # a database that is away for a moment must not end the supervisor
                    log.warning("the supervisor's step failed", exc_info=True)
                wake.wait(POLL_S)
                wake.clear()
        finally:
            unlisten()
            wake.set()  # the preparer sees `stop` now, or once the version it prepares is done
            self.stop_all()
            preparer.join(timeout=BUILD_TIMEOUT)
        return 0


def _index_options() -> list[str]:
    """Where environments' requirements come from, as the worker's settings say."""
    found: list[str] = []
    if url := os.environ.get("FEATHER_PACKAGE_INDEX_URL"):
        found += ["--index-url", url]
    for links in filter(None, os.environ.get("FEATHER_PACKAGE_FIND_LINKS", "").split(os.pathsep)):
        found += ["--find-links", links]
    if os.environ.get("FEATHER_PACKAGE_NO_INDEX") == "1":
        found.append("--no-index")
    return found


def run_supervisor(
    env: str,
    *,
    database_url: str,
    worker_id: str | None = None,
    packages: Sequence[str] = ("*",),
    cache: Path | None = None,
    slots: int = 8,
    poll_s: float = 0.5,
    tenant: str = "default",
    stop: threading.Event | None = None,
) -> int:
    """Run a supervisor until told to stop (SIGTERM, SIGINT or SIGBREAK, or `stop`), and return the exit code."""
    from feather_connect.storage.postgres.connect import connect
    from feather_connect.storage.postgres.migrate import migrate

    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"))
    worker_id = worker_id or os.environ.get("FEATHER_WORKER_ID") or socket.gethostname()
    cache = cache or Path(
        os.environ.get("FEATHER_PACKAGE_CACHE") or Path.home() / ".cache" / "feather" / "packages"
    )
    migrate(database_url)
    store, _ = connect(database_url, tenant=tenant)
    envs = Environments(
        cache / "envs",
        tenant=tenant,
        index=_index_options(),
        timeout=float(os.environ.get("FEATHER_ENV_BUILD_TIMEOUT_S", BUILD_TIMEOUT)),
        max_bytes=int(float(os.environ.get("FEATHER_ENV_CACHE_MAX_GB", MAX_BYTES / 1024**3)) * 1024**3),
    )
    told = threading.Event() if stop is None else stop
    if stop is None:

        def on_signal(signum: int, frame: FrameType | None) -> None:
            told.set()

        for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
            if (number := getattr(signal, name, None)) is not None:
                signal.signal(number, on_signal)
    supervisor = Supervisor(
        store,
        env=env,
        worker_id=worker_id,
        database_url=database_url,
        envs=envs,
        cache=cache / tenant,
        packages=packages,
        slots=slots,
        poll_s=poll_s,
        log_dir=Path(d) if (d := os.environ.get("FEATHER_HOST_LOG_DIR")) else None,
    )
    log.info("supervisor %s runs the packages %s of %s", worker_id, ", ".join(packages), env)
    try:
        return supervisor.run(told)
    finally:
        store.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m feather_connect.supervisor", description=__doc__)
    parser.add_argument("env")
    parser.add_argument("--database-url", default=os.environ.get("FEATHER_DATABASE_URL"))
    parser.add_argument("--worker-id")
    parser.add_argument("--packages", action="append", default=[], metavar="GLOB")
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--poll-s", type=float, default=float(os.environ.get("FEATHER_ENGINE_POLL_S", "0.5")))
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("give --database-url or set FEATHER_DATABASE_URL")
    return run_supervisor(
        args.env,
        database_url=args.database_url,
        worker_id=args.worker_id,
        packages=args.packages or ["*"],
        cache=args.cache,
        slots=args.slots,
        poll_s=args.poll_s,
    )


if __name__ == "__main__":
    raise SystemExit(main())

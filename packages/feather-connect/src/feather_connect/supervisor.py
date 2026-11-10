"""The worker supervisor (flow packages §7.1): it imports no flow code, and runs one package host per package version
that has work here.

    python -m feather_connect.supervisor ENV --database-url postgresql://... [--packages 'acme-*']

It wakes on NOTIFY when the catalogue changes, and every `POLL_S` seconds anyway, and then:

- **prepares** installed versions (on a thread of its own, one at a time): builds the version's dependency environment
  and load-checks it there, and records it `ready` or `failed` (`packages.prepare`). An environment that can't be
  built, which may be the network, is tried again after a back-off that doubles up to `PREPARE_BACKOFF_MAX_S`;
- **builds environments** of versions that need a host, on a thread per lock hash, so a long build holds up nothing;
- **starts a host** for each version that has an active flow, a flow activating, or queued or running runs tagged with
  it, in that version's environment (`feather_connect.host`), labelled with the version's pool. A host says `ready` once
  it takes work, and only then are the flows activating moved to it (`promote`): the triggers stay on the version active
  before until then, and if the host can't load, the activation is marked failed and they stay there (§6);
- **drains a host** whose version has none of these: SIGTERM (CTRL_BREAK_EVENT on Windows), and the host stops taking
  work, waits for its running tasks and exits. One that hasn't gone after `KILL_AFTER_S` is killed with its process
  tree. A version wanted again while its host drains (a rollback) gets a new host as soon as that one is gone;
- **restarts a host that died** while its version still has work, under the same id, after a back-off that doubles
  up to `BACKOFF_MAX_S`, so that DBOS gives it back its pending tasks. A host that dies `CRASH_LOOP` times in
  `CRASH_WINDOW_S` is recorded in a crash loop (`record_host`), and is still restarted at the longest back-off.

A version switch is these steps together: the new version's host starts and says ready, its flows' triggers move to
it; the old version's host keeps running while runs tagged with it are queued or running, so they finish on the code
they started with, and is drained when they are over. No run moves between versions, so none is lost or run twice.

What a host gets (§11): an environment with no secret of the worker's (no master key, no session key; only what
`host_environment` lets through, and `FEATHER_DATABASE_URL` set to the host role's URL where one is set up), and a pipe
to this process (`packages.link`) on which it asks for the credentials of its version's own connections, which this
process resolves with the environment's vault and checks against the version's manifest and configuration. A host is
a process tree of its own (a Job Object on Windows), killed with the supervisor's when it ends; on Linux it is also
sent SIGTERM when this process dies (`PR_SET_PDEATHSIG`), and on every system it stops when the pipe closes.
"""

import argparse
import fnmatch
import logging
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import IO, Any, cast

from feather_connect.clock import Clock, SystemClock
from feather_connect.logs import configure_logging
from feather_connect.packages import manifest as m
from feather_connect.packages.check import load_check, unpack
from feather_connect.packages.envs import BUILD_TIMEOUT, MAX_BYTES, EnvError, Environments, EnvUse
from feather_connect.packages.link import LinkError, SupervisorSide
from feather_connect.packages.prepare import Checker, lock_of, prepare_version
from feather_connect.packages.tree import ProcessTree
from feather_connect.project import (
    RESERVED_VARIABLES,
    ConnectionConfig,
    ProjectError,
    check_binding,
    connection_configs,
    environment_layer,
    resolve_secret,
)
from feather_connect.runtime.catalogue import sync_package_catalog
from feather_connect.storage.port import PackageVersion, Store

__all__ = ["POLL_S", "Supervisor", "host_environment", "run_supervisor"]

log = logging.getLogger(__name__)

SUPERVISOR_ROLE = "supervisor"  # the label a supervisor registers with; its package hosts have a `pool`

POLL_S = 2.0  # the poll that backs NOTIFY up (§7.1)
KILL_AFTER_S = 90.0  # a draining host that hasn't exited by then is killed: its drain waits 30 s for tasks
BACKOFF_MAX_S = 60.0
TAKEN_RETRY_S = 1.0  # a host whose id another host still holds is started again this soon
CRASH_LOOP = 5
CRASH_WINDOW_S = 300.0
SWEEP_ENVS_S = 600.0  # how often environments no installed version needs are removed
PARTITIONS_EVERY_S = 600.0  # how often the partitions hosts write to are made ahead
PREPARE_BACKOFF_S = 5.0
PREPARE_BACKOFF_MAX_S = 600.0
_CONFIG_KEPT_S = (
    5.0  # how long a version's connections, as configured, are reused to check credential requests
)
_LOG_TAIL = 800  # characters of a host's log kept as the error it died of
_SECRET = re.compile(r"^\$\{secret:([^:{}]+):(.+)\}$")
# What a host gets of the supervisor's environment, besides what `pass_env` names: what Python and its libraries need
# to run, and no secret. `FEATHER_DATABASE_URL` is set to the host's own.
_HOST_KEPT = (
    "PATH",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "SYSTEMDRIVE",
    "PROGRAMDATA",
    "LANG",
    "LC_ALL",
    "TZ",
    "HOME",
    "USERPROFILE",
    "LOCALAPPDATA",
    "APPDATA",
    "TEMP",
    "TMP",
    "TMPDIR",
    "FEATHER_LOG_LEVEL",
    "FEATHER_ENGINE_POLL_S",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
)


def _reserved(name: str) -> bool:
    upper = name.upper()
    return upper in RESERVED_VARIABLES or upper.startswith(("FEATHER_MASTER_KEY", "FEATHER_SECRET_KEY"))


def host_environment(database_url: str, pass_env: Sequence[str] = ()) -> dict[str, str]:
    """The environment a package host runs with: `_HOST_KEPT` and the variables `pass_env` names, never one that
    holds the master key or the server's session key, and `FEATHER_DATABASE_URL` set to `database_url`."""
    names = {n.upper() for n in (*_HOST_KEPT, *pass_env)}
    kept = {k: v for k, v in os.environ.items() if k.upper() in names and not _reserved(k)}
    return kept | {
        "FEATHER_DATABASE_URL": database_url,
        "FEATHER_SUPERVISOR_PID": str(os.getpid()),
        "PYTHONIOENCODING": "utf-8",
    }


@dataclass
class _Host:
    package: str
    version: str
    worker_id: str
    tree: ProcessTree
    link: SupervisorSide
    env: EnvUse
    started_at: float
    stopping_since: float | None = None
    ready: bool = False
    error: str | None = None
    taken: bool = False


@dataclass
class _Crashes:
    times: list[float] = field(default_factory=list[float])
    next_start: float = 0.0


@dataclass
class _Configured:
    at: float
    connections: dict[str, ConnectionConfig]


Spawn = Callable[[list[str], dict[str, str], str], ProcessTree]


class Supervisor:
    """Runs the package hosts of one environment on this worker. `packages` are the glob patterns of the packages it
    runs (all by default). `host_database_url` is what hosts connect with (the database's host role; by default
    `database_url`), and `pass_env` names variables of this process that hosts get besides the ones they always do.
    `spawn` starts a host's process from its command, environment and id, for tests; it must give the process its
    stdin and stdout as pipes. `workdir` is for `feather dev`: hosts run in it, and a relative `${secret:file:…}` is
    read against it, so the project's own relative paths mean the project folder."""

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
        tenant: str = "default",
        clock: Clock | None = None,
        checker: Checker = load_check,
        log_dir: Path | None = None,
        spawn: Spawn | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        host_database_url: str | None = None,
        pass_env: Sequence[str] = (),
        db_secrets: Callable[[str], str] | None = None,
        partitions: Callable[[], None] | None = None,
        workdir: Path | None = None,
    ) -> None:
        self._store = store
        self._workdir = workdir
        self._env = env
        self._worker_id = worker_id
        self._url = database_url
        self._host_url = host_database_url or database_url
        self._pass_env = [n for n in pass_env if not _reserved(n)]
        self._envs = envs
        self._cache = cache
        self._patterns = list(packages) or ["*"]
        self._slots = slots
        self._poll_s = poll_s
        self._tenant = tenant
        self._clock: Clock = clock or SystemClock()
        self._checker = checker
        self._log_dir = log_dir
        self._spawn = spawn or self._start_process
        self._monotonic = monotonic
        self._db_secrets = db_secrets
        self._partitions = partitions
        self._partitions_at: float | None = None
        self._swept_at: float | None = None
        self._hosts: dict[tuple[str, str], _Host] = {}
        self._crashes: dict[tuple[str, str], _Crashes] = {}
        self._built: dict[tuple[str, str], EnvUse] = {}  # environments built for a host, held until it starts
        self._building: dict[str, threading.Thread] = {}  # by lock hash
        self._build_failed: dict[tuple[str, str], str] = {}
        self._prepare_after: dict[tuple[str, str], tuple[float, int]] = {}
        self._configured: dict[tuple[str, str], _Configured] = {}
        self._lock = threading.Lock()

    def _ours(self, package: str) -> bool:
        return any(fnmatch.fnmatchcase(package, p) for p in self._patterns)

    def _in_use(self) -> set[str]:
        with self._lock:
            keys = [*self._hosts, *self._built]
        found: set[str] = set()
        for key in keys:
            row = self._store.packages.version(*key)
            if row is not None:
                found.add(row.lock_hash)
        return found

    # Preparing.

    def prepare_pending(self) -> list[str]:
        """Prepare every installed version not prepared yet, and return their names. One whose environment can't be
        built stays `preparing`, and is tried again after a back-off."""
        done: list[str] = []
        now = self._monotonic()
        for row in self._store.packages.versions():
            key = (row.name, row.version)
            if row.state not in ("installed", "preparing") or not self._ours(row.name):
                continue
            after, tries = self._prepare_after.get(key, (0.0, 0))
            if now < after:
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
                    keep=self._in_use(),
                )
            except EnvError as error:
                delay = min(PREPARE_BACKOFF_MAX_S, PREPARE_BACKOFF_S * 2.0**tries)
                self._prepare_after[key] = (now + delay, tries + 1)
                log.warning(
                    "the environment of %s %s can't be built now, trying again in %g s: %s",
                    row.name,
                    row.version,
                    delay,
                    error,
                )
                continue
            except Exception:
                log.exception("could not prepare %s %s", row.name, row.version)
                continue
            self._prepare_after.pop(key, None)
            done.append(f"{row.name} {row.version}")
        return done

    def remove_unneeded_envs(self) -> list[str]:
        """Remove the environments that no installed version needs any more, its last one uninstalled (task 10), and
        that no process holds in use. Returns the lock hashes removed."""
        needed = self._store.packages.needed_lock_hashes()  # every one: a list of versions stops at 500
        removed: list[str] = []
        for lock_hash in self._envs.built():
            if lock_hash in needed or self._envs.in_use(lock_hash):
                continue
            if self._envs.remove(lock_hash):
                log.info("removed the environment of lock %s: no installed version needs it", lock_hash[:12])
                removed.append(lock_hash)
        return removed

    # Hosts.

    def wanted(self) -> set[tuple[str, str]]:
        """The versions that need a host here: those with an active flow or one activating, and those with queued or
        running runs."""
        found = {(a.package, a.version) for a in self._store.packages.active()}
        found |= {(a.package, a.version) for a in self._store.packages.activating()}
        found |= self._store.runs.tagged_versions(("queued", "running"))
        ready: set[tuple[str, str]] = set()
        for package, version in found:
            if not self._ours(package):
                continue
            row = self._store.packages.version(package, version)
            if row is not None and row.state == "ready":
                ready.add((package, row.version))
            elif row is None or row.state == "uninstalled":
                self._fail_orphans(package, version)
        return ready

    def _fail_orphans(self, package: str, version: str) -> None:
        """Fail the queued runs tagged with a version that isn't installed (any more): no host will ever take them.
        One can be written after an uninstall by a scheduler that hadn't heard of it yet."""
        for run in self._store.runs.queued_of(package, version, limit=100):
            why = f"{package} {version} is not installed here (uninstalled): no host can run it"
            log.warning("failing run %s: %s", run.run_id, why)
            self._store.runs.set_run_status(run.run_id, "failed", why)

    def hosts(self) -> dict[tuple[str, str], str]:
        """The hosts running, by version, with their worker ids."""
        with self._lock:
            return {
                key: host.worker_id for key, host in self._hosts.items() if host.tree.process.poll() is None
            }

    def step(self) -> None:
        """Bring the hosts in line with the work there is. Hosts are started from here, the supervisor's own thread,
        so that on Linux a host's parent-death signal is tied to the supervisor and not to a thread that ends."""
        wanted = self.wanted()
        now = self._monotonic()
        if self._partitions is not None and (
            self._partitions_at is None or now - self._partitions_at > PARTITIONS_EVERY_S
        ):
            self._partitions()  # before a host needs one: a host role can't make them
            self._partitions_at = now
        if self._swept_at is None or now - self._swept_at > SWEEP_ENVS_S:
            self._swept_at = now
            self.remove_unneeded_envs()
        with self._lock:
            for key, host in list(self._hosts.items()):
                if host.tree.process.poll() is not None:  # it ended: drained, or died
                    del self._hosts[key]
                    self._ended(key, host, now, key in wanted)
                elif key not in wanted and host.stopping_since is None:
                    log.info("draining the host of %s %s: it has no work", *key)
                    host.stopping_since = now
                    host.tree.terminate()
                elif host.stopping_since is not None and now - host.stopping_since > KILL_AFTER_S:
                    log.warning("killing the host of %s %s: it didn't drain in %g s", *key, KILL_AFTER_S)
                    host.tree.kill()
            for key in [k for k in self._built if k not in wanted]:
                self._built.pop(key).release()
            for key in sorted(wanted - self._hosts.keys()):
                crashes = self._crashes.get(key)
                if crashes is not None and now < crashes.next_start:
                    continue
                found = self._built.pop(key, None)
                if found is None:
                    self._build_for(key)
                    continue
                try:
                    self._start(key, found, now)
                except Exception as error:
                    found.release()
                    log.exception("could not start the host of %s %s", *key)
                    self._crashed(key, None, now, str(error))

    def _ended(self, key: tuple[str, str], host: _Host, now: float, wanted: bool) -> None:
        host.tree.kill()  # and whatever it started
        host.link.close()
        host.env.release()
        code = host.tree.process.returncode
        if host.stopping_since is not None:
            self._record(key, "stopped", 0, None)
            self._crashes.pop(key, None)  # wanted again (a rollback): it starts at once
            return
        if host.taken:  # its id is still held by the host it replaces, which is draining
            self._crashes.setdefault(key, _Crashes()).next_start = now + TAKEN_RETRY_S
            return
        error = host.error or f"it exited with code {code}" + self._tail(host.worker_id)
        if not host.ready:
            failed = self._store.packages.activation_failed(*key, error, self._clock.now())
            if failed:
                log.error(
                    "the host of %s %s didn't come up, so %s stay on the version active before: %s",
                    *key,
                    ", ".join(a.flow for a in failed),
                    error,
                )
        self._crashed(key, host, now, error)

    def _crashed(self, key: tuple[str, str], host: _Host | None, now: float, error: str) -> None:
        crashes = self._crashes.setdefault(key, _Crashes())
        crashes.times = [t for t in crashes.times if now - t < CRASH_WINDOW_S] + [now]
        delay = min(BACKOFF_MAX_S, 2.0 ** (len(crashes.times) - 1))
        crashes.next_start = now + delay
        looping = len(crashes.times) >= CRASH_LOOP
        self._record(key, "crash_loop" if looping else "failed", len(crashes.times), error)
        if looping:
            log.error(
                "the host of %s %s is in a crash loop: %d ends in %g s",
                *key,
                len(crashes.times),
                CRASH_WINDOW_S,
            )
        else:
            log.warning("the host of %s %s ended: starting it again in %g s: %s", *key, delay, error)

    def _record(self, key: tuple[str, str], state: Any, restarts: int, error: str | None) -> None:
        try:
            self._store.packages.record_host(
                self._worker_id, *key, state, restarts, self._clock.now(), error and error[:2000]
            )
        except Exception:
            log.warning("could not record the host of %s %s", *key, exc_info=True)

    def _tail(self, host_id: str) -> str:
        path = self._log_path(host_id)
        if path is None or not path.is_file():
            return ""
        with path.open("rb") as file:
            file.seek(max(0, path.stat().st_size - _LOG_TAIL))
            text = file.read().decode("utf-8", "replace").strip()
        return f": {text}" if text else ""

    def _build_for(self, key: tuple[str, str]) -> None:
        """Build the version's environment on a thread of its own, one per lock hash, and keep it for `step`."""
        row = self._store.packages.version(*key)
        if row is None or row.lock_hash in self._building:
            return

        def build(row: PackageVersion = row) -> None:
            try:
                content = self._store.packages.wheel(row.sha256)
                if content is None:
                    raise EnvError(f"{row.name} {row.version} has no stored wheel")
                self._store.packages.record_env(self._worker_id, row.lock_hash, "building", self._clock.now())
                use = self._envs.use(lock_of(content, row), row.lock_hash, keep=self._in_use())
            except (EnvError, ValueError) as error:
                self._store.packages.record_env(
                    self._worker_id, row.lock_hash, "failed", self._clock.now(), str(error)
                )
                with self._lock:
                    self._crashed(key, None, self._monotonic(), f"its environment: {error}")
            except Exception as error:
                log.exception("building the environment of %s %s failed", *key)
                with self._lock:
                    self._crashed(key, None, self._monotonic(), f"its environment: {error}")
            else:
                self._store.packages.record_env(self._worker_id, row.lock_hash, "ready", self._clock.now())
                with self._lock:
                    old = self._built.pop(key, None)
                    self._built[key] = use
                if old is not None:
                    old.release()
            finally:
                with self._lock:
                    self._building.pop(row.lock_hash, None)

        thread = threading.Thread(target=build, name=f"env-{row.lock_hash[:12]}", daemon=True)
        self._building[row.lock_hash] = thread
        thread.start()

    def _start(self, key: tuple[str, str], env: EnvUse, now: float) -> None:
        package, version = key
        host_id = f"{self._worker_id}/{package}@{version}"
        command = [
            str(env.python),
            "-m",
            "feather_connect.host",
            package,
            version,
            self._env,
            "--worker-id",
            host_id,
            "--cache",
            str(self._cache),
            "--slots",
            str(self._slots),
            "--poll-s",
            str(self._poll_s),
            "--tenant",
            self._tenant,
        ]
        tree = self._spawn(command, host_environment(self._host_url, self._pass_env), host_id)
        process = tree.process
        if process.stdin is None or process.stdout is None:
            tree.kill()
            raise RuntimeError("a host's process needs its stdin and stdout as pipes")
        host: _Host

        def note(message: Mapping[str, Any]) -> None:
            if message.get("op") == "ready":
                host.ready = True
                self._ready(key)
            elif message.get("op") == "failed":
                host.error = str(message.get("error") or "")[:2000]
                host.taken = bool(message.get("taken"))

        link = SupervisorSide(
            process.stdin,
            process.stdout,
            {"secret": lambda message: self._secret(key, message)},
            note,
            host_id,
        )
        host = _Host(package, version, host_id, tree, link, env, now)
        self._hosts[key] = host
        self._record(key, "starting", len(self._crashes.get(key, _Crashes()).times), None)
        log.info("started the host %s", host_id)

    def _ready(self, key: tuple[str, str]) -> None:
        """A host of the version takes work: move the flows activating to it, and show them in the catalogue."""
        try:
            promoted = self._store.packages.promote(*key, self._clock.now())
            row = self._store.packages.version(*key)
            if promoted and row is not None:
                sync_package_catalog(self._store, row.name, row)
                log.info("%s now run on %s %s", ", ".join(a.flow for a in promoted), *key)
            restarts = len(self._crashes.get(key, _Crashes()).times)
            self._record(key, "ready", restarts, None)
        except Exception:
            log.exception("could not move the flows of %s %s to its host", *key)

    # Credentials (§11): the host asks, this process resolves.

    def _connections(self, key: tuple[str, str]) -> dict[str, ConnectionConfig]:
        """The connections of the version's manifest, as the package's files and the environment configure them."""
        now = self._monotonic()
        found = self._configured.get(key)
        if found is not None and now - found.at < _CONFIG_KEPT_S:
            return found.connections
        row = self._store.packages.version(*key)
        content = None if row is None else self._store.packages.wheel(row.sha256)
        if row is None or content is None:
            raise LinkError(f"{key[0]} {key[1]} is not installed")
        site = self._cache / "config" / row.sha256
        if not (site / ".unpacked").is_file():  # its files only: no code of it is imported here
            site.mkdir(parents=True, exist_ok=True)
            unpack(content, site)
            (site / ".unpacked").write_text(row.sha256, encoding="utf-8")
        listed = cast(list[Mapping[str, Any]], row.manifest.get("connections") or [])
        named = {str(c["name"]) for c in listed}
        configs = connection_configs(
            site / m.module_of(row.name),
            self._env,
            environment=environment_layer(self._store.env_config.entries(self._env)),
            package=row.name,
        )
        connections = {name: config for name, config in configs.items() if name in named}
        self._configured[key] = _Configured(now, connections)
        return connections

    def _secret(self, key: tuple[str, str], message: Mapping[str, Any]) -> Mapping[str, Any]:
        connection, field_, ref = (str(message.get(k) or "") for k in ("connection", "field", "ref"))
        try:
            config = self._connections(key).get(connection)
            if config is None or config.secret_refs.get(field_) != ref:
                raise LinkError(
                    f"{key[0]} {key[1]} has no connection {connection!r} whose {field_!r} is {ref}: "
                    "a host gets only its own connections' credentials"
                )
            check_binding(ref, connection, field_)
            found = _SECRET.match(ref)
            if found is not None and found.group(1) == "db":
                if self._db_secrets is None:
                    raise LinkError(f"secret {ref}: this worker has no master key to open db secrets with")
                return {"value": self._db_secrets(found.group(2))}
            return {"value": resolve_secret(ref, self._workdir or config.root)}
        except ProjectError as error:
            raise LinkError(str(error)) from None
        except LinkError:
            raise
        except Exception as error:  # noqa: BLE001 - whatever the vault raises names the secret, never its value
            raise LinkError(f"secret {ref}: {error}") from None

    def _log_path(self, host_id: str) -> Path | None:
        if self._log_dir is None:
            return None
        return self._log_dir / f"{host_id.replace('/', '_').replace(':', '_')}.log"

    def _start_process(self, command: list[str], environ: dict[str, str], host_id: str) -> ProcessTree:
        out: IO[bytes] | int | None = None
        path = self._log_path(host_id)
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            out = path.open("ab")
        try:
            return ProcessTree(
                command,
                env=environ,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=out,
                bufsize=0,
                cwd=self._workdir,
            )
        finally:
            if out is not None and not isinstance(out, int):
                out.close()  # the child has its own handle

    def stop_all(self, wait_s: float = KILL_AFTER_S) -> None:
        """Drain every host, and kill those still running after `wait_s`."""
        with self._lock:
            hosts = list(self._hosts.items())
            self._hosts.clear()
            built = list(self._built.values())
            self._built.clear()
        for use in built:
            use.release()
        for _, host in hosts:
            host.tree.terminate()
        deadline = self._monotonic() + wait_s
        for key, host in hosts:
            try:
                host.tree.process.wait(timeout=max(0.0, deadline - self._monotonic()))
            except subprocess.TimeoutExpired:
                log.warning("killing the host %s: it didn't drain", host.worker_id)
            host.tree.kill()
            host.link.close()
            host.env.release()
            self._record(key, "stopped", 0, None)

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
                wake.wait(min(POLL_S, self._poll_s * 2))
                wake.clear()
        finally:
            unlisten()
            wake.set()  # the preparer sees `stop` now, or once the version it prepares is done
            self.stop_all()
            preparer.join(timeout=BUILD_TIMEOUT)
            for thread in list(self._building.values()):
                thread.join(timeout=BUILD_TIMEOUT)
            with self._lock:
                for use in self._built.values():
                    use.release()
                self._built.clear()
        return 0


def _index_options() -> list[str]:
    """Where environments' requirements come from, as the worker's settings say. With no index set, none is used."""
    found: list[str] = []
    if url := os.environ.get("FEATHER_PACKAGE_INDEX_URL"):
        found += ["--index-url", url]
    for links in filter(None, os.environ.get("FEATHER_PACKAGE_FIND_LINKS", "").split(os.pathsep)):
        found += ["--find-links", links]
    if os.environ.get("FEATHER_PACKAGE_NO_INDEX") == "1" or not url:
        found.append("--no-index")
    return found


def _not_dumpable() -> None:
    """On Linux, keep processes of the same user, such as a package host, from reading this one's memory, where the
    master key is (`PR_SET_DUMPABLE` 0)."""
    if sys.platform == "linux":
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        pr_set_dumpable = 4
        if libc.prctl(pr_set_dumpable, 0, 0, 0, 0) != 0:
            log.warning("could not make the supervisor not dumpable (errno %d)", ctypes.get_errno())


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
    workdir: Path | None = None,
) -> int:
    """Run a supervisor until told to stop (SIGTERM, SIGINT or SIGBREAK, or `stop`), and return the exit code."""
    from feather_connect.storage.postgres.connect import connect
    from feather_connect.storage.postgres.host_role import ensure_partitions_ahead, grant_host_role, role_of
    from feather_connect.storage.postgres.migrate import migrate
    from feather_connect.vault import Vault, keyring_from_env

    configure_logging(os.environ.get("FEATHER_LOG_LEVEL", "INFO"))
    _not_dumpable()
    worker_id = worker_id or os.environ.get("FEATHER_WORKER_ID") or socket.gethostname()
    cache = cache or Path(
        os.environ.get("FEATHER_PACKAGE_CACHE") or Path.home() / ".cache" / "feather" / "packages"
    )
    migrate(database_url)
    host_url = os.environ.get("FEATHER_HOST_DATABASE_URL")
    if host_url:
        grant_host_role(database_url, role_of(host_url))  # the migrations may have added tables
    else:
        log.warning(
            "FEATHER_HOST_DATABASE_URL is not set: package hosts connect as %s, which can read every table, "
            "secrets included. Set up the feather_host role (Operating Guide, 'The host role')",
            "the worker",
        )
    store, spool = connect(database_url, tenant=tenant)
    keyring = keyring_from_env()
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
        tenant=tenant,
        log_dir=Path(d) if (d := os.environ.get("FEATHER_HOST_LOG_DIR")) else None,
        host_database_url=host_url or database_url,
        pass_env=[n for n in os.environ.get("FEATHER_HOST_ENV_PASS", "").split(",") if n.strip()],
        db_secrets=Vault(store.secrets, env, keyring).open if keyring is not None else None,
        partitions=lambda: ensure_partitions_ahead(store, spool),
        workdir=workdir,
    )
    log.info("supervisor %s runs the packages %s of %s", worker_id, ", ".join(packages), env)
    # The supervisor shows as a worker of its own, so the fleet view and `feather.worker` see it before any package is
    # active. It runs no task itself (no slots); its package hosts register as `<worker>/<package>@<version>`.
    from feather_connect.worker import heartbeats

    labels = {"role": SUPERVISOR_ROLE}
    sha = os.environ.get("FEATHER_CODE_SHA", "")
    store.workers.heartbeat(worker_id, labels, sha, 0)
    store.workers.mark(worker_id, "alive")
    stop_beating = heartbeats(store, worker_id, labels, sha, 0)
    try:
        return supervisor.run(told)
    finally:
        stop_beating.set()
        try:
            store.workers.mark(worker_id, "draining")
        except Exception:
            log.warning("could not mark %s as stopping", worker_id, exc_info=True)
        store.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m feather_connect.supervisor", description=__doc__)
    parser.add_argument("env")
    parser.add_argument("--database-url", default=os.environ.get("FEATHER_DATABASE_URL"))
    parser.add_argument("--worker-id")
    parser.add_argument("--packages", action="append", default=[], metavar="GLOB")
    parser.add_argument("--cache", type=Path)
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--tenant", default="default")
    parser.add_argument("--poll-s", type=float, default=float(os.environ.get("FEATHER_ENGINE_POLL_S", "0.5")))
    parser.add_argument(
        "--workdir", type=Path, help="for development: where hosts run and relative paths point"
    )
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
        tenant=args.tenant,
        workdir=args.workdir,
    )


if __name__ == "__main__":
    raise SystemExit(main())

"""The world of the package host tests (flow packages tasks 6 to 8): a database of its own, the `hosted` test package
built at the versions a test asks for, a supervisor running real host processes, and what the package reports.

A test installs a version as the server does (`preparing`), and the supervisor prepares it, builds its environment
with no index (`--find-links`) and starts its host when it has work. A test queues runs as the scheduler does, tagged
with the package and version, or runs the scheduler itself (`start_scheduler`).

Hosts connect as a host role of the world's own (`feather_host_<id>`), granted as `host_role` grants `feather_host`,
and get the sink's credential, `${secret:db:out/token}`, from the supervisor, which holds the vault's master key.
"""

import hashlib
import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg
from psycopg import sql

from feather_connect.packages.build import Built
from feather_connect.packages.envs import Environments
from feather_connect.runtime.catalogue import Catalogue
from feather_connect.storage.port import PackageVersion
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.host_role import ensure_partitions_ahead, grant_host_role
from feather_connect.storage.postgres.migrate import migrate
from feather_connect.supervisor import Supervisor
from feather_connect.testing.packaging import built_with_lock, library_wheel, lock_for
from feather_connect.vault import MasterKey, Vault, new_master_key

SOURCE = Path(__file__).with_name("hosted_package")
FLOW = "feed"
TOKEN = "s3cret-token-of-out"
TOKEN_DIGEST = hashlib.sha256(TOKEN.encode()).hexdigest()[:12]

DDL = """
create schema hosted;
create table hosted.holds (point text primary key, remaining int not null);
create table hosted.reached (
  id bigserial primary key, point text not null, code text not null, pid int not null,
  released boolean not null default false
);
create table hosted.attempts (
  id bigserial primary key, idempotency_key text not null, code text not null, pid int not null, token text,
  feather_env text
);
create table hosted.rows (
  route text not null, idempotency_key text not null, key text not null, code text not null, lib text,
  primary key (route, idempotency_key, key)
);
create table hosted.fail_load (code text primary key);
"""


def wait_for(check: Callable[[], Any], timeout: float, what: str) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = check()
        if found:
            return found
        time.sleep(0.1)
    raise AssertionError(f"timed out after {timeout:g} s waiting for {what}")


def pid_alive(pid: int) -> bool:
    """Whether a process of that id is running."""
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        code = ctypes.c_ulong()
        try:
            kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        finally:
            kernel32.CloseHandle(handle)
        return code.value == 259  # STILL_ACTIVE
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class HostWorld:
    """A database, a supervisor and its hosts, and the questions a test asks of them."""

    def __init__(self, url: str, tmp: Path) -> None:
        self.url = url
        self.tmp = tmp
        migrate(url)
        self.conn = psycopg.connect(url, autocommit=True)
        self.conn.execute(DDL)
        self.role = f"feather_host_{uuid.uuid4().hex[:8]}"
        password = uuid.uuid4().hex
        self.conn.execute(
            sql.SQL("create role {} login password {}").format(
                sql.Identifier(self.role), sql.Literal(password)
            )
        )
        grant_host_role(url, self.role)
        self.conn.execute(sql.SQL("grant usage on schema hosted to {}").format(sql.Identifier(self.role)))
        self.conn.execute(
            sql.SQL("grant select, insert, update on all tables in schema hosted to {}").format(
                sql.Identifier(self.role)
            )
        )
        self.conn.execute(
            sql.SQL("grant usage on all sequences in schema hosted to {}").format(sql.Identifier(self.role))
        )
        parts = urlsplit(url)
        self.host_url = parts._replace(
            netloc=f"{self.role}:{password}@{parts.hostname}:{parts.port}"
        ).geturl()
        self.store, self.spool = connect(url)
        self.master_key = new_master_key()
        vault = Vault(self.store.secrets, "dev", MasterKey.parse(self.master_key))
        vault.set("out/token", TOKEN, "ops", datetime.now(UTC))
        self.wheels = tmp / "wheels"
        self.wheels.mkdir(parents=True, exist_ok=True)
        self.envs = Environments(tmp / "envs", index=["--find-links", str(self.wheels)])
        self.logs = tmp / "logs"
        self.supervisor = Supervisor(
            self.store,
            env="dev",
            worker_id="sup",
            database_url=url,
            envs=self.envs,
            cache=tmp / "cache",
            log_dir=self.logs,
            poll_s=0.1,
            host_database_url=self.host_url,
            pass_env=["FEATHER_HOSTED_URL"],
            db_secrets=vault.open,
            partitions=lambda: ensure_partitions_ahead(self.store, self.spool),
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._scheduler: threading.Thread | None = None
        self._processes: list[subprocess.Popen[bytes]] = []

    # Packages.

    def build(self, name: str, version: str, lib: str | None = None) -> Built:
        """The test package as `name` at `version`, depending on featherlib at `lib` if given."""
        out = self.tmp / "dist"
        if lib is None:
            return built_with_lock(SOURCE, out, name=name, version=version, dependencies=[], lock=b"")
        wheel = library_wheel(
            self.wheels, "featherlib", lib, {"featherlib/__init__.py": f"VERSION = {lib!r}\n"}
        )
        return built_with_lock(
            SOURCE,
            out,
            name=name,
            version=version,
            dependencies=[f"featherlib=={lib}"],
            lock=lock_for([wheel]),
        )

    def install(self, built: Built) -> None:
        """Record the version as the server's install does: `preparing`, every flow inactive."""
        manifest = dict(built.manifest)
        content = built.path.read_bytes()
        name, version = str(manifest["name"]), str(manifest["version"])
        row = PackageVersion(
            name,
            version,
            hashlib.sha256(content).hexdigest(),
            "feather",
            built.path.name,
            len(content),
            manifest,
            str(manifest["fc_versions"]),
            str(manifest["lock_hash"]),
            "preparing",
            {},
            "root",
            datetime.now(UTC),
        )
        flows = [f"{name}/{f['name']}" for f in manifest["flows"]]
        self.store.packages.install(row, content, flows)

    def state(self, name: str, version: str) -> str:
        row = self.store.packages.version(name, version)
        return "missing" if row is None else row.state

    def activate(self, name: str, version: str, flow: str = FLOW) -> None:
        """Ask for the version, as the server's activate does: it is activating until its host says ready."""
        self.store.packages.activate(f"{name}/{flow}", name, version, "ops", datetime.now(UTC))

    def activation(self, name: str, version: str, flow: str = FLOW) -> str:
        found = [a for a in self.store.packages.activations(name, version) if a.flow == f"{name}/{flow}"]
        return found[0].state if found else "missing"

    def active(self, name: str, flow: str = FLOW) -> str | None:
        """The version whose triggers fire, if any."""
        found = [a.version for a in self.store.packages.active() if a.flow == f"{name}/{flow}"]
        return found[0] if found else None

    def deactivate(self, name: str, flow: str = FLOW) -> None:
        self.store.packages.deactivate(f"{name}/{flow}", "ops", datetime.now(UTC))

    def ticks(self, name: str, code: str | None = None) -> list[Any]:
        """The ticks of the poller's stream runs that ran, and of `code` if given, by the rows they delivered."""
        found = self.conn.execute(
            "select distinct idempotency_key from hosted.rows where route = %s"
            + (" and code = %s" if code else ""),
            (f"{name}/poll-out", code) if code else (f"{name}/poll-out",),
        ).fetchall()
        return [r[0] for r in found]

    def fail_load(self, version: str) -> None:
        self.conn.execute("insert into hosted.fail_load (code) values (%s)", (version,))

    # The supervisor and the scheduler.

    def start(self) -> None:
        self._thread = threading.Thread(target=self.supervisor.run, args=(self._stop,), name="supervisor")
        self._thread.start()

    def start_scheduler(self, every_s: float = 0.2) -> None:
        """Fire the active flows' triggers as the server's scheduler does, from the catalogue."""
        catalogue = Catalogue(self.store)
        catalogue.listen()

        def run() -> None:
            try:
                while not self._stop.wait(every_s):
                    catalogue.tick(datetime.now(UTC))
            finally:
                catalogue.close()

        self._scheduler = threading.Thread(target=run, name="scheduler", daemon=True)
        self._scheduler.start()

    def supervisor_process(self, cache: Path) -> "subprocess.Popen[bytes]":
        """A supervisor in a process of its own, as `feather worker --packages '*'` runs one."""
        env = {
            **os.environ,
            "FEATHER_PACKAGE_FIND_LINKS": str(self.wheels),
            "FEATHER_PACKAGE_NO_INDEX": "1",
            "FEATHER_HOST_LOG_DIR": str(self.logs),
            "FEATHER_HOST_ENV_PASS": "FEATHER_HOSTED_URL",
            "FEATHER_HOST_DATABASE_URL": self.host_url,
            "FEATHER_MASTER_KEY": self.master_key,
        }
        out = (self.tmp / f"supervisor-{len(self._processes)}.log").open("ab")
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "feather_connect.supervisor",
                "dev",
                "--database-url",
                self.url,
                "--worker-id",
                "sup",
                "--cache",
                str(cache),
                "--poll-s",
                "0.1",
            ],
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
        )
        out.close()
        self._processes.append(process)
        return process

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=180)
        if self._scheduler is not None:
            self._scheduler.join(timeout=10)
        for process in self._processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    process.kill()
        self.store.close()
        self.conn.execute(
            "select pg_terminate_backend(pid) from pg_stat_activity where usename = %s", (self.role,)
        )
        self.conn.execute(sql.SQL("drop owned by {}").format(sql.Identifier(self.role)))
        self.conn.execute(sql.SQL("drop role {}").format(sql.Identifier(self.role)))
        self.conn.close()

    def host_of(self, name: str, version: str) -> str | None:
        return self.supervisor.hosts().get((name, version))

    def alive(self, worker_id: str) -> bool:
        return any(w.worker_id == worker_id and w.state == "alive" for w in self.store.workers.list_workers())

    def kill_host(self, name: str, version: str) -> int:
        """Kill the version's host and everything it started at once, as `kill -9` does, and return its pid."""
        host = self.supervisor._hosts[(name, version)]  # pyright: ignore[reportPrivateUsage]
        pid = host.tree.process.pid
        host.tree.kill()
        return pid

    def keys_by_code(self) -> dict[str, set[str]]:
        """The idempotency keys the sink wrote under, by the version of the code that wrote them."""
        found: dict[str, set[str]] = {}
        for key, _, code, _ in self.rows():
            found.setdefault(code, set()).add(key)
        return found

    def log_of(self, worker_id: str) -> str:
        path = self.logs / f"{worker_id.replace('/', '_').replace(':', '_')}.log"
        return path.read_text(errors="replace")[-4000:] if path.is_file() else "(no log)"

    # Runs.

    def queue(self, name: str, version: str) -> str:
        """A run as the scheduler writes one for an active flow: queued, tagged with its package and version."""
        params = {"due_at": datetime.now(UTC).isoformat(), "package": name, "version": version}
        return self.store.runs.create_run(f"{name}/{FLOW}", "every:60s", "scheduler", params)

    def status(self, run_id: str) -> str:
        return self.store.runs.get_run(run_id).status

    def runs(self, name: str) -> list[Any]:
        return self.store.runs.list_runs(flow=f"{name}/{FLOW}", limit=500)

    def wait_for_runs(self, run_ids: list[str], timeout: float = 120) -> dict[str, str]:
        def over() -> dict[str, str] | None:
            found = {r: self.status(r) for r in run_ids}
            return found if all(s not in ("queued", "running") for s in found.values()) else None

        return wait_for(over, timeout, f"runs {run_ids} to end")

    # What the package reported.

    def rows(self, code: str | None = None) -> list[tuple[str, str, str, str | None]]:
        query = "select idempotency_key, key, code, lib from hosted.rows"
        found = self.conn.execute(
            query + (" where code = %s" if code else ""), (code,) if code else ()
        ).fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in found]

    def attempts(self) -> list[tuple[str, str, int, str | None, str | None]]:
        rows = self.conn.execute(
            "select idempotency_key, code, pid, token, feather_env from hosted.attempts order by id"
        ).fetchall()
        return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    def arm(self, point: str, holds: int = 1) -> None:
        self.conn.execute("insert into hosted.holds (point, remaining) values (%s, %s)", (point, holds))

    def reached(self, point: str) -> list[tuple[int, int]]:
        rows = self.conn.execute("select id, pid from hosted.reached where point = %s order by id", (point,))
        return [(r[0], r[1]) for r in rows.fetchall()]

    def release(self, point: str) -> None:
        self.conn.execute("update hosted.reached set released = true where point = %s", (point,))


def set_url(url: str) -> None:
    """Tell the hosts, which inherit this process's environment, where the package reports."""
    os.environ["FEATHER_HOSTED_URL"] = url

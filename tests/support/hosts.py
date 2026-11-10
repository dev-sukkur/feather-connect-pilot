"""The world of the package host tests (flow packages tasks 6 to 8): a database of its own, the `hosted` test package
built at the versions a test asks for, a supervisor running real host processes, and what the package reports.

A test installs a version as the server does (`preparing`), and the supervisor prepares it, builds its environment
with no index (`--no-index --find-links`) and starts its host when it has work. A test queues runs as the scheduler
does, tagged with the package and version.
"""

import hashlib
import os
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from feather_connect.packages.build import Built
from feather_connect.packages.envs import Environments
from feather_connect.storage.port import PackageVersion
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate
from feather_connect.supervisor import Supervisor
from feather_connect.testing.packaging import built_with_lock, library_wheel, lock_for

SOURCE = Path(__file__).with_name("hosted_package")
FLOW = "feed"

DDL = """
create schema hosted;
create table hosted.holds (point text primary key, remaining int not null);
create table hosted.reached (
  id bigserial primary key, point text not null, code text not null, pid int not null,
  released boolean not null default false
);
create table hosted.attempts (id bigserial primary key, idempotency_key text not null, code text not null, pid int not null);
create table hosted.rows (
  route text not null, idempotency_key text not null, key text not null, code text not null, lib text,
  primary key (route, idempotency_key, key)
);
"""


def wait_for(check: Callable[[], Any], timeout: float, what: str) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = check()
        if found:
            return found
        time.sleep(0.1)
    raise AssertionError(f"timed out after {timeout:g} s waiting for {what}")


class HostWorld:
    """A database, a supervisor and its hosts, and the questions a test asks of them."""

    def __init__(self, url: str, tmp: Path) -> None:
        self.url = url
        self.tmp = tmp
        migrate(url)
        self.conn = psycopg.connect(url, autocommit=True)
        self.conn.execute(DDL)
        self.store, self.spool = connect(url)
        self.wheels = tmp / "wheels"
        self.wheels.mkdir(parents=True, exist_ok=True)
        self.envs = Environments(tmp / "envs", index=["--no-index", "--find-links", str(self.wheels)])
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
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

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

    def activate(self, name: str, version: str) -> None:
        self.store.packages.activate(f"{name}/{FLOW}", name, version, "ops", datetime.now(UTC))

    def deactivate(self, name: str) -> None:
        self.store.packages.deactivate(f"{name}/{FLOW}", "ops", datetime.now(UTC))

    # The supervisor.

    def start(self) -> None:
        self._thread = threading.Thread(target=self.supervisor.run, args=(self._stop,), name="supervisor")
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=180)
        self.store.close()
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

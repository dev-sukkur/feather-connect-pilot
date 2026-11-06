"""Fixtures of the engine conformance suite: the test database, an engine, and real worker processes.

Workers run `worker_main.py` as host processes. With `FEATHER_TEST_WORKERS=docker` they run in containers of
`FEATHER_TEST_WORKER_IMAGE` (a Python 3.12 image with uv; the default is the uv image) that reach the database
on the host. `kill` is as abrupt as `kill -9` either way.
"""

import atexit
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import dict_row

from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate
from feather_connect.storage.postgres.store import PostgresStore
from feather_engine_dbos import DbosEngine

ROOT = Path(__file__).resolve().parents[2]
WORKER_MAIN = Path(__file__).with_name("worker_main.py")
DEFAULT_IMAGE = "ghcr.io/astral-sh/uv:python3.12-bookworm-slim"

DDL = """
create schema if not exists conformance;
create table if not exists conformance.exec_log (
  id bigserial primary key,
  run text not null,
  task_id text not null,
  attempt int not null,
  worker text not null,
  event text not null,
  at timestamptz not null default clock_timestamp(),
  info jsonb not null default '{}'
);
create index if not exists exec_log_run_idx on conformance.exec_log (run, task_id);
create table if not exists conformance.effects (
  id bigserial primary key,
  run text not null,
  effect_key text not null,
  worker text not null
);
create table if not exists conformance.checkpoint (
  run text not null, task_id text not null, chunk text not null,
  primary key (run, task_id, chunk)
);
create table if not exists conformance.counter (
  run text not null, task_id text not null, n int not null default 0,
  primary key (run, task_id)
);
create table if not exists conformance.submit_log (
  run text not null, task_id text not null, at timestamptz not null default clock_timestamp()
);
create table if not exists conformance.ready (
  worker text primary key, at timestamptz not null default clock_timestamp()
);
"""


class Db:
    """The test database as the tests see it. One autocommit connection; calls are serialized."""

    def __init__(self, url: str) -> None:
        self._conn = psycopg.connect(url, autocommit=True, row_factory=dict_row)
        self._lock = threading.Lock()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        rows = self.query(sql, params)
        return next(iter(rows[0].values())) if rows else None

    def count(self, run: str, event: str, task_id: str | None = None) -> int:
        if task_id:
            sql = "select count(*) from conformance.exec_log where run = %s and event = %s and task_id = %s"
            return self.scalar(sql, (run, event, task_id))
        return self.scalar(
            "select count(*) from conformance.exec_log where run = %s and event = %s", (run, event)
        )

    def events(self, run: str, event: str | None = None) -> list[dict[str, Any]]:
        sql, params = "select * from conformance.exec_log where run = %s", (run,)
        if event:
            sql, params = sql + " and event = %s", (run, event)
        return self.query(sql + " order by at, id", params)

    def close(self) -> None:
        self._conn.close()


class Workers:
    """Starts and kills the worker processes of one test. A worker's `name` is the test's own; its executor id
    in the engine is unique to the test (`executor(name)`), so that no test recovers another's tasks."""

    def __init__(self, url: str, run: str, log_dir: Path) -> None:
        self._url, self._run, self._log_dir = url, run, log_dir
        self._db = Db(url)
        self._procs: dict[str, subprocess.Popen[bytes]] = {}
        self._containers: dict[str, str] = {}
        self._docker = os.environ.get("FEATHER_TEST_WORKERS") == "docker"
        self.survivors: list[str] = []  # workers that could not be stopped

    def executor(self, name: str) -> str:
        return f"{self._run}-{name}"

    def start(
        self,
        name: str,
        slots: int = 4,
        env: dict[str, str] | None = None,
        *,
        poll_s: float = 0.1,
        version: str = "feather-1",
    ) -> float:
        """Start a worker and return how many seconds it took to be ready."""
        executor = self.executor(name)
        settings = {
            "FEATHER_CONF_WORKER": executor,
            "FEATHER_CONF_SLOTS": str(slots),
            "FEATHER_CONF_POLL_S": str(poll_s),
            "FEATHER_CONF_VERSION": version,
            **(env or {}),
        }
        self._db.execute("delete from conformance.ready where worker = %s", (executor,))
        t0 = time.monotonic()
        if self._docker:
            self._docker_start(name, settings)
        else:
            self._host_start(name, settings)
        deadline = time.monotonic() + 90
        while not self._db.scalar("select 1 from conformance.ready where worker = %s", (executor,)):
            if time.monotonic() > deadline:
                raise TimeoutError(f"worker {name} was not ready after 90 s; see the log in {self._log_dir}")
            if name in self._procs and self._procs[name].poll() is not None:
                raise RuntimeError(
                    f"worker {name} exited with {self._procs[name].returncode}; see {self._log_dir}"
                )
            time.sleep(0.1)
        return time.monotonic() - t0

    def _host_start(self, name: str, settings: dict[str, str]) -> None:
        log = open(self._log_dir / f"{self.executor(name)}.log", "ab")  # noqa: SIM115 - closed by the child's exit
        self._procs[name] = subprocess.Popen(
            [sys.executable, str(WORKER_MAIN)],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1", "FEATHER_CONF_DB": self._url, **settings},
            start_new_session=sys.platform != "win32",
        )
        log.close()  # the child has its own handle

    def _docker_start(self, name: str, settings: dict[str, str]) -> None:
        container = f"fc-conf-{uuid.uuid4().hex[:8]}-{name}"
        url = self._url.replace("127.0.0.1", "host.docker.internal")
        env = {
            "FEATHER_CONF_DB": url,
            "UV_PROJECT_ENVIRONMENT": "/tmp/venv",
            "UV_LINK_MODE": "copy",
            **settings,
        }
        command = ["docker", "run", "-d", "--name", container, "--add-host=host.docker.internal:host-gateway"]
        command += ["-v", f"{ROOT.as_posix()}:/app:ro", "-w", "/app"]
        for key, value in env.items():
            command += ["-e", f"{key}={value}"]
        command += [os.environ.get("FEATHER_TEST_WORKER_IMAGE", DEFAULT_IMAGE)]
        command += [
            "uv",
            "run",
            "--frozen",
            "--all-extras",
            "python",
            "tests/engine_conformance/worker_main.py",
        ]
        subprocess.run(command, check=True, capture_output=True)
        self._containers[name] = container

    def kill(self, name: str) -> None:
        """Stop a worker without letting it finish anything."""
        if name in self._containers:
            container = self._containers.pop(name)
            subprocess.run(["docker", "kill", container], check=False, capture_output=True)
            with open(self._log_dir / f"{self.executor(name)}.log", "ab") as log:
                subprocess.run(
                    ["docker", "logs", container], check=False, stdout=log, stderr=subprocess.STDOUT
                )
            subprocess.run(["docker", "rm", "-f", container], check=False, capture_output=True)
            return
        proc = self._procs.pop(name)
        if sys.platform == "win32":  # the venv's python.exe is a launcher with the interpreter as its child
            done = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)], check=False, capture_output=True, text=True
            )
            if done.returncode != 0 and proc.poll() is None:
                self.survivors.append(
                    f"{name} (pid {proc.pid}): {done.stdout.strip() or done.stderr.strip()}"
                )
                return
        else:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        proc.wait(timeout=30)

    def stop_all(self) -> None:
        for name in [*self._procs, *self._containers]:
            try:
                self.kill(name)
            except Exception as error:  # noqa: BLE001 - keep stopping the rest
                self.survivors.append(f"{name}: {error}")
        self._db.close()
        if self.survivors:
            warnings.warn(f"these workers could not be stopped and were left running: {self.survivors}")


@pytest.fixture(scope="session")
def conformance_url(pg_url: str) -> str:
    """The test database with the schema `feather`, the system database of DBOS, and the journal tables."""
    migrate(pg_url)
    with psycopg.connect(pg_url, autocommit=True) as conn:
        conn.execute(DDL)
    return pg_url


@pytest.fixture(scope="session")
def engine(conformance_url: str) -> Iterator[DbosEngine]:
    engine = DbosEngine(conformance_url)
    yield engine
    engine.close()


@pytest.fixture(scope="session")
def store(conformance_url: str) -> Iterator[PostgresStore]:
    store, _ = connect(conformance_url, max_size=2)
    yield store
    store.close()


@pytest.fixture
def db(conformance_url: str) -> Iterator[Db]:
    db = Db(conformance_url)
    yield db
    db.close()


@pytest.fixture
def run(request: pytest.FixtureRequest) -> str:
    return f"{request.node.name}-{uuid.uuid4().hex[:6]}".replace("[", "-").replace("]", "")


@pytest.fixture
def workers(
    conformance_url: str, engine: DbosEngine, db: Db, run: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Workers]:
    # Drop work that earlier tests left queued, so that it can't flood this test's workers.
    db.execute("delete from dbos.workflow_status where status in ('ENQUEUED', 'PENDING', 'DELAYED')")
    log_dir = (
        Path(os.environ.get("FEATHER_CONFORMANCE_LOGS", ""))
        if os.environ.get("FEATHER_CONFORMANCE_LOGS")
        else None
    )
    log_dir = log_dir or tmp_path_factory.mktemp("workers")
    log_dir.mkdir(parents=True, exist_ok=True)
    workers = Workers(conformance_url, run, log_dir)
    atexit.register(workers.stop_all)  # in case the session dies without running the fixture's cleanup
    try:
        yield workers
    finally:
        workers.stop_all()
        atexit.unregister(workers.stop_all)


@pytest.fixture
def wait_until() -> Callable[..., Any]:
    def wait(check: Callable[[], Any], timeout: float, what: str = "the condition") -> Any:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = check()
            if value:
                return value
            time.sleep(0.25)
        raise TimeoutError(f"timed out after {timeout} s waiting for {what}")

    return wait


@pytest.fixture
def record(request: pytest.FixtureRequest) -> Callable[..., None]:
    """Print the numbers a test measured, and append them to `FEATHER_CONFORMANCE_RESULTS` when that is set."""

    def write(test: str, **facts: Any) -> None:
        line = json.dumps({"test": test, "node": request.node.name, **facts}, default=str)
        print(f"MEASURED {line}")
        path = os.environ.get("FEATHER_CONFORMANCE_RESULTS")
        if path:
            with open(path, "a", encoding="utf-8") as out:
                out.write(line + "\n")

    return write

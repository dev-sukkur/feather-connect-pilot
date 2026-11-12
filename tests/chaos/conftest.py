"""The chaos tests' world: a database of its own, real worker processes, and what the sink and source report.

Each test gets its own database, on the test server or on a server of its own, so nothing a killed worker leaves behind
can reach the next test. A test only ever kills processes that it started and holds.
"""

import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from latency_proxy import LatencyProxy, delayed
from real_stack import (
    free_port,
    kill_abruptly,
    scratch_database,
    server_is_ready,
    server_url,
    start_server,
    start_worker,
    wait_until,
)

from feather_connect.host import PoolEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate
from feather_engine_dbos import DbosEngine

PROJECT = Path(__file__).with_name("project")
PACKAGE = "chaos-project"  # what the project installs as: its flows and datasets are `chaos-project/<name>`
HOOK_TOKEN = "chaos-hook-token"
CONTAINER = "feather-chaos-postgres"
CONTAINER_PORT = 25434  # the busy ports of the development machine are 5432, 55432, 7077 and 8888
CONTAINER_URL = f"postgresql://feather:feather@127.0.0.1:{CONTAINER_PORT}/feather_chaos"
SLOW_NETWORK_DELAY_S = (
    0.002  # each chunk of bytes, each way: some ms a round trip, as between zones of a region
)

DDL = """
create schema chaos;
create table chaos.holds (point text primary key, remaining int not null);
create table chaos.reached (
  id bigserial primary key, point text not null, who text not null, released boolean not null default false
);
create table chaos.reads (id bigserial primary key, connection text not null, who text not null);
create table chaos.attempts (
  id bigserial primary key, route text not null, idempotency_key text not null, chunk_no int not null, who text not null,
  finished boolean not null default false
);
create table chaos.sink_rows (
  route text not null, idempotency_key text not null, key text not null, primary key (route, idempotency_key, key)
);
"""


class World:
    """A test's database and worker processes, and the questions a test asks of them."""

    def __init__(self, url: str, logs: Path) -> None:
        self.url = url
        self.logs = logs
        migrate(url)
        self.conn = psycopg.connect(url, autocommit=True)
        self.conn.execute(DDL)
        self.store, self.spool = connect(url)
        self.engine = DbosEngine(url)  # for `recover_worker`: the tests' runs are planned by the hosts
        # The server's runtime, for a sweep run here: no package's flows, each task sent to its run's version's pool.
        self.runtime = Runtime([], self.store, self.spool, PoolEngine(self.engine, "server", self.store))
        self.procs: dict[str, subprocess.Popen[bytes]] = {}  # the current process of each worker id
        self.starts: dict[str, int] = {}
        self.hook_token = HOOK_TOKEN  # the bearer token of the declared webhook
        self.port = free_port()  # a server's, kept across its restarts

    def reconnect(self) -> None:
        """Open the test's own session again, after the server has been away."""
        self.conn = psycopg.connect(self.url, autocommit=True)

    def close(self) -> None:
        self.kill_all()
        self.engine.close()
        self.store.close()
        self.conn.close()

    # Workers.

    def start_worker(self, worker_id: str | None = None) -> str:
        """Start a worker, and return its id once it is registered. An id given again is a restart."""
        worker_id = worker_id or "w-" + uuid.uuid4().hex[:8]
        self.starts[worker_id] = self.starts.get(worker_id, 0) + 1
        with (self.logs / f"{worker_id}.log").open("ab") as log:
            self.procs[worker_id] = start_worker(
                PROJECT, self.url, worker_id, env=self._env(worker_id), log=log
            )
        host = self.host_of(worker_id)
        wait_until(lambda: host in [w.worker_id for w in self.store.workers.list_workers()], 120)
        return worker_id

    def _env(self, process_id: str) -> dict[str, str]:
        return {
            "FEATHER_CHAOS_URL": self.url,
            "FEATHER_CHAOS_WHO": self.who(process_id),
            "FEATHER_CHAOS_HOOK_TOKEN": HOOK_TOKEN,
        }

    def version(self) -> str:
        """The installed version, which the project's flows are active on once a worker has started."""
        versions = self.store.packages.versions(PACKAGE)
        assert versions, "no worker has installed the project yet"
        return versions[-1].version

    def host_of(self, worker_id: str) -> str:
        """The id of the worker's package host, which runs the tasks: `<worker>/<package>@<version>`."""
        return f"{worker_id}/{PACKAGE}@{self.version()}"

    @staticmethod
    def name(short: str) -> str:
        """A flow's or dataset's name as installed."""
        return f"{PACKAGE}/{short}"

    def start_run(self, flow: str) -> str:
        """Queue a run of the flow as the server or a tick does: tagged with the active version, whose host plans it."""
        name, version = self.name(flow), self.version()
        active = lambda: any(
            a.flow == name and a.state == "active" for a in self.store.packages.activations(PACKAGE)
        )
        wait_until(active, 120)
        return self.store.runs.create_run(name, "manual", "chaos", {"package": PACKAGE, "version": version})

    def start_server(self, server_id: str = "server") -> str:
        """Start a server, which runs the sealer and the sweeper, and return its id once it answers. An id given again
        is a restart, on the same port."""
        self.starts[server_id] = self.starts.get(server_id, 0) + 1
        env = self._env(server_id)
        log_file = self.logs / f"{server_id}.log"
        with log_file.open("ab") as log:
            proc = start_server(PROJECT, self.url, self.port, env=env, log=log)
        self.procs[server_id] = proc
        try:
            wait_until(lambda: server_is_ready(proc, self.server_address, log_file), 90)
        except AssertionError:
            raise AssertionError(
                f"{server_id} didn't answer: {log_file.read_text(errors='replace')[-3000:]}"
            ) from None
        return server_id

    @property
    def server_address(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def who(self, worker_id: str) -> str:
        """Name the current process of a worker, as the connectors report it. A process id wouldn't do, since on
        Windows `Popen.pid` is that of the launcher of the virtual environment."""
        return f"{worker_id}#{self.starts[worker_id]}"

    @staticmethod
    def worker_of(who: str) -> str:
        return who.partition("#")[0]

    def kill(self, worker_id: str) -> None:
        """Kill the worker at once, and make sure that the process stopped in a connector has gone: its session has."""
        who = self.who(worker_id)
        kill_abruptly(self.procs[worker_id])
        self.wait(lambda: not self._sessions_of(who), 30, f"the session of {who} to go")

    def parked(self, worker_id: str, task_ref: str) -> bool:
        """Whether DBOS has told the worker that it no longer owns the task's workflow, and parked that execution."""
        log = (self.logs / f"{worker_id}.log").read_text(errors="replace")
        return f"{task_ref} is no longer owned by this execution" in log

    def _sessions_of(self, who: str) -> int:
        found = self.conn.execute(
            "select count(*) from pg_stat_activity where application_name = %s", (who,)
        ).fetchone()
        assert found is not None
        return found[0]

    def kill_all(self) -> None:
        for proc in self.procs.values():
            if proc.poll() is None:
                kill_abruptly(proc)

    # Hold points.

    def arm(self, point: str, holds: int = 1) -> None:
        self.conn.execute("insert into chaos.holds (point, remaining) values (%s, %s)", (point, holds))

    def wait_for_point(self, point: str, count: int = 1, timeout: float = 90) -> list[str]:
        """Wait until `count` calls are stopped at `point`, and return the processes that reached it."""
        who: list[str] = []

        def reached() -> bool:
            rows = self.conn.execute("select who from chaos.reached where point = %s order by id", (point,))
            who[:] = [row[0] for row in rows.fetchall()]
            return len(who) >= count

        self.wait(reached, timeout, f"{count} calls to reach {point}")
        return who

    def release(self, point: str, nth: int | None = None) -> None:
        """Let go every call stopped at `point`, or only the one that reached it as number `nth`, from 0."""
        self.conn.execute(
            """update chaos.reached set released = true where id in (
                 select id from chaos.reached where point = %s order by id offset %s limit %s)""",
            (point, nth or 0, None if nth is None else 1),
        )

    # What happened.

    def sink_keys(self, route: str) -> set[str]:
        rows = self.conn.execute("select key from chaos.sink_rows where route = %s", (route,))
        return {row[0] for row in rows.fetchall()}

    def rows_of_chunk(self, route: str, chunk_no: int) -> int:
        """How many rows the sink holds of a chunk."""
        found = self.conn.execute(
            """select count(*) from chaos.sink_rows s where s.route = %s and s.idempotency_key in
                 (select a.idempotency_key from chaos.attempts a where a.route = %s and a.chunk_no = %s)""",
            (route, route, chunk_no),
        ).fetchone()
        assert found is not None
        return found[0]

    def max_writes_per_key(self) -> int:
        """The most *effective* writes of any one key on a route: `chaos.sink_rows`, not `chaos.attempts`. See `SlowSink`."""
        found = self.conn.execute(
            "select coalesce(max(n), 0) from (select count(*) n from chaos.sink_rows group by route, key) t"
        ).fetchone()
        assert found is not None
        return found[0]

    def attempts(self, route: str, chunk_no: int) -> list[tuple[str, bool]]:
        """Every call of `apply` for a chunk, as (process, whether it finished), oldest first."""
        rows = self.conn.execute(
            "select who, finished from chaos.attempts where route = %s and chunk_no = %s order by id",
            (route, chunk_no),
        )
        return [(row[0], row[1]) for row in rows.fetchall()]

    def reads(self, connection: str) -> list[str]:
        """The process of each read of the source, oldest first."""
        rows = self.conn.execute(
            "select who from chaos.reads where connection = %s order by id", (connection,)
        )
        return [row[0] for row in rows.fetchall()]

    def claimed_versions(self) -> list[str]:
        """The versions that some execution has claimed, sealed or not."""
        rows = self.conn.execute("select version_id from feather.spool_version")
        return [row[0] for row in rows.fetchall()]

    def deliveries(self, route: str) -> list[str]:
        return [delivery.status for delivery in self.store.ledger.list_deliveries(route=self.name(route))]

    def state(self) -> dict[str, Any]:
        """What a failing test should show."""
        runs = self.store.runs.list_runs(limit=20)
        workflows = self.conn.execute(
            "select workflow_uuid, status, executor_id, recovery_attempts from dbos.workflow_status"
        )
        return {
            "runs": [(run.run_id, run.flow, run.status, run.reason) for run in runs],
            "tasks": [
                (t.ref, t.status, t.attempt, t.error)
                for run in runs
                for t in self.store.runs.list_tasks(run.run_id)
            ],
            "deliveries": [
                (d.route, d.status, d.last_error) for d in self.store.ledger.list_deliveries(limit=50)
            ],
            "workflows": workflows.fetchall(),
            "workers": [(self.who(id_), proc.poll()) for id_, proc in self.procs.items()],
            "logs": str(self.logs),
        }

    def wait(self, check: Callable[[], Any], timeout: float, what: str) -> None:
        try:
            wait_until(check, timeout)
        except AssertionError:
            raise AssertionError(f"timed out after {timeout} s waiting for {what}: {self.state()}") from None

    def wait_for_run(self, run_id: str, timeout: float = 150) -> str:
        """Wait until the run is over, and return how it ended."""
        over = ("succeeded", "failed", "cancelled")
        self.wait(lambda: self.store.runs.get_run(run_id).status in over, timeout, f"run {run_id} to end")
        return self.store.runs.get_run(run_id).status


@pytest.fixture
def world(tmp_path: Path) -> Iterator[World]:
    """A world on a new database of the shared test server."""
    with scratch_database(server_url()) as url:
        found = World(url, tmp_path)
        try:
            yield found
        finally:
            found.close()


def _docker(*args: str, timeout: float = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, check=False)


def _wait_for_server(url: str, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with psycopg.connect(url, connect_timeout=2) as conn:
                conn.execute("select 1")
            return
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


class Server:
    """A Postgres of the test's own, in a container that only it uses."""

    def stop(self) -> None:
        done = _docker("stop", CONTAINER)
        assert done.returncode == 0, done.stderr

    def start(self) -> None:
        done = _docker("start", CONTAINER)
        assert done.returncode == 0, done.stderr
        _wait_for_server(CONTAINER_URL)


@pytest.fixture
def own_server() -> Iterator[Server]:
    """Start the throwaway Postgres, on disk since a restart must keep the data, and remove it with its volume after."""
    try:
        available = _docker("info", "--format", "{{.ServerVersion}}", timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        available = False
    if not available:
        pytest.skip("this test restarts its own Postgres in a container, and Docker is not available")
    _docker(
        "rm", "-f", "-v", CONTAINER
    )  # one that a crashed session left behind; the name is this fixture's alone
    done = _docker(
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-p",
        f"127.0.0.1:{CONTAINER_PORT}:5432",
        "-e",
        "POSTGRES_USER=feather",
        "-e",
        "POSTGRES_PASSWORD=feather",
        "-e",
        "POSTGRES_DB=feather_chaos",
        "postgres:17",
        "postgres",
        "-c",
        "max_connections=200",
    )
    assert done.returncode == 0, done.stderr
    try:
        _wait_for_server(CONTAINER_URL)
        yield Server()
    finally:
        _docker("rm", "-f", "-v", CONTAINER)


@pytest.fixture
def world_on_own_server(own_server: Server, tmp_path: Path) -> Iterator[tuple[World, Server]]:
    with scratch_database(CONTAINER_URL) as url:
        found = World(url, tmp_path)
        try:
            yield found, own_server
        finally:
            found.close()


@pytest.fixture
def slow_world(tmp_path: Path) -> Iterator[tuple[World, LatencyProxy]]:
    """A world whose every connection to Postgres, the workers' and the test's own, goes through a slow network
    (M3 plan 1 P2)."""
    with scratch_database(server_url()) as url, delayed(url, SLOW_NETWORK_DELAY_S) as (slow, proxy):
        found = World(slow, tmp_path)
        try:
            yield found, proxy
        finally:
            found.close()

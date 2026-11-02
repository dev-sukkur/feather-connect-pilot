"""The real stack as a fixture: a server under uvicorn and a worker process, on a database of their own.

The tests marked `perf` here take minutes, so they run only when asked for, by marker or by file. `tests/e2e` as a folder
leaves them out, which is what the `postgres` job and the usual local command run. See the `chaos` job for the nightly run.
"""

import subprocess
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
import pytest
from real_stack import (
    free_port,
    kill_abruptly,
    scratch_database,
    server_is_ready,
    server_url,
    start_server,
    start_worker,
    stop_gracefully,
    wait_until,
)

from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate

STARTING = 90  # a server or a worker to start: no statement about latency


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    named_a_file = any(str(arg).partition("::")[0].endswith(".py") for arg in config.args)
    if config.option.markexpr or named_a_file:
        return
    leave_out = pytest.mark.skip(reason="takes minutes: name the file, or select with -m perf")
    here = Path(__file__).parent
    for item in items:
        if item.get_closest_marker("perf") and item.path.is_relative_to(here):
            item.add_marker(leave_out)


@dataclass
class Stack:
    """A running server and worker, and what a test asks of them."""

    project: Path
    pg_url: str
    logs: Path
    env: dict[str, str]
    poll_s: float
    conn: psycopg.Connection[Any]
    store: Any
    port: int = field(default_factory=free_port)
    server: subprocess.Popen[bytes] | None = None
    worker: subprocess.Popen[bytes] | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start_server(self) -> None:
        log_file = self.logs / "server.log"
        with log_file.open("ab") as log:
            proc = start_server(self.project, self.pg_url, self.port, env=self.env, log=log)
        self.server = proc
        wait_until(lambda: server_is_ready(proc, self.url, log_file), STARTING)

    def kill_server(self) -> None:
        """Kill the server this stack started, and no other process."""
        assert self.server is not None
        kill_abruptly(self.server)
        self.server = None

    def start_worker(self) -> None:
        with (self.logs / "worker.log").open("ab") as log:
            self.worker = start_worker(
                self.project, self.pg_url, "w-perf", env=self.env, log=log, poll_s=self.poll_s
            )
        wait_until(lambda: "w-perf" in [w.worker_id for w in self.store.workers.list_workers()], STARTING)

    def close(self) -> None:
        if self.server is not None:
            kill_abruptly(self.server)
        if self.worker is not None and self.worker.poll() is None:
            stop_gracefully(self.worker)
            try:
                self.worker.wait(timeout=45)
            except subprocess.TimeoutExpired:
                kill_abruptly(self.worker)
        self.store.close()
        self.conn.close()


@pytest.fixture
def stack(tmp_path: Path) -> Iterator[Callable[..., Stack]]:
    """Make a `Stack` on a new database of the test server, running `project`. `ddl` is for what a project reports to."""
    made: list[tuple[Stack, Any]] = []

    def start(
        project: Path, *, env: dict[str, str] | None = None, ddl: str = "", poll_s: float = 0.2
    ) -> Stack:
        database = scratch_database(server_url())
        url = database.__enter__()
        migrate(url)
        conn = psycopg.connect(url, autocommit=True)
        if ddl:
            conn.execute(ddl)  # pyright: ignore[reportArgumentType]
        store, _ = connect(url)
        full_env = {
            "FEATHER_CODE_SHA": "testsha",
            "FEATHER_ENGINE_POLL_S": str(poll_s),
            "FEATHER_STACK_DATABASE_URL": url,  # for a connector that reports through the database
            **(env or {}),
        }
        found = Stack(project, url, tmp_path, full_env, poll_s, conn, store)
        made.append((found, database))
        found.start_server()
        found.start_worker()
        return found

    yield start
    for found, database in reversed(made):
        try:
            found.close()
        finally:
            database.__exit__(None, None, None)

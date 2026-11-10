"""The real stack: a server under uvicorn and a worker process, on a database of their own (`Stack`), and `open_stack`
to start one. The `stack` fixtures of tests/e2e and tests/dashboard are built on these."""

import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
from real_stack import (
    free_port,
    install_project,
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
        package = install_project(self.project, self.pg_url)  # installed already, by start_worker: its name
        wait_until(lambda: self._active(package), STARTING)  # its host took its flows over: they can run

    def _active(self, package: str) -> bool:
        found = self.store.packages.activations(package)
        return bool(found) and all(a.state == "active" for a in found if a.version == found[-1].version)

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


@contextmanager
def open_stack(
    project: Path, logs: Path, *, env: dict[str, str] | None = None, ddl: str = "", poll_s: float = 0.2
) -> Iterator[Stack]:
    """Start a `Stack` on a new database of the test server, running `project`, and stop it and drop the database at
    the end. `ddl` is for what a project reports to."""
    with scratch_database(server_url()) as url:
        migrate(url)
        conn = psycopg.connect(url, autocommit=True)
        if ddl:
            conn.execute(ddl)  # pyright: ignore[reportArgumentType, reportCallIssue]
        store, _ = connect(url)
        full_env = {
            "FEATHER_CODE_SHA": "testsha",
            "FEATHER_ENGINE_POLL_S": str(poll_s),
            "FEATHER_STACK_DATABASE_URL": url,  # for a connector that reports through the database
            **(env or {}),
        }
        found = Stack(project, url, logs, full_env, poll_s, conn, store)
        try:
            found.start_server()
            found.start_worker()
            yield found
        finally:
            found.close()


def stack_factory(logs: Path) -> tuple[Callable[..., Stack], Callable[[], None]]:
    """A function that starts stacks as `open_stack` does, and one that stops them all, the newest first."""
    opened: list[Any] = []

    def start(project: Path, **options: Any) -> Stack:
        manager = open_stack(project, logs, **options)
        found = manager.__enter__()
        opened.append(manager)
        return found

    def stop_all() -> None:
        for manager in reversed(opened):
            manager.__exit__(None, None, None)

    return start, stop_all

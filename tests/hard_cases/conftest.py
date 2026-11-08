"""The hard cases of design §16 on the full stack: a server and a worker process on Postgres, real `feather-postgres`
tables and `feather-http` calls, and an HTTP stand-in (examples/demo's sims/targets.py) that can go down.

    uv run pytest tests/hard_cases -q

One stack runs all of them: each case has its own flow, fixture and tables. The same cases run on `LocalEngine` in
packages/feather-connect/tests/runtime (test_extract.py, test_apply.py).
"""

import json
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from real_stack import free_port, kill_abruptly
from stack import Stack, open_stack

from feather_server.auth import issue_token

ROOT = Path(__file__).resolve().parents[2]
PROJECT = Path(__file__).resolve().parent / "project"
TARGETS = ROOT / "examples" / "demo" / "sims" / "targets.py"
CASES = ("h1", "h2", "h3", "h4", "h6")
DDL = """
create schema hard;
create table hard.h1_upsert (id text primary key, day text, v int);
create table hard.h1_replace (id text primary key, day text, v int);
create table hard.h2 (id text primary key, v text);
create table hard.h3 (sku text primary key, row_id int, p int);
create table hard.h4 (id text, u text, v int);
create table hard.h6 (id text primary key, v text);
"""
SETTLE = 180


def wait_until(check: Callable[[], object], what: str, timeout: float = SETTLE) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, httpx.HTTPError):
            pass
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


@dataclass
class Erp:
    """The HTTP stand-in, in a process of its own."""

    url: str
    proc: subprocess.Popen[bytes]

    def received(self) -> dict[str, Any]:
        return httpx.get(f"{self.url}/orders", timeout=5).json()

    def go_down(self) -> None:
        httpx.post(f"{self.url}/_targets/down", timeout=5).raise_for_status()

    def come_up(self) -> None:
        httpx.post(f"{self.url}/_targets/up", timeout=5).raise_for_status()


@dataclass
class Cases:
    """The running stack, and what a case does to it."""

    stack: Stack
    fixtures: Path
    erp: Erp
    api: httpx.Client

    def serve(self, case: str, rows: list[dict[str, Any]]) -> None:
        """Make the case's source hold `rows`; the next read sees them."""
        (self.fixtures / f"{case}.json").write_text(json.dumps(rows), encoding="utf-8")

    def run(self, flow: str, partitions: list[str] | None = None) -> str:
        """Run `flow` and wait for the run to end. Returns the run's id."""
        run_id = self.start(flow, partitions)
        wait_until(
            lambda: self.stack.store.runs.get_run(run_id).status in ("succeeded", "failed"), f"run {run_id}"
        )
        return run_id

    def settled(self, route: str) -> bool:
        """Whether every delivery of `route` is applied."""
        found = self.stack.store.ledger.list_deliveries(route=route)
        return bool(found) and all(d.status == "applied" for d in found)

    def wait_applied(self, *routes: str) -> None:
        for route in routes:
            try:
                wait_until(lambda route=route: self.settled(route), f"the deliveries of {route}")
            except AssertionError as error:
                found = [
                    (d.partition, d.status, d.attempts, d.last_error)
                    for d in self.stack.store.ledger.list_deliveries(route=route)
                ]
                raise AssertionError(f"{error}: {found}\n{self.worker_log()}") from None

    def start(self, flow: str, partitions: list[str] | None = None) -> str:
        """Start a run of `flow` and return its id, without waiting for it."""
        answer = self.api.post(f"/flows/{flow}:run", json={"partitions": partitions} if partitions else {})
        assert answer.status_code == 202, answer.text
        return answer.json()["run_id"]

    def worker_log(self) -> str:
        path = self.stack.logs / "worker.log"
        return path.read_text(encoding="utf-8", errors="replace")[-3000:] if path.exists() else ""

    @staticmethod
    def wait_until(check: Callable[[], object], what: str, timeout: float = SETTLE) -> None:
        wait_until(check, what, timeout)

    def rows(self, statement: str) -> list[tuple[Any, ...]]:
        return self.stack.conn.execute(statement).fetchall()  # pyright: ignore[reportArgumentType, reportCallIssue]


@pytest.fixture(scope="module")
def erp(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Erp]:
    port = free_port()
    data = tmp_path_factory.mktemp("erp")
    proc = subprocess.Popen(
        [sys.executable, str(TARGETS), "--port", str(port), "--data", str(data), "--no-table"]
    )

    def listening() -> bool:
        assert proc.poll() is None, "sims/targets.py ended"
        with socket.socket() as probe:
            return probe.connect_ex(("127.0.0.1", port)) == 0

    wait_until(listening, "the HTTP stand-in", 60)
    yield Erp(f"http://127.0.0.1:{port}", proc)
    kill_abruptly(proc)


@pytest.fixture(scope="module")
def cases(tmp_path_factory: pytest.TempPathFactory, erp: Erp) -> Iterator[Cases]:
    root = tmp_path_factory.mktemp("hard")
    project, fixtures = root / "project", root / "fixtures"
    shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns("__pycache__"))
    (project / "env" / "dev.yaml").write_text(  # JSON is YAML
        json.dumps({"connections": {"erp": {"base_url": erp.url}}}), encoding="utf-8"
    )
    fixtures.mkdir()
    for case in CASES:
        (fixtures / f"{case}.json").write_text("[]", encoding="utf-8")
    with open_stack(project, root, env={"FEATHER_HARD_FIXTURES": str(fixtures)}, ddl=DDL) as stack:
        token = issue_token(stack.store, "hard-cases", timedelta(hours=1))
        api = httpx.Client(
            base_url=f"{stack.url}/api/v1", headers={"Authorization": f"Bearer {token}"}, timeout=30
        )
        yield Cases(stack, fixtures, erp, api)
        api.close()

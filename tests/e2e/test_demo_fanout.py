"""The M1 exit demo on the real stack: examples/demo's orders, from a JSON simulation to Postgres, DuckDB and HTTP.

A copy of the demo runs with an overlay that points its three routes at this test's own targets: the `demo` schema
of the stack's database, a DuckDB file under tmp_path, and the demo's own `sims/targets.py` on a free port.
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

import duckdb
import httpx
import pytest
from real_stack import free_port, kill_abruptly

from feather_server.auth import issue_token

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(600)]

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "examples" / "demo"
DDL = (DEMO / "sims" / "demo.sql").read_text(encoding="utf-8")
FIXTURE = json.loads((DEMO / "fixtures" / "orders.json").read_text(encoding="utf-8"))
EXPECTED = {row["order_id"] for row in FIXTURE}
SETTLE = 240  # a run, its three deliveries, and on a down route its retries


def wait_until(check: Callable[[], object], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, httpx.HTTPError, duckdb.Error):
            pass
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


@dataclass
class Echo:
    """The demo's ERP stand-in, sims/targets.py, in a process of its own."""

    url: str
    proc: subprocess.Popen[bytes]

    def received(self) -> dict[str, Any]:
        return httpx.get(f"{self.url}/orders", timeout=5).json()

    def received_keys(self) -> set[str]:
        return set(self.received())

    def go_down(self) -> None:
        httpx.post(f"{self.url}/_targets/down", timeout=5).raise_for_status()

    def come_up(self) -> None:
        httpx.post(f"{self.url}/_targets/up", timeout=5).raise_for_status()


@pytest.fixture
def http_echo(tmp_path: Path) -> Iterator[Echo]:
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, str(DEMO / "sims" / "targets.py"), "--port", str(port)]
        + ["--data", str(tmp_path / "echo"), "--no-table"]
    )

    def listening() -> bool:
        assert proc.poll() is None, "sims/targets.py ended"
        with socket.socket() as probe:
            return probe.connect_ex(("127.0.0.1", port)) == 0

    wait_until(listening, 60, "the ERP stand-in")
    yield Echo(f"http://127.0.0.1:{port}", proc)
    kill_abruptly(proc)


@pytest.fixture
def duckdb_file(tmp_path: Path) -> Path:
    return tmp_path / "lake.duckdb"


@pytest.fixture
def demo(tmp_path: Path, http_echo: Echo, duckdb_file: Path) -> Path:
    project = tmp_path / "demo"
    shutil.copytree(DEMO, project, ignore=shutil.ignore_patterns("__pycache__", ".data", "tests"))
    overlay = {
        "connections": {
            "warehouse": {"schema": "demo", "auth": {"dsn": "${secret:env:FEATHER_STACK_DATABASE_URL}"}},
            "lake": {"database": duckdb_file.as_posix()},
            "erp": {
                "base_url": http_echo.url,
                "retry_attempts": 1,
                "auth": {"token": "${secret:file:secrets/erp-token.txt}"},
            },
        }
    }
    (project / "env" / "dev.yaml").write_text(json.dumps(overlay), encoding="utf-8")  # JSON is YAML
    return project


class Demo:
    """The stack running the demo, and what the tests look at."""

    def __init__(self, stack: Any, duckdb_file: Path) -> None:
        self.stack = stack
        self.duckdb_file = duckdb_file
        token = issue_token(stack.store, "e2e", timedelta(hours=1))
        self.api = httpx.Client(
            base_url=f"{stack.url}/api/v1", headers={"Authorization": f"Bearer {token}"}, timeout=30
        )

    def run_and_wait(self) -> str:
        answer = self.api.post("/flows/orders:run")
        assert answer.status_code == 202, answer.text
        run_id = answer.json()["run_id"]
        wait_until(
            lambda: self.stack.store.runs.get_run(run_id).status in ("succeeded", "failed"), SETTLE, "the run"
        )
        return run_id

    def pg_rows(self) -> dict[str, dict[str, Any]]:
        rows = self.stack.conn.execute("select order_id, status, total from demo.orders").fetchall()
        return {r[0]: {"status": r[1], "total": r[2]} for r in rows}

    def duck_rows(self) -> dict[str, dict[str, Any]]:
        with duckdb.connect(str(self.duckdb_file), read_only=True) as conn:
            rows = conn.execute("select order_id, status, total from bronze_orders").fetchall()
        return {r[0]: {"status": r[1], "total": str(r[2])} for r in rows}

    def deliveries(self, route: str, status: str) -> list[Any]:
        return self.stack.store.ledger.list_deliveries(route=route, status=status)

    def retry_failed(self, route: str) -> None:
        preview = self.api.post("/deliveries:retry:preview", json={"route": route})
        assert preview.status_code == 200, preview.text
        token = preview.json()["confirmation_token"]
        done = self.api.post("/deliveries:retry", json={"confirmation_token": token})
        assert done.status_code == 200, done.text


@pytest.fixture
def running(stack: Callable[..., Any], demo: Path, duckdb_file: Path) -> Demo:
    return Demo(stack(demo, ddl=DDL), duckdb_file)


def test_demo_fans_out_to_postgres_duckdb_and_http(running: Demo, http_echo: Echo):  # M1 exit criterion
    run = running.run_and_wait()
    assert running.stack.store.runs.get_run(run).status == "succeeded"
    wait_until(lambda: len(running.deliveries("erp", "applied")) >= 1, SETTLE, "the ERP's delivery")
    assert set(running.pg_rows()) == set(running.duck_rows()) == http_echo.received_keys() == EXPECTED


def test_a_down_route_does_not_hold_the_others_and_catches_up(running: Demo, http_echo: Echo):  # FR-B1, H6
    http_echo.go_down()
    running.run_and_wait()
    wait_until(lambda: running.deliveries("erp", "failed"), SETTLE, "the ERP's delivery to fail")
    assert set(running.pg_rows()) == set(running.duck_rows()) == EXPECTED
    assert http_echo.received_keys() == set()

    http_echo.come_up()
    running.retry_failed("erp")
    wait_until(lambda: http_echo.received_keys() == EXPECTED, SETTLE, "the ERP to catch up")
    assert running.deliveries("erp", "failed") == []


def test_new_data_produces_only_the_changes(running: Demo, http_echo: Echo, demo: Path):
    running.run_and_wait()
    wait_until(lambda: http_echo.received_keys() == EXPECTED, SETTLE, "the first delivery")
    first = {route: len(running.deliveries(route, "applied")) for route in ("warehouse", "lake", "erp")}

    changed = [dict(row) for row in FIXTURE]
    changed[0]["status"] = "delivered"
    (demo / "fixtures" / "orders.json").write_text(json.dumps(changed, indent=2), encoding="utf-8")
    running.run_and_wait()
    order = changed[0]["order_id"]
    wait_until(lambda: http_echo.received()[order]["status"] == "delivered", SETTLE, "the change at the ERP")
    assert running.pg_rows()[order]["status"] == running.duck_rows()[order]["status"] == "delivered"

    for route in ("warehouse", "lake", "erp"):
        applied = running.deliveries(route, "applied")
        assert len(applied) == first[route] + 1, route
        newest = max(applied, key=lambda d: d.seal_seq)
        assert newest.rows_applied == 1, route  # only the changed order went out

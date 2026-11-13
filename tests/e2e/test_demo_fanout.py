"""The M1 exit demo on the real stack: examples/demo's orders, from a JSON simulation to Postgres, DuckDB and HTTP.

A copy of the demo runs with an overlay that points its three routes at this test's own targets: the `demo` schema
of the stack's database, a DuckDB file under tmp_path, and the demo's own `sims/targets.py` on a free port.
"""

import asyncio
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
import httpx2
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from real_stack import free_port, kill_abruptly

from feather_server.auth import issue_token

pytestmark = [pytest.mark.postgres, pytest.mark.timeout(600)]

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "examples" / "demo"
DDL = (DEMO / "sims" / "demo.sql").read_text(encoding="utf-8")
FIXTURE = json.loads((DEMO / "fixtures" / "orders.json").read_text(encoding="utf-8"))
EXPECTED = {row["order_id"] for row in FIXTURE}
SETTLE = 240  # a run and its three deliveries
GIVING_UP = (
    480  # a delivery's 6 attempts, 10 s doubling to 160 s with jitter: up to 310 s of waits (RetryPolicy)
)


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
                # absolute: the installed package runs elsewhere, and secret files are never packaged
                "auth": {
                    "token": "${secret:file:" + (project / "secrets" / "erp-token.txt").as_posix() + "}"
                },
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
        token = issue_token(stack.store, "e2e", timedelta(hours=1), role="operator")
        self.api = httpx.Client(
            base_url=f"{stack.url}/api/v1", headers={"Authorization": f"Bearer {token}"}, timeout=30
        )

    def start_run(self) -> str:
        answer = self.api.post("/flows/demo/orders:run")
        assert answer.status_code == 202, answer.text
        return answer.json()["run_id"]

    def run_and_wait(self) -> str:
        run_id = self.start_run()
        wait_until(
            lambda: self.stack.store.runs.get_run(run_id).status in ("succeeded", "failed"), SETTLE, "the run"
        )
        return run_id

    def errors(self, run_id: str) -> list[tuple[str, str | None]]:
        """What the run's tasks failed with, for an assertion's message."""
        return [(t.kind, t.error) for t in self.stack.store.runs.list_tasks(run_id) if t.error]

    def pg_rows(self) -> dict[str, dict[str, Any]]:
        rows = self.stack.conn.execute("select order_id, status, total from demo.orders").fetchall()
        return {r[0]: {"status": r[1], "total": r[2]} for r in rows}

    def duck_rows(self) -> dict[str, dict[str, Any]]:
        with duckdb.connect(str(self.duckdb_file), read_only=True) as conn:
            rows = conn.execute("select order_id, status, total from bronze_orders").fetchall()
        return {r[0]: {"status": r[1], "total": str(r[2])} for r in rows}

    def deliveries(self, route: str, status: str) -> list[Any]:
        return self.stack.store.ledger.list_deliveries(route=f"demo/{route}", status=status)

    def retry_failed(self, route: str) -> None:
        preview = self.api.post("/deliveries:retry:preview", json={"route": f"demo/{route}"})
        assert preview.status_code == 200, preview.text
        token = preview.json()["confirmation_token"]
        done = self.api.post("/deliveries:retry", json={"confirmation_token": token})
        assert done.status_code == 200, done.text


@pytest.fixture
def running(stack: Callable[..., Any], demo: Path, duckdb_file: Path) -> Demo:
    return Demo(stack(demo, ddl=DDL), duckdb_file)


def test_demo_fans_out_to_postgres_duckdb_and_http(running: Demo, http_echo: Echo):  # M1 exit criterion
    run = running.run_and_wait()
    assert running.stack.store.runs.get_run(run).status == "succeeded", running.errors(run)
    wait_until(lambda: len(running.deliveries("erp", "applied")) >= 1, SETTLE, "the ERP's delivery")
    assert set(running.pg_rows()) == set(running.duck_rows()) == http_echo.received_keys() == EXPECTED


def test_a_down_route_does_not_hold_the_others_and_catches_up(running: Demo, http_echo: Echo):  # FR-B1, H6
    http_echo.go_down()
    running.start_run()  # it ends only once the ERP's delivery has given up
    wait_until(
        lambda: set(running.pg_rows()) == set(running.duck_rows()) == EXPECTED, SETTLE, "the other two routes"
    )
    assert http_echo.received_keys() == set()  # they did not wait for the ERP
    wait_until(lambda: running.deliveries("erp", "failed"), GIVING_UP, "the ERP's delivery to fail")
    assert http_echo.received_keys() == set()
    investigate_through_mcp(running)  # M3.1 A1's exit criterion, while the delivery is still failed

    http_echo.come_up()
    running.retry_failed("erp")
    wait_until(lambda: http_echo.received_keys() == EXPECTED, SETTLE, "the ERP to catch up")
    assert running.deliveries("erp", "failed") == []


def investigate_through_mcp(running: Demo) -> None:
    """An agent with a viewer token walks the failed delivery to its cause over /mcp, as the investigate-failure
    skill does, and has no way to change anything."""
    viewer = issue_token(running.stack.store, "claude", timedelta(hours=1), role="viewer")
    operator = issue_token(running.stack.store, "claude-op", timedelta(hours=1), role="operator")

    async def walk() -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {viewer}"}
        async with (
            httpx2.AsyncClient(headers=headers, timeout=60) as http,
            streamable_http_client(f"{running.stack.url}/mcp", http_client=http) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            tools = {t.name for t in (await session.list_tools()).tools}

            async def call(name: str, **arguments: Any) -> Any:
                result = await session.call_tool(name, arguments)
                assert not result.is_error, result
                return json.loads(result.content[0].text)  # pyright: ignore[reportAttributeAccessIssue]

            failures = (await call("list_failures"))["untrusted_data"]["items"]
            [erp] = [f for f in failures if f["route"] == "demo/erp"]
            [version] = erp["versions"][:1]
            runs = (await call("list_runs", flow="demo/orders"))["items"]
            run = await call("get_run", run_id=runs[0]["run_id"])
            key = FIXTURE[0]["order_id"]
            trace = await call("trace_record", dataset="demo/orders", key=key)
            return {"tools": tools, "erp": erp, "version": version, "run": run, "trace": trace}

    found = asyncio.run(walk())
    assert not any(t.startswith(("retry", "skip", "pause", "run_", "approve")) for t in found["tools"])
    assert "erp" in found["erp"]["message"].lower() or found["erp"]["count"] >= 1  # the cause, with the error
    assert found["trace"]["untrusted_content_note"]  # record data comes wrapped
    assert "demo/erp" in json.dumps(found["trace"])
    agent_rows = [a for a in running.stack.store.audit.list_audit(actor="claude") if a.actor_type == "agent"]
    assert {a.action for a in agent_rows} >= {"mcp:list_failures", "mcp:get_run", "mcp:trace_record"}
    refused = httpx.post(
        f"{running.stack.url}/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        headers={"Authorization": f"Bearer {operator}", "Accept": "application/json, text/event-stream"},
        timeout=30,
    )
    assert refused.status_code == 403  # an operator's token can't be an agent's


def test_new_data_produces_only_the_changes(running: Demo, http_echo: Echo, demo: Path):
    running.run_and_wait()
    wait_until(lambda: http_echo.received_keys() == EXPECTED, SETTLE, "the first delivery")
    first = {route: len(running.deliveries(route, "applied")) for route in ("warehouse", "lake", "erp")}

    changed = [dict(row) for row in FIXTURE]
    changed[0]["status"] = "delivered"
    (demo / "fixtures" / "orders.json").write_text(json.dumps(changed, indent=2), encoding="utf-8")
    running.stack.reinstall()  # the package carries its fixture: the change goes out as the next version
    running.run_and_wait()
    order = changed[0]["order_id"]
    wait_until(lambda: http_echo.received()[order]["status"] == "delivered", SETTLE, "the change at the ERP")
    assert running.pg_rows()[order]["status"] == running.duck_rows()[order]["status"] == "delivered"

    for route in ("warehouse", "lake", "erp"):
        applied = running.deliveries(route, "applied")
        assert len(applied) == first[route] + 1, route
        newest = max(applied, key=lambda d: d.seal_seq)
        assert newest.rows_applied == 1, route  # only the changed order went out

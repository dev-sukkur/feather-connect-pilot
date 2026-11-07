"""The Zakya demo under examples/zakya-demo, end to end: both simulators on free ports in this process, the flow on the
local engine with the memory store, and the receiver's SQLite file read back."""

import importlib.util
import json
import socket
import sqlite3
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import uvicorn

from feather_connect.clock import FrozenClock
from feather_connect.engine.local import LocalEngine
from feather_connect.project import load_project, validate_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

REPO = Path(__file__).resolve().parents[2]
DEMO = REPO / "examples" / "zakya-demo"


def _module(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"zakya_demo_{name}", DEMO / "sims" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _serve(app: Any) -> tuple[uvicorn.Server, threading.Thread, str]:
    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert thread.is_alive() and time.monotonic() < deadline, "a simulator did not start"
        time.sleep(0.05)
    return server, thread, f"http://127.0.0.1:{port}"


@pytest.fixture
def sims(tmp_path: Path) -> Iterator[tuple[Any, Path, dict[str, str]]]:
    """The Zakya simulator (changing only when told), the receiver's SQLite file, and the URLs of both."""
    zakya_sim, receiver_sim = _module("zakya"), _module("receiver")
    zakya = zakya_sim.Zakya(start=5, every=None, fault_rate=0)
    db = tmp_path / "receiver.sqlite"
    receiver = receiver_sim.Receiver(db, fault_rate=0)
    servers = [_serve(zakya_sim.create_app(zakya)), _serve(receiver_sim.create_app(receiver))]
    yield zakya, db, {"zakya": servers[0][2], "erp": servers[1][2]}
    for server, thread, _ in servers:
        server.should_exit = True
        thread.join(10)
    receiver.close()


def _received(db: Path) -> list[dict[str, Any]]:
    with sqlite3.connect(db) as conn:
        rows = conn.execute("select body, idempotency_key, headers from requests order by id").fetchall()
    return [{**json.loads(body), "_key": key, "_headers": json.loads(headers)} for body, key, headers in rows]


def test_the_zakya_demo_project_is_sound():
    project = load_project(DEMO, "dev")
    assert [f.name for f in project.flows] == ["zakya-invoices"]
    assert [r.name for r in project.flows[0].routes] == ["erp"]
    assert validate_project(project) == []


def test_each_invoice_reaches_the_receiver_once_and_a_changed_one_again(sims, monkeypatch):
    zakya, db, urls = sims
    monkeypatch.chdir(DEMO.parent)  # the fake secret files are read against the project, wherever this runs
    project = load_project(DEMO, "dev")
    clock = FrozenClock(datetime(2026, 11, 7, 10, tzinfo=UTC))
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    for budget in project.budgets.values():
        store.budgets.define(budget)

    def config_of(connection: str) -> dict[str, Any]:
        config = project.connections[connection].resolve()
        base = urls[connection]
        return {
            **config,
            "base_url": base,
            **({"token_url": f"{base}/oauth/v2/token"} if "token_url" in config else {}),
        }

    runtime = Runtime(
        project.flows,
        store,
        spool,
        engine,
        code_sha="demo",
        clock=clock,
        budgets={c.name: c.budget for c in project.connections.values() if c.budget is not None},
        config_of=config_of,
    )
    engine.register(runtime.tasks())

    first = runtime.start_run("zakya-invoices")
    engine.run_until_idle()
    assert store.runs.get_run(first).status == "succeeded"
    received = _received(db)
    assert sorted(r["external_id"] for r in received) == sorted(zakya.invoices)  # each invoice once
    assert all(r["lines"] and r["status"] == "sent" for r in received)
    assert all("authorization" not in r["_headers"] for r in received)  # the receiver keeps no credentials
    (delivery,) = store.ledger.list_deliveries(route="erp")
    answers = store.responses.of_delivery("erp", delivery.version_id)
    assert len(answers) == 5 and all(
        a.status == 201 and str(a.fields["erp_id"]).startswith("ERP-") for a in answers
    )

    changed = zakya.change()
    (added,) = zakya.add(1)
    second = runtime.start_run("zakya-invoices")
    engine.run_until_idle()
    assert store.runs.get_run(second).status == "succeeded"
    received = _received(db)
    assert len(received) == 7
    again = [r for r in received if r["external_id"] == changed]
    assert [r["status"] for r in again] == ["sent", "paid"] and again[0]["_key"] != again[1]["_key"]
    assert [r["external_id"] for r in received].count(added) == 1

    third = runtime.start_run("zakya-invoices")  # nothing new: nothing is sent
    engine.run_until_idle()
    assert store.runs.get_run(third).status == "succeeded"
    assert len(_received(db)) == 7


def test_the_zakya_simulator_keeps_its_invoices_across_a_restart(tmp_path):
    zakya_sim = _module("zakya")
    state = tmp_path / "zakya.json"
    first = zakya_sim.Zakya(start=3, every=None, fault_rate=0, state=state)
    first.change()
    again = zakya_sim.Zakya(start=3, every=None, fault_rate=0, state=state)
    assert again.invoices == first.invoices
    assert again.add() == ["4600000000004"]

"""The demo project under examples/demo, run on the local engine: no Postgres, no network."""

from datetime import UTC, datetime
from pathlib import Path

from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory
from feather_connect.core.triggers import every
from feather_connect.engine.local import LocalEngine
from feather_connect.project import load_project, validate_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

DEMO = Path(__file__).resolve().parents[2] / "examples" / "demo"


def test_the_demo_project_is_sound():
    project = load_project(DEMO, "dev")
    assert [f.name for f in project.flows] == ["orders"]
    assert project.flows[0].triggers == (every("10s"),)
    assert [r.name for r in project.flows[0].routes] == ["warehouse", "archive"]
    assert validate_project(project) == []


def test_both_routes_receive_the_rows():
    clock = FrozenClock(datetime(2026, 10, 18, 10, tzinfo=UTC))
    project = load_project(DEMO, "dev")
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    runtime = Runtime(project.flows, store, spool, engine, code_sha="demo", clock=clock)
    engine.register(runtime.tasks())

    run = runtime.start_run("orders")
    engine.run_until_idle()

    assert store.runs.get_run(run).status == "succeeded"
    received: dict[str, list[dict[str, object]]] = {}
    for route in project.flows[0].routes:
        assert isinstance(route.sink, memory.Sink)
        received[route.name] = route.sink.rows
    assert len(received["warehouse"]) == 20  # a duplicated row is one row, as it has one key
    assert received["warehouse"] == received["archive"]

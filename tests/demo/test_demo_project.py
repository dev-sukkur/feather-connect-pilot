"""The demo project under examples/demo, run on the local engine: no Postgres, no network."""

from datetime import UTC, datetime
from pathlib import Path

from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory, simulation
from feather_connect.core.triggers import cron
from feather_connect.engine.local import LocalEngine
from feather_connect.project import load_project, validate_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

DEMO = Path(__file__).resolve().parents[2] / "examples" / "demo"


def test_the_demo_project_is_sound():
    project = load_project(DEMO, "dev")
    assert [f.name for f in project.flows] == ["orders"]
    assert project.flows[0].triggers == (cron("* * * * *", tz="UTC"),)
    assert [r.name for r in project.flows[0].routes] == ["warehouse", "archive"]
    assert validate_project(project) == []


def _demo(fail_rate: float | None = None):
    """The demo project on the local engine. `fail_rate` overrides how often the archive sink fails a delivery."""
    clock = FrozenClock(datetime(2026, 10, 18, 10, tzinfo=UTC))
    project = load_project(DEMO, "dev")
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    runtime = Runtime(project.flows, store, spool, engine, code_sha="demo", clock=clock)
    engine.register(runtime.tasks())
    sinks = {route.name: route.sink for route in project.flows[0].routes}
    if fail_rate is not None:
        archive = sinks["archive"]
        assert isinstance(archive, simulation.Sink)
        archive.fail_rate = fail_rate
    return store, engine, runtime, sinks


def test_the_archive_route_can_fail_a_delivery():
    _, _, _, sinks = _demo()
    archive = sinks["archive"]
    assert isinstance(archive, simulation.Sink) and 0 < archive.fail_rate < 0.2
    warehouse = sinks["warehouse"]
    assert isinstance(warehouse, simulation.Sink) and warehouse.fail_rate == 0


def test_both_routes_receive_the_rows():
    store, engine, runtime, sinks = _demo(fail_rate=0)
    run = runtime.start_run("orders")
    engine.run_until_idle()

    assert store.runs.get_run(run).status == "succeeded"
    received: dict[str, list[dict[str, object]]] = {}
    for name, sink in sinks.items():
        assert isinstance(sink, memory.Sink)
        received[name] = sink.rows
    assert len(received["warehouse"]) == 20  # a duplicated row is one row, as it has one key
    assert received["warehouse"] == received["archive"]


def test_retrying_a_failed_delivery_succeeds():
    store, engine, runtime, sinks = _demo(fail_rate=1.0)
    run = runtime.start_run("orders")
    engine.run_until_idle()

    [failed] = store.reads.failure_groups()
    assert failed.route == "archive" and "simulated delivery failure" in failed.message
    assert store.runs.get_run(run).status == "failed"
    assert isinstance(sinks["archive"], memory.Sink) and sinks["archive"].rows == []
    version_id = failed.versions[0]

    runtime.retry("archive", version_id, actor="op")
    engine.run_until_idle()

    assert store.ledger.get("archive", version_id).status == "applied"
    assert store.runs.get_run(run).status == "succeeded"
    assert len(sinks["archive"].rows) == 20

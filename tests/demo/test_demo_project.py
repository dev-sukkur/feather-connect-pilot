"""The demo project under examples/demo, run on the local engine with the qa overlay: no Postgres, no network.

dev points the three routes at real targets on this machine (tests/e2e/test_demo_fanout.py runs those); qa runs them
on simulation stand-ins, which is what these tests use.
"""

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
ROUTES = ["warehouse", "lake", "erp"]


def test_the_demo_project_is_sound():
    project = load_project(DEMO, "qa")
    assert [f.name for f in project.flows] == ["orders"]
    assert project.flows[0].triggers == (cron("* * * * *", tz="UTC"),)
    assert [r.name for r in project.flows[0].routes] == ROUTES
    assert validate_project(project) == []


def _demo(erp_fail_rate: float = 0):
    """The demo project on the local engine, with the ERP's stand-in failing that share of deliveries."""
    clock = FrozenClock(datetime(2026, 11, 7, 10, tzinfo=UTC))
    project = load_project(DEMO, "qa")
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    runtime = Runtime(project.flows, store, spool, engine, code_sha="demo", clock=clock)
    engine.register(runtime.tasks())
    sinks = {route.name: route.sink for route in project.flows[0].routes}
    erp = sinks["erp"]
    assert isinstance(erp, simulation.Sink)
    erp.fail_rate = erp_fail_rate
    return store, engine, runtime, sinks


def _rows(sink: object) -> list[dict[str, object]]:
    assert isinstance(sink, memory.Sink)
    return sink.rows


def test_every_route_receives_the_orders():
    store, engine, runtime, sinks = _demo()
    run = runtime.start_run("orders")
    engine.run_until_idle()

    assert store.runs.get_run(run).status == "succeeded"
    keys = {name: sorted(str(r["order_id"]) for r in _rows(sink)) for name, sink in sinks.items()}
    assert keys["warehouse"] == [f"ORD-0000{n}" for n in range(1, 6)]
    assert keys["warehouse"] == keys["lake"] == keys["erp"]


def test_a_failed_route_holds_only_itself_and_a_retry_catches_it_up():
    store, engine, runtime, sinks = _demo(erp_fail_rate=1.0)
    run = runtime.start_run("orders")
    engine.run_until_idle()

    [failed] = store.reads.failure_groups()
    assert failed.route == "erp" and "simulated delivery failure" in failed.message
    assert store.runs.get_run(run).status == "failed"
    assert _rows(sinks["erp"]) == [] and len(_rows(sinks["warehouse"])) == len(_rows(sinks["lake"])) == 5
    version_id = failed.versions[0]

    erp = sinks["erp"]
    assert isinstance(erp, simulation.Sink)
    erp.fail_rate = 0
    runtime.retry("erp", version_id, actor="op")
    engine.run_until_idle()

    assert store.ledger.get("erp", version_id).status == "applied"
    assert store.runs.get_run(run).status == "succeeded"
    assert len(_rows(sinks["erp"])) == 5
    assert len(_rows(sinks["warehouse"])) == 5  # sent nothing again

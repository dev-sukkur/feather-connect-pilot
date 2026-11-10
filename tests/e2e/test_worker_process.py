"""The worker as a real process, against the test Postgres: a supervisor running the installed `demo_project`
(flow packages §7.1), a small project made for these tests and installed as a package by `start_worker`."""

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest
from real_stack import install_project, scratch_database, server_url, start_worker, stop_gracefully, wait_until

from feather_connect.project import ProjectError
from feather_connect.storage.postgres.migrate import migrate

pytestmark = pytest.mark.postgres

ROWS = [{"id": n, "name": f"row {n}"} for n in range(1, 6)]
FLOW = "demo-project/demo"


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    (tmp_path / "fixtures").mkdir()
    (tmp_path / "fixtures" / "rows.json").write_text(json.dumps(ROWS))
    (tmp_path / "flows").mkdir()
    (tmp_path / "env").mkdir()
    (tmp_path / "feather.yaml").write_text(
        dedent(
            """
            name: demo-project
            modules: [flows.demo]
            connections:
              sim_source: {connector: simulation.source}
              sim_sink: {connector: simulation.sink}
            """
        )
    )
    (tmp_path / "env" / "dev.yaml").write_text("")
    (tmp_path / "flows" / "demo.py").write_text(
        f"ROOT = {str(tmp_path)!r}\n"
        + dedent(
            """
            from pathlib import Path

            import feather_connect as fc
            from feather_connect.connectors import simulation

            root = Path(ROOT)  # the test's folder: the installed copy of this file runs from elsewhere
            orders = fc.Dataset(
                "orders",
                source=simulation.Source("sim_source", fixture=root / "fixtures" / "rows.json"),
                key="id",
                strategy=fc.strategy.replace(),
            )
            demo = fc.Flow(
                "demo",
                reads=orders,
                deliver=[fc.to(simulation.Sink("sim_sink", capture_to=root / "out" / "sink.jsonl"))],
            )
            """
        )
    )
    return tmp_path


def sim_sink_rows(project: Path) -> list[dict[str, Any]]:
    out = project / "out" / "sink.jsonl"
    return [json.loads(line)["payload"] for line in out.read_text().splitlines()] if out.exists() else []


def queue(store: Any, pg_url: str, project: Path) -> str:
    """A run of the flow as the scheduler writes one: queued, tagged with the installed version."""
    version = store.packages.versions("demo-project")[-1]  # this test's: the newest, as the database is shared
    params = {"due_at": datetime.now(UTC).isoformat(), "package": "demo-project", "version": version.version}
    return store.runs.create_run(FLOW, "manual", "test", params)


def alive(store: Any, wid: str) -> bool:
    return any(w.worker_id == wid and w.state == "alive" for w in store.workers.list_workers())


def test_the_worker_installs_the_flow_and_heartbeats_as_a_supervisor(demo_project, pg_url, db, workers):
    store, _ = db
    wid = "w-" + uuid.uuid4().hex[:6]
    workers.append(start_worker(demo_project, pg_url, wid))
    wait_until(lambda: alive(store, wid), 60)
    wait_until(lambda: store.catalog.flow(FLOW), 60)
    flow = store.catalog.flow(FLOW)
    assert flow.spec["datasets"][0]["name"] == "demo-project/orders"
    [mine] = [w for w in store.workers.list_workers() if w.worker_id == wid]
    assert mine.labels.get("role") == "supervisor"
    first = mine.last_seen
    wait_until(lambda: next(w for w in store.workers.list_workers() if w.worker_id == wid).last_seen > first, 20)


def test_a_run_completes_through_a_real_worker(demo_project, pg_url, db, workers):
    store, _ = db
    workers.append(start_worker(demo_project, pg_url, "w-run-" + uuid.uuid4().hex[:6]))
    run = queue(store, pg_url, demo_project)
    wait_until(lambda: store.runs.get_run(run).status == "succeeded", timeout=120)
    assert sim_sink_rows(demo_project) == ROWS


def test_a_retry_is_noted_in_the_journal_through_a_real_worker(demo_project, pg_url, db, workers):
    """Seed 5 makes the first two reads of the source time out and the third one work."""
    store, _ = db
    flow = demo_project / "flows" / "demo.py"
    flow.write_text(
        flow.read_text().replace(
            'fixture=root / "fixtures" / "rows.json"',
            'fixture=root / "fixtures" / "rows.json", seed=5, faults=simulation.Faults(http_5xx=0.5)',
        )
    )
    workers.append(start_worker(demo_project, pg_url, "w-retry-" + uuid.uuid4().hex[:6]))
    run = queue(store, pg_url, demo_project)
    wait_until(lambda: store.runs.get_run(run).status == "succeeded", timeout=120)
    [extract] = [t for t in store.runs.list_tasks(run) if t.kind == "extract"]
    assert (extract.status, extract.attempt, extract.error) == ("succeeded", 3, "simulated HTTP 503")
    assert sim_sink_rows(demo_project) == ROWS


def test_sigterm_drains_and_exits_zero(demo_project, pg_url, db, workers):
    store, _ = db
    wid = "w-term-" + uuid.uuid4().hex[:6]
    proc = start_worker(demo_project, pg_url, wid)
    workers.append(proc)
    wait_until(lambda: alive(store, wid), 60)
    time.sleep(2)  # let it finish starting, so that the signal finds its handler
    stop_gracefully(proc)
    assert proc.wait(timeout=120) == 0
    [mine] = [w for w in store.workers.list_workers() if w.worker_id == wid]
    assert mine.state == "draining"


def test_a_worker_started_again_under_its_id_is_alive(demo_project, pg_url, db, workers):
    """A restarted container keeps its hostname, the default id: the new process is alive, not its last one's drain."""
    store, _ = db
    wid = "w-again-" + uuid.uuid4().hex[:6]
    proc = start_worker(demo_project, pg_url, wid)
    workers.append(proc)
    wait_until(lambda: alive(store, wid), 60)
    time.sleep(2)
    stop_gracefully(proc)
    assert proc.wait(timeout=120) == 0
    workers.append(start_worker(demo_project, pg_url, wid))
    wait_until(lambda: alive(store, wid), 60)


def test_a_project_that_does_not_load_is_not_installed(tmp_path):
    (tmp_path / "feather.yaml").write_text("name: x\nmodules: [flows.nope]\n")
    (tmp_path / "env").mkdir()
    (tmp_path / "env" / "dev.yaml").write_text("")
    with scratch_database(server_url()) as url:
        migrate(url)
        with pytest.raises(ProjectError, match="can't import it"):
            install_project(tmp_path, url)

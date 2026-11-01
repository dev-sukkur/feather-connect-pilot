"""The worker as a real process, against the test Postgres. `demo_project` is a small project made for these tests."""

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest

from feather_connect.project import load_project
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.port import Store
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.spool import Spool
from feather_engine_dbos import DbosEngine

pytestmark = pytest.mark.postgres

ROWS = [{"id": n, "name": f"row {n}"} for n in range(1, 6)]
SHA = "testsha"


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
        dedent(
            """
            from pathlib import Path

            import feather_connect as fc
            from feather_connect.connectors import simulation

            root = Path(__file__).parent.parent
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


def wait_until(check: Callable[[], Any], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except KeyError:
            pass
        time.sleep(0.2)
    raise AssertionError("timed out waiting")


@pytest.fixture
def db(pg_url: str) -> Iterator[tuple[Store, Spool]]:
    from feather_connect.storage.postgres.migrate import migrate

    migrate(pg_url)
    yield connect(pg_url)


def start_worker(project: Path, pg_url: str, worker_id: str, sha: str = SHA) -> subprocess.Popen[bytes]:
    env = {**os.environ, "FEATHER_CODE_SHA": sha}
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0  # type: ignore[attr-defined]
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "feather_connect.worker",
            str(project),
            "dev",
            "--database-url",
            pg_url,
            "--worker-id",
            worker_id,
            "--poll-s",
            "0.1",
        ],
        env=env,
        creationflags=flags,
    )


@pytest.fixture
def workers() -> Iterator[list[subprocess.Popen[bytes]]]:
    procs: list[subprocess.Popen[bytes]] = []
    yield procs
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait()


def stop_gracefully(proc: subprocess.Popen[bytes]) -> None:
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
    else:
        proc.terminate()


def test_worker_registers_catalogue_and_heartbeats(demo_project, pg_url, db, workers):
    store, _ = db
    wid = "w-" + uuid.uuid4().hex[:6]
    workers.append(start_worker(demo_project, pg_url, wid))
    wait_until(lambda: store.catalog.flow("demo").code_sha == SHA)
    wait_until(lambda: wid in [w.worker_id for w in store.workers.list_workers()])
    flow = store.catalog.flow("demo")
    assert flow.project == "demo-project" and flow.spec["datasets"][0]["name"] == "orders"
    assert [r.name for r in store.catalog.routes()] == ["sim_sink"]
    [mine] = [w for w in store.workers.list_workers() if w.worker_id == wid]
    assert (mine.code_sha, mine.slots, mine.state) == (SHA, 8, "alive")
    first = mine.last_seen
    wait_until(
        lambda: next(w for w in store.workers.list_workers() if w.worker_id == wid).last_seen > first, 20
    )


def test_a_run_completes_through_a_real_worker(demo_project, pg_url, db, workers):
    store, spool = db
    workers.append(start_worker(demo_project, pg_url, "w-run-" + uuid.uuid4().hex[:6]))
    client = Runtime(load_project(demo_project, "dev").flows, store, spool, DbosEngine(pg_url))
    wait_until(lambda: store.workers.list_workers())
    run = client.start_run("demo")
    wait_until(lambda: store.runs.get_run(run).status == "succeeded", timeout=60)
    assert sim_sink_rows(demo_project) == ROWS


def test_a_retry_is_noted_in_the_journal_through_a_real_worker(demo_project, pg_url, db, workers):
    """Seed 5 makes the first two reads of the source time out and the third one work."""
    store, spool = db
    flow = demo_project / "flows" / "demo.py"
    flow.write_text(
        flow.read_text().replace(
            'fixture=root / "fixtures" / "rows.json"',
            'fixture=root / "fixtures" / "rows.json", seed=5, faults=simulation.Faults(http_5xx=0.5)',
        )
    )
    workers.append(start_worker(demo_project, pg_url, "w-retry-" + uuid.uuid4().hex[:6]))
    client = Runtime(load_project(demo_project, "dev").flows, store, spool, DbosEngine(pg_url))
    wait_until(lambda: store.workers.list_workers())
    run = client.start_run("demo")
    wait_until(lambda: store.runs.get_run(run).status == "succeeded", timeout=90)
    [extract] = [t for t in store.runs.list_tasks(run) if t.kind == "extract"]
    assert (extract.status, extract.attempt, extract.error) == ("succeeded", 3, "simulated HTTP 503")
    assert sim_sink_rows(demo_project) == ROWS


def test_sigterm_drains_and_exits_zero(demo_project, pg_url, db, workers):
    store, _ = db
    wid = "w-term-" + uuid.uuid4().hex[:6]
    proc = start_worker(demo_project, pg_url, wid)
    workers.append(proc)
    wait_until(lambda: wid in [w.worker_id for w in store.workers.list_workers()])
    time.sleep(2)  # let it finish starting, so that the signal finds its handler
    stop_gracefully(proc)
    assert proc.wait(timeout=60) == 0
    [mine] = [w for w in store.workers.list_workers() if w.worker_id == wid]
    assert mine.state == "draining"


def test_workers_with_different_shas_are_visible(demo_project, pg_url, db, workers):
    store, _ = db
    a, b = "w-a-" + uuid.uuid4().hex[:6], "w-b-" + uuid.uuid4().hex[:6]
    workers.append(start_worker(demo_project, pg_url, a, "sha-a"))
    workers.append(start_worker(demo_project, pg_url, b, "sha-b"))
    wait_until(lambda: {a, b} <= {w.worker_id for w in store.workers.list_workers()})
    shas = {w.worker_id: w.code_sha for w in store.workers.list_workers()}
    assert (shas[a], shas[b]) == ("sha-a", "sha-b")
    assert {store.catalog.flow("demo", "sha-a").code_sha, store.catalog.flow("demo", "sha-b").code_sha} == {
        "sha-a",
        "sha-b",
    }


def test_a_project_that_does_not_load_ends_the_worker_with_a_message(tmp_path, pg_url, workers):
    (tmp_path / "feather.yaml").write_text("name: x\nmodules: [flows.nope]\n")
    (tmp_path / "env").mkdir()
    (tmp_path / "env" / "dev.yaml").write_text("")
    proc = start_worker(tmp_path, pg_url, "w-bad")
    workers.append(proc)
    assert proc.wait(timeout=60) != 0

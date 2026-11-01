import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from dbos import DBOSClient

import feather_engine_dbos
from feather_connect.engine.port import RetryPolicy, TaskCall
from feather_engine_dbos import DbosEngine

pytestmark = pytest.mark.postgres

WORKER = Path(__file__).parent / "echo_worker.py"


def key() -> str:
    return uuid.uuid4().hex


@pytest.fixture(scope="module")
def engine(pg_url: str) -> Iterator[DbosEngine]:
    engine = DbosEngine(pg_url)
    yield engine
    engine.close()


@pytest.fixture(scope="module")
def sys_db(pg_url: str, engine: DbosEngine) -> Iterator[DBOSClient]:
    client = DBOSClient(system_database_url=pg_url)
    yield client
    client.destroy()


@pytest.fixture(autouse=True)
def _nothing_left_waiting(sys_db: DBOSClient) -> Iterator[None]:
    """The submit tests leave tasks that no worker could run. Cancel them so that they don't clog a queue."""
    yield
    left = sys_db.list_workflows(status=["ENQUEUED", "PENDING"], load_input=False, load_output=False)
    if left:
        sys_db.cancel_workflows([row.workflow_id for row in left])


def workflow_row(sys_db: DBOSClient, ref: str) -> Any:
    [row] = sys_db.list_workflows(workflow_ids=[ref])
    return row


def test_submit_maps_key_priority_and_queue(engine, sys_db):
    ref = engine.submit(TaskCall("t", {"a": 1}, idempotency_key=key(), concurrency_key="ck", priority=2))
    row = workflow_row(sys_db, ref)
    assert (row.queue_name, row.queue_partition_key, row.priority) == ("feather-keyed", "ck", 1)
    assert row.name == "feather_task" and row.status == "ENQUEUED"


def test_priorities_map_to_dbos_priorities(engine, sys_db):
    got = [
        workflow_row(sys_db, engine.submit(TaskCall("t", {}, key(), priority=p))).priority for p in (2, 1, 0)
    ]
    assert got == [1, 2, 3]  # DBOS runs the lowest number first


def test_submit_without_key_uses_free_queue(engine, sys_db):
    ref = engine.submit(TaskCall("t", {}, idempotency_key=key()))
    row = workflow_row(sys_db, ref)
    assert (row.queue_name, row.queue_partition_key) == ("feather-free", None)


def test_same_idempotency_key_returns_same_ref(engine, sys_db):
    k = key()
    first = engine.submit(TaskCall("t", {"a": 1}, idempotency_key=k))
    again = engine.submit(TaskCall("t", {"a": 2}, idempotency_key=k))
    assert first == again == k
    assert len(sys_db.list_workflows(workflow_ids=[k])) == 1


def test_pool_label_changes_the_queue(engine, sys_db):
    free = engine.submit(TaskCall("t", {}, key(), labels={"pool": "gpu"}))
    keyed = engine.submit(TaskCall("t", {}, key(), concurrency_key="ck", labels={"pool": "gpu"}))
    assert workflow_row(sys_db, free).queue_name == "feather-free@gpu"
    assert workflow_row(sys_db, keyed).queue_name == "feather-keyed@gpu"


def test_args_over_64kb_are_refused(engine):  # I8
    with pytest.raises(ValueError, match="64"):
        engine.submit(TaskCall("t", {"a": "x" * 70_000}, idempotency_key=key()))


def test_cancel_and_health(engine, sys_db):
    ref = engine.submit(TaskCall("t", {}, key(), labels={"pool": "nobody"}))
    assert engine.health()["queues"]["feather-free@nobody"] >= 1
    engine.cancel(ref)
    assert workflow_row(sys_db, ref).status == "CANCELLED"


def test_has_ended_says_whether_a_known_task_is_over(engine):
    ref = engine.submit(TaskCall("t", {}, key(), None))
    assert engine.has_ended(ref) is False
    engine.cancel(ref)
    assert engine.has_ended(ref) is True
    assert engine.has_ended("nobody-" + key()) is False


def test_recover_worker_with_nothing_to_recover(engine):
    assert engine.recover_worker("nobody-" + key()) == []


def test_no_private_dbos_api_is_used():
    sources = list(Path(feather_engine_dbos.__file__).parent.rglob("*.py"))
    assert sources
    assert not any("._recover" in p.read_text() or "DBOS._" in p.read_text() for p in sources)


# A real worker, in a process of its own: DBOS is one per process.


@pytest.fixture
def worker(pg_url: str) -> Iterator[Callable[[str], None]]:
    procs: list[subprocess.Popen[bytes]] = []

    def start(worker_id: str) -> None:
        procs.append(subprocess.Popen([sys.executable, str(WORKER), pg_url, worker_id]))

    yield start
    for proc in procs:
        proc.kill()
        proc.wait()


def wait_for(sys_db: DBOSClient, ref: str, status: str, timeout: float = 60) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = workflow_row(sys_db, ref)
        if row.status == status:
            return row
        time.sleep(0.2)
    raise AssertionError(f"{ref} is {workflow_row(sys_db, ref).status}, not {status}")


def test_a_worker_runs_a_task_and_its_steps(engine, sys_db, worker):
    ref = engine.submit(TaskCall("t.echo", {"n": 21}, key()))
    worker("w-echo-" + key())
    assert wait_for(sys_db, ref, "SUCCESS").output == {"first": 42, "attempt": 1}


def test_a_retry_is_a_new_attempt_and_keeps_finished_steps(engine, sys_db, worker):
    policy = RetryPolicy(
        max_attempts=3, first_delay=timedelta(milliseconds=100), max_delay=timedelta(seconds=1)
    )
    ref = engine.submit(TaskCall("t.flaky", {}, key(), retry=policy))
    worker("w-flaky-" + key())
    assert wait_for(sys_db, ref, "SUCCESS").output == {"attempt": 3, "step_runs": 1}


def test_a_permanent_error_is_not_retried(engine, sys_db, worker):
    ref = engine.submit(TaskCall("t.refuse", {}, key()))
    worker("w-refuse-" + key())
    row = wait_for(sys_db, ref, "ERROR")
    assert "no" in str(row.error)


def test_a_signal_wakes_a_waiting_task(engine, sys_db, worker):
    ref = engine.submit(TaskCall("t.waits", {}, key()))
    worker("w-waits-" + key())
    wait_for(sys_db, ref, "PENDING")
    engine.signal(ref, "go", {"ok": True})
    assert wait_for(sys_db, ref, "SUCCESS").output == {"got": {"ok": True}}


def test_a_killed_workers_task_is_recovered_by_another(engine, sys_db, pg_url):
    ref = engine.submit(TaskCall("t.waits", {}, key()))
    first = "w-dead-" + key()
    doomed = subprocess.Popen([sys.executable, str(WORKER), pg_url, first])
    try:
        wait_for(sys_db, ref, "PENDING")
    finally:
        doomed.kill()
        doomed.wait()
    second = subprocess.Popen([sys.executable, str(WORKER), pg_url, "w-alive-" + key()])
    try:
        assert engine.recover_worker(first) == [ref]
        engine.signal(ref, "go", {"ok": 1})
        assert wait_for(sys_db, ref, "SUCCESS").output == {"got": {"ok": 1}}
    finally:
        second.kill()
        second.wait()

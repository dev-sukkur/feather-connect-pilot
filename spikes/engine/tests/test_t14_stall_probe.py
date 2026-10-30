"""Hatchet-only probes to isolate the T13 engine stall. Sample the engine's Postgres sessions."""

import os
import time

import psycopg
import pytest

from conftest import count, record, seconds, wait_until
from spike import journal
from spike.port import TaskCall

HATCHET_PG = os.environ.get("SPIKE_HATCHET_PG_URL",
                            "postgresql://feather:feather_spike@127.0.0.1:25432/hatchet")


def engine_sessions() -> dict:
    with psycopg.connect(HATCHET_PG, autocommit=True) as c:
        rows = c.execute(
            "select state, count(*) from pg_stat_activity where datname = current_database()"
            " and pid <> pg_backend_pid() group by 1").fetchall()
    return {state or "none": n for state, n in rows}


def smoke(client, run: str, tag: str, n: int = 20, timeout: float = 90) -> bool:
    for i in range(n):
        client.submit(TaskCall(task="work", args={"ms": 10}, idempotency_key=f"{run}-{tag}-{i}"), run)
    try:
        wait_until(lambda: journal.scalar(
            "select count(*) from spike.exec_log where run = %s and event = 'end' and task_id like %s",
            (run, f"{run}-{tag}-%")) >= n, timeout=timeout, what=f"{tag} smoke tasks")
        return True
    except TimeoutError:
        return False


@pytest.fixture(autouse=True)
def _hatchet_only(engine_name):
    if engine_name != "hatchet":
        pytest.skip("Hatchet stall probe")


def test_t14a_kill_cycles_without_load(client, workers, run, engine_name):
    """Hypothesis A: abruptly killed workers leak engine-side DB connections."""
    samples = [engine_sessions()]
    for i in range(6):
        workers.start(f"k{i}", slots=16)
        workers.kill(f"k{i}")
        time.sleep(5)
        samples.append(engine_sessions())
    workers.start("survivor", slots=16)
    healthy = smoke(client, run, "after-kills")
    record(engine_name, "T14a kill cycles", sessions_after_each_kill=samples, smoke_ok=healthy)
    assert healthy


def test_t14b_bursts_with_live_worker(client, workers, run, engine_name):
    """Hypothesis B: a 1,000-task burst stalls the engine even with no worker killed."""
    workers.start("w1", slots=16)
    rounds = []
    for r in range(3):
        calls = [TaskCall(task="work", args={"ms": 10}, idempotency_key=f"{run}-r{r}-{i}")
                 for i in range(1000)]
        t0 = journal.scalar("select clock_timestamp()")
        for i in range(0, 1000, 50):
            client.submit_many(calls[i:i + 50], run)
        try:
            wait_until(lambda: journal.scalar(
                "select count(*) from spike.exec_log where run = %s and event = 'end' and task_id like %s",
                (run, f"{run}-r{r}-%")) >= 1000, timeout=240, what=f"burst {r}")
            last = journal.scalar("select max(at) from spike.exec_log where run = %s and task_id like %s",
                                  (run, f"{run}-r{r}-%"))
            rounds.append({"round": r, "ok": True, "tasks_per_s": round(1000 / seconds(t0, last), 1),
                           "sessions": engine_sessions()})
        except TimeoutError:
            rounds.append({"round": r, "ok": False, "ended": count(run, "end"),
                           "sessions": engine_sessions()})
            break
    record(engine_name, "T14b bursts, live worker", rounds=rounds)
    assert all(r["ok"] for r in rounds)

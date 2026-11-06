"""The engine conformance suite (design 6.8): what any engine behind the engine port must do.

Each test starts real worker processes on the test Postgres (see `conftest.py`) and judges what they did from
the journal `conformance.exec_log`, which the task bodies of `worker_main.py` write.
"""

import itertools
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any

import pytest
from dbos import DBOSClient

from feather_connect.engine.port import RetryPolicy, TaskCall
from feather_connect.storage.port import BudgetSpec, Window

# The default of 120 s is too short for the durable-sleep and rate tests. The waits inside the tests are all
# shorter than this, so that a stuck test fails by itself and still cleans up its workers.
pytestmark = [pytest.mark.postgres, pytest.mark.timeout(420)]

CHUNKS = {"c0": 1, "c1": 1, "c2": 1}


def test_1_priority_and_one_at_a_time_per_key(engine, workers, db, run, wait_until, record):
    calls = [
        TaskCall(
            "work",
            {"run": run, "ms": 300},
            f"{run}-{key}-{i}-p{i % 3}",
            concurrency_key=f"{run}-{key}",
            priority=i % 3,
        )
        for key in ("A", "B")
        for i in range(9)
    ]
    for call in calls:  # everything is queued before the worker starts, so that priority alone orders it
        engine.submit(call)
    workers.start("w1", slots=4)
    wait_until(lambda: db.count(run, "end") >= len(calls), 120, "all tasks")

    spans: dict[str, list[tuple[Any, Any, int]]] = {"A": [], "B": []}
    seen: dict[str, dict[str, Any]] = {}
    for e in db.events(run):
        seen.setdefault(e["task_id"], {})[e["event"]] = e["at"]
    for task_id, at in seen.items():
        _, key, _, prio = task_id.rsplit("-", 3)
        spans[key].append((at["start"], at["end"], int(prio[1:])))
    overlaps, order = 0, {}
    for key, items in spans.items():
        items.sort()
        overlaps += sum(1 for a, b in itertools.pairwise(items) if b[0] < a[1])
        order[key] = [p for _, _, p in items]
    record("T1", overlaps=overlaps, priority_order=order)
    assert overlaps == 0
    assert order == {"A": [2, 2, 2, 1, 1, 1, 0, 0, 0], "B": [2, 2, 2, 1, 1, 1, 0, 0, 0]}


def _effects(db, run: str) -> dict[str, int]:
    rows = db.query(
        "select effect_key, count(*) as n from conformance.effects where run = %s group by 1", (run,)
    )
    return {r["effect_key"].rsplit(":", 1)[1]: r["n"] for r in rows}


def _chunk_runs(db, run: str) -> dict[str, int]:
    runs: dict[str, int] = {}
    for e in db.events(run, "chunk_exec"):
        runs[e["info"]["chunk"]] = runs.get(e["info"]["chunk"], 0) + 1
    return runs


@pytest.mark.parametrize("takeover", ["same_worker_restarts", "other_worker_after_recover_worker"])
def test_2_kill_mid_step_no_loss_no_duplicates(engine, workers, db, run, wait_until, record, takeover):
    ref = engine.submit(TaskCall("chunks", {"run": run, "n": 3, "ms": 3000}, f"{run}-t"))
    workers.start("w1")
    wait_until(lambda: any(e["info"]["chunk"] == "c1" for e in db.events(run, "chunk_exec")), 60, "chunk c1")
    time.sleep(1)
    workers.kill("w1")
    if takeover == "same_worker_restarts":
        workers.start("w1")  # DBOS recovers the tasks of its own executor id on launch
    else:
        workers.start("w2")
        assert engine.recover_worker(workers.executor("w1")) == [ref]
    wait_until(lambda: db.count(run, "end") >= 1, 90, "task completion")
    time.sleep(1)  # a duplicate would show up now

    effects, runs = _effects(db, run), _chunk_runs(db, run)
    record("T2 " + takeover, effects=effects, chunk_runs=runs)
    assert effects == CHUNKS  # every effect happened, once
    assert runs["c0"] == 1  # the chunk that had completed was not redone
    assert db.count(run, "end") == 1


def test_3_deploy_same_version_mid_drain(engine, workers, db, run, wait_until, record):
    workers.start("w-old", slots=2)
    for i in range(50):
        engine.submit(TaskCall("work", {"run": run, "ms": 1000}, f"{run}-{i}"))
    wait_until(lambda: db.count(run, "end") >= 10, 60, "10 done on the old worker")
    workers.kill("w-old")
    workers.start("w-new", slots=2)  # the same application version: a deploy is a new worker
    engine.recover_worker(workers.executor("w-old"))
    wait_until(
        lambda: (
            db.scalar(
                "select count(distinct task_id) from conformance.exec_log where run = %s and event = 'end'",
                (run,),
            )
            >= 50
        ),
        150,
        "all 50 done",
    )
    time.sleep(1)

    ends = db.query(
        "select task_id, count(*) as n from conformance.exec_log where run = %s and event = 'end' group by 1",
        (run,),
    )
    versions = db.query(
        "select distinct application_version as v from dbos.workflow_status where workflow_uuid like %s",
        (f"{run}-%",),
    )
    record("T3", done=len(ends), duplicated=sum(1 for r in ends if r["n"] > 1), versions=versions)
    assert len(ends) == 50
    assert all(r["n"] == 1 for r in ends)
    assert [r["v"] for r in versions] == ["feather-1"]


def test_3b_rollback_of_latest_version_resumes_dequeue(pg_url, engine, workers, db, run, wait_until, record):
    workers.start("w-1", version="feather-1")
    workers.kill("w-1")
    workers.start("w-2", version="feather-2")  # a newer version makes feather-1 stale
    workers.kill("w-2")
    for i in range(5):
        engine.submit(TaskCall("work", {"run": run, "ms": 50}, f"{run}-{i}"))
    workers.start("w-1b", version="feather-1")
    time.sleep(4)
    stalled = db.count(run, "start")

    client = DBOSClient(system_database_url=pg_url)
    try:
        client.set_latest_application_version("feather-1", application_name="feather")
    finally:
        client.destroy()
    wait_until(lambda: db.count(run, "end") >= 5, 60, "dequeue to resume")
    record("T3b", started_while_stale=stalled)
    assert stalled == 0


def test_4_dedup(engine, workers, db, run, wait_until, record):
    workers.start("w1")
    call = TaskCall("work", {"run": run, "ms": 300}, f"{run}-dup")
    with ThreadPoolExecutor(5) as pool:
        refs = list(pool.map(lambda _: engine.submit(call), range(5)))
    refs += [engine.submit(call) for _ in range(5)]
    wait_until(lambda: db.count(run, "end") >= 1, 30, "the first execution")
    time.sleep(3)  # a duplicate would show up now

    record("T4", starts=db.count(run, "start"), ends=db.count(run, "end"), refs=len(set(refs)))
    assert set(refs) == {f"{run}-dup"}
    assert db.count(run, "start") == 1 and db.count(run, "end") == 1


def test_5_durable_sleep_across_restart(engine, workers, db, run, wait_until, record):
    workers.start("w1")
    engine.submit(TaskCall("nap", {"run": run, "seconds": 20}, f"{run}-nap"))
    wait_until(lambda: db.count(run, "nap_start") >= 1, 60, "the nap to start")
    time.sleep(5)
    workers.kill("w1")
    restart_s = workers.start("w1")
    wait_until(lambda: db.count(run, "nap_end") >= 1, 90, "the nap to end")

    elapsed = (db.events(run, "nap_end")[0]["at"] - db.events(run, "nap_start")[0]["at"]).total_seconds()
    record(
        "T5",
        elapsed_s=round(elapsed, 2),
        worker_restart_s=round(restart_s, 2),
        nap_starts=db.count(run, "nap_start"),
    )
    assert 20 <= elapsed <= 24
    assert db.count(run, "nap_start") == 1


def test_6_retry_timing(engine, workers, db, run, wait_until, record):
    policy = RetryPolicy(max_attempts=5, first_delay=timedelta(seconds=1), max_delay=timedelta(seconds=10))
    ok, bad = f"{run}-ok", f"{run}-bad"
    engine.submit(TaskCall("flaky", {"run": run, "fail_times": 3}, ok, retry=policy))
    engine.submit(TaskCall("flaky", {"run": run, "permanent": True}, bad, retry=policy))
    workers.start("w1")
    wait_until(lambda: db.count(run, "end", ok) >= 1, 60, "the flaky task to succeed")
    wait_until(lambda: engine.has_ended(bad), 30, "the permanent failure")

    tries = [e["at"] for e in db.events(run, "try") if e["task_id"] == ok]
    gaps = [(b - a).total_seconds() for a, b in itertools.pairwise(tries)]
    waits = [1, 2, 4]  # the policy's waits before the equal jitter cuts them to between half and all
    bad_tries = sum(1 for e in db.events(run, "try") if e["task_id"] == bad)
    record(
        "T6",
        attempts=len(tries),
        gaps_s=[round(g, 2) for g in gaps],
        policy_waits_s=waits,
        permanent_attempts=bad_tries,
    )
    assert len(tries) == 4
    for gap, wait in zip(gaps, waits):
        assert 0.5 * wait <= gap <= 1.1 * wait
    assert bad_tries == 1
    assert db.scalar("select status from dbos.workflow_status where workflow_uuid = %s", (bad,)) == "ERROR"


def test_6b_succeeded_step_not_rerun_on_retry(engine, workers, db, run, wait_until, record):
    policy = RetryPolicy(
        max_attempts=4, first_delay=timedelta(milliseconds=100), max_delay=timedelta(seconds=1)
    )
    engine.submit(TaskCall("step_then_fail", {"run": run}, f"{run}-s", retry=policy))
    workers.start("w1")
    wait_until(lambda: db.count(run, "end") >= 1, 60, "the task to succeed")

    step_runs = db.scalar(
        "select n from conformance.counter where run = %s and task_id = %s", (run, f"{run}-s")
    )
    attempts = [e["attempt"] for e in db.events(run, "try")]
    record("T6b", step_runs=step_runs, attempts=attempts)
    assert attempts == [1, 2, 3]
    assert step_runs == 1


def test_7_schema_isolation(pg_url, engine, workers, db, run, wait_until, record):
    import psycopg

    workers.start("w1")
    engine.submit(TaskCall("hold", {"run": run, "seconds": 15}, f"{run}-hold"))
    wait_until(lambda: db.count(run, "start") >= 1, 60, "the long task to run")

    table = f"feather.t7_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(pg_url, autocommit=True) as conn:
        conn.execute("set lock_timeout = '5s'")
        t0 = time.monotonic()
        conn.execute(f"create table {table} (id int)")
        conn.execute(f"alter table {table} add column note text")
        conn.execute(f"create index on {table} (id)")
        conn.execute(f"drop table {table}")
        ddl_s = time.monotonic() - t0
        row = conn.execute(
            "select count(*) from pg_locks l join pg_class r on r.oid = l.relation"
            " join pg_namespace n on n.oid = r.relnamespace"
            " where n.nspname = 'feather' and l.pid <> pg_backend_pid()"
            " and l.database = (select oid from pg_database where datname = current_database())"
        ).fetchone()
        assert row is not None
        foreign_locks = row[0]
    record("T7", ddl_s=round(ddl_s, 3), foreign_locks_on_feather=foreign_locks)
    assert ddl_s < 2
    assert foreign_locks == 0


def test_8_two_workers_share(engine, workers, db, run, wait_until, record):
    workers.start("w1", slots=4)
    workers.start("w2", slots=4)
    for i in range(200):
        engine.submit(TaskCall("work", {"run": run, "ms": 20}, f"{run}-{i}"))
    wait_until(lambda: db.count(run, "end") >= 200, 150, "200 tasks")
    time.sleep(2)  # a duplicate would show up now

    per_task = db.query(
        "select task_id, count(*) filter (where event = 'start') as s, count(*) filter (where event = 'end') as e"
        " from conformance.exec_log where run = %s group by 1",
        (run,),
    )
    per_worker = {
        r["worker"]: r["n"]
        for r in db.query(
            "select worker, count(*) as n from conformance.exec_log where run = %s and event = 'end' group by 1",
            (run,),
        )
    }
    record("T8", tasks=len(per_task), per_worker=per_worker)
    assert len(per_task) == 200
    assert all(r["s"] == 1 and r["e"] == 1 for r in per_task)
    assert len(per_worker) == 2 and min(per_worker.values()) > 0


def test_9_shared_budget(engine, workers, db, store, run, wait_until, record):
    # A bucket of 10 units that refills in 2 s (5 a second); low requests may use only 40% of it.
    name = f"vendor-{run}"
    store.budgets.define(BudgetSpec(name, (Window(10, 2),), {"high": 0.6}))
    defined_at = db.scalar("select extract(epoch from clock_timestamp())")
    workers.start("w1", slots=10)
    workers.start("w2", slots=10)
    for priority in ("high", "low"):
        for i in range(4):
            engine.submit(
                TaskCall(
                    "budgeted",
                    {"run": run, "budget": name, "units": 15, "priority": priority},
                    f"{run}-{priority}-{i}",
                )
            )
    wait_until(lambda: db.count(run, "end") >= 8, 150, "all budgeted tasks")

    grants = db.query(
        "select extract(epoch from at) as t, info->>'priority' as priority"
        " from conformance.exec_log where run = %s and event = 'grant' order by at",
        (run,),
    )
    # Replay the bucket from the grants: it must never go empty, and a low grant needs 1 + 6 tokens in it.
    tokens, last, worst_low, lowest = 10.0, float(defined_at), 10.0, 10.0
    for g in grants:
        tokens = min(10.0, tokens + 5 * (float(g["t"]) - last))
        last = float(g["t"])
        if g["priority"] == "low":
            worst_low = min(worst_low, tokens)
        tokens -= 1
        lowest = min(lowest, tokens)
    ends = {
        p: max(e["at"] for e in db.events(run, "end") if f"-{p}-" in e["task_id"]) for p in ("high", "low")
    }
    elapsed = float(grants[-1]["t"]) - float(defined_at)
    record(
        "T9",
        grants=len(grants),
        elapsed_s=round(elapsed, 1),
        lowest_tokens=round(lowest, 2),
        fewest_tokens_at_a_low_grant=round(worst_low, 2),
        failures=db.count(run, "fail"),
    )
    assert len(grants) == 120  # 8 tasks of 15 units
    assert len(grants) <= 10 + 5 * elapsed + 1  # never more than the bucket and its refill
    # The journal is written a few ms after each grant, so the replay is off by up to half a token.
    assert lowest >= -0.5
    assert worst_low >= 7 - 0.5  # the reserve held: low never took the last 6 tokens
    assert ends["high"] <= ends["low"]  # and the reserve let high finish first
    assert db.count(run, "fail") == 0


def test_10_dispatch_latency(engine, workers, db, run, wait_until, record):
    rate, seconds = 25, 30
    n = rate * seconds
    workers.start("w1", slots=16, poll_s=0.2)
    t0 = time.monotonic()
    for i in range(n):
        delay = t0 + i / rate - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        task_id = f"{run}-{i}"
        db.execute("insert into conformance.submit_log (run, task_id) values (%s, %s)", (run, task_id))
        engine.submit(TaskCall("work", {"run": run, "ms": 10}, task_id))
    achieved = n / (time.monotonic() - t0)
    wait_until(lambda: db.count(run, "end") >= n, 120, "all tasks")

    latencies = sorted(
        r["s"]
        for r in db.query(
            "select extract(epoch from min(e.at) - s.at)::float as s from conformance.submit_log s"
            " join conformance.exec_log e on e.run = s.run and e.task_id = s.task_id and e.event = 'start'"
            " where s.run = %s group by s.task_id, s.at",
            (run,),
        )
    )
    p95 = latencies[int(0.95 * (len(latencies) - 1))]
    lost = n - db.count(run, "end")
    record(
        "T10",
        submitted=n,
        achieved_rate=round(achieved, 1),
        p50_s=round(statistics.median(latencies), 3),
        p95_s=round(p95, 3),
        max_s=round(latencies[-1], 3),
        lost=lost,
    )
    assert len(latencies) == n and lost == 0
    assert p95 <= 1.0


def test_11_gateway_relay(engine, workers, db, run, wait_until, record):
    workers.start("w1")
    ref = engine.submit(TaskCall("relay", {"run": run}, f"{run}-relay"))
    wait_until(lambda: db.count(run, "waiting") >= 1, 60, "the task to wait for its signal")
    time.sleep(0.5)
    signalled_at = db.scalar("select clock_timestamp()")
    engine.signal(ref, "agent_result", {"rows": 42})
    wait_until(lambda: db.count(run, "end") >= 1, 30, "the task to finish")

    done = db.events(run, "relay_done")[0]
    resume_s = (done["at"] - signalled_at).total_seconds()
    record("T11", result=done["info"]["result"], signal_to_resume_s=round(resume_s, 3))
    assert done["info"]["result"] == {"rows": 42}
    assert resume_s <= 1.0


def test_12_recover_worker_keeps_key_order(engine, workers, db, run, wait_until, record):
    a, b = f"{run}-a", f"{run}-b"
    workers.start("w1")
    engine.submit(TaskCall("chunks", {"run": run, "n": 3, "ms": 3000}, a, concurrency_key="k1"))
    wait_until(lambda: any(e["info"]["chunk"] == "c1" for e in db.events(run, "chunk_exec")), 60, "chunk c1")
    engine.submit(TaskCall("work", {"run": run, "ms": 10}, b, concurrency_key="k1"))
    time.sleep(1)
    workers.kill("w1")
    workers.start("w2")
    time.sleep(5)
    blocked = db.count(run, "start", b) == 0  # the dead worker still holds the key
    recovered = engine.recover_worker(workers.executor("w1"))
    wait_until(lambda: db.count(run, "end") >= 2, 60, "both tasks to finish")

    a_end = next(e["at"] for e in db.events(run, "end") if e["task_id"] == a)
    b_start = next(e["at"] for e in db.events(run, "start") if e["task_id"] == b)
    record(
        "T12", blocked_until_recovered=blocked, recovered=recovered, b_started_after_a_ended=b_start >= a_end
    )
    assert blocked
    assert recovered == [a]
    assert b_start >= a_end
    assert all(n == 1 for n in _effects(db, run).values())

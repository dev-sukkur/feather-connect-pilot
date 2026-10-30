import statistics
import time

import pytest

from conftest import count, record, wait_until
from spike import journal
from spike.port import TaskCall

RATE, SECONDS = 50, 30


@pytest.mark.parametrize("poll_s", ["0.2", "1.0"])
def test_t10_dispatch_latency(client, workers, run, engine_name, poll_s):
    if engine_name != "dbos" and poll_s != "0.2":
        pytest.skip("polling interval is a DBOS setting")
    workers.start("w1", slots=16, env={"SPIKE_DBOS_POLL_S": poll_s})
    n = RATE * SECONDS
    t0 = time.monotonic()
    for i in range(n):
        delay = t0 + i / RATE - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        task_id = f"{run}-{i}"
        journal.execute(
            "insert into spike.submit_log (run, task_id, at) values (%s, %s, clock_timestamp())",
            (run, task_id))
        client.submit(TaskCall(task="work", args={"ms": 10}, idempotency_key=task_id), run)
    achieved_rate = n / (time.monotonic() - t0)
    try:
        wait_until(lambda: count(run, "end") >= n, timeout=120, what="all tasks")
    except TimeoutError:
        pass

    lat = [r["s"] for r in journal.query(
        "select extract(epoch from min(e.at) - s.at)::float as s from spike.submit_log s"
        " join spike.exec_log e on e.run = s.run and e.task_id = s.task_id and e.event = 'start'"
        " where s.run = %s group by s.task_id, s.at", (run,))]
    lat.sort()
    assert lat, "no task started"
    p95 = lat[int(0.95 * (len(lat) - 1))]
    lost = n - count(run, "end")
    record(engine_name, f"T10 poll={poll_s}", submitted=n, achieved_rate=round(achieved_rate, 1),
           p50_s=round(statistics.median(lat), 3), p95_s=round(p95, 3), max_s=round(lat[-1], 3),
           lost=lost)
    assert lost == 0 and p95 <= 1.0


def test_t10b_batched_dispatch(client, workers, run, engine_name):
    """50 tasks/s submitted as batches of 10 every 200 ms, the way the sealer submits work."""
    workers.start("w1", slots=16)
    batches, size = RATE * SECONDS // 10, 10
    submit_ms: list[float] = []
    t0 = time.monotonic()
    for b in range(batches):
        delay = t0 + b * 0.2 - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        ids = [f"{run}-{b}-{j}" for j in range(size)]
        with journal.pool().connection() as c:
            with c.cursor() as cur:
                cur.executemany("insert into spike.submit_log (run, task_id, at) values (%s, %s, clock_timestamp())",
                                [(run, i) for i in ids])
        s0 = time.perf_counter()
        client.submit_many([TaskCall(task="work", args={"ms": 10}, idempotency_key=i) for i in ids], run)
        submit_ms.append((time.perf_counter() - s0) * 1000)
    n = batches * size
    achieved_rate = n / (time.monotonic() - t0)
    try:
        wait_until(lambda: count(run, "end") >= n, timeout=120, what="all tasks")
    except TimeoutError:
        pass

    lat = sorted(r["s"] for r in journal.query(
        "select extract(epoch from min(e.at) - s.at)::float as s from spike.submit_log s"
        " join spike.exec_log e on e.run = s.run and e.task_id = s.task_id and e.event = 'start'"
        " where s.run = %s group by s.task_id, s.at", (run,)))
    assert lat, "no task started"
    p95 = lat[int(0.95 * (len(lat) - 1))]
    lost = n - count(run, "end")
    record(engine_name, "T10b batched", submitted=n, achieved_rate=round(achieved_rate, 1),
           batch_submit_ms_p50=round(statistics.median(submit_ms), 1),
           p50_s=round(statistics.median(lat), 3), p95_s=round(p95, 3), max_s=round(lat[-1], 3),
           lost=lost)
    assert lost == 0 and p95 <= 1.0

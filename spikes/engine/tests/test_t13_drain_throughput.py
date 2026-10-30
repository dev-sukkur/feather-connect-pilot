from conftest import count, record, seconds, wait_until
from spike import journal
from spike.port import TaskCall

N = 1000


def test_t13_drain_throughput(client, workers, run, engine_name):
    """Extra measurement for sizing: tasks/s one 16-slot worker drains from a full queue."""
    workers.start("w1", slots=16)
    workers.kill("w1")  # queues exist, nothing running
    calls = [TaskCall(task="work", args={"ms": 10}, idempotency_key=f"{run}-{i}") for i in range(N)]
    for i in range(0, N, 50):
        client.submit_many(calls[i:i + 50], run)
    workers.start("w1", slots=16)
    wait_until(lambda: count(run, "end") >= N, timeout=300, what=f"{N} tasks drained")

    first, last = journal.query(
        "select min(at) filter (where event = 'start') as first,"
        " max(at) filter (where event = 'end') as last from spike.exec_log where run = %s", (run,))[0].values()
    elapsed = seconds(first, last)
    record(engine_name, "T13 drain", tasks=N, slots=16, seconds=round(elapsed, 2),
           tasks_per_s=round(N / elapsed, 1))

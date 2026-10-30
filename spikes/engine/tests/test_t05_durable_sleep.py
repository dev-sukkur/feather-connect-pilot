import time

from conftest import count, events, record, seconds, wait_until
from spike.port import TaskCall


def test_t05_durable_sleep_across_restart(client, workers, run, engine_name):
    workers.start("w1")
    client.submit(TaskCall(task="nap", args={"seconds": 20}, idempotency_key=f"{run}-nap"), run)
    wait_until(lambda: count(run, "nap_start") >= 1, timeout=60, what="nap started")
    time.sleep(5)
    workers.kill("w1")
    restart_s = workers.start("w1")
    wait_until(lambda: count(run, "nap_end") >= 1, timeout=120, what="nap finished")

    start = events(run, "nap_start")[0]["at"]
    end = events(run, "nap_end")[0]["at"]
    elapsed = seconds(start, end)
    record(engine_name, "T5", sleep_s=20, elapsed_s=elapsed, worker_restart_s=restart_s,
           nap_start_logged=count(run, "nap_start"))
    assert 19.5 <= elapsed <= 20 + restart_s + 8

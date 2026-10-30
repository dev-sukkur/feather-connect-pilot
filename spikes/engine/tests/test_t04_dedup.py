import time
from concurrent.futures import ThreadPoolExecutor

from conftest import count, record, wait_until
from spike.port import TaskCall


def test_t04_dedup(client, workers, run, engine_name):
    workers.start("w1")
    call = TaskCall(task="work", args={"ms": 300}, idempotency_key=f"{run}-dup")

    with ThreadPoolExecutor(5) as pool:
        refs = list(pool.map(lambda _: client.submit(call, run), range(5)))
    refs += [client.submit(call, run) for _ in range(5)]

    wait_until(lambda: count(run, "end") >= 1, timeout=30, what="first execution")
    time.sleep(3)  # give any duplicate time to show up

    starts, ends = count(run, "start"), count(run, "end")
    record(engine_name, "T4", starts=starts, ends=ends, distinct_refs=len(set(refs)))
    assert starts == 1 and ends == 1

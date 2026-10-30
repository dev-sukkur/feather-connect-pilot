import time

from conftest import count, record, wait_until
from spike import journal
from spike.port import TaskCall


def test_t08_two_workers_share_work(client, workers, run, engine_name):
    workers.start("w1", slots=4)
    workers.start("w2", slots=4)
    for i in range(200):
        client.submit(TaskCall(task="work", args={"ms": 20}, idempotency_key=f"{run}-{i}"), run)
    wait_until(lambda: count(run, "end") >= 200, timeout=180, what="200 tasks")
    time.sleep(2)

    per_task = journal.query(
        "select task_id, count(*) filter (where event = 'start') as s,"
        " count(*) filter (where event = 'end') as e from spike.exec_log where run = %s group by 1",
        (run,))
    per_worker = {r["worker"]: r["n"] for r in journal.query(
        "select worker, count(*) as n from spike.exec_log where run = %s and event = 'end' group by 1",
        (run,))}
    not_once = [r for r in per_task if r["s"] != 1 or r["e"] != 1]
    record(engine_name, "T8", tasks=len(per_task), not_exactly_once=len(not_once), per_worker=per_worker)
    assert len(per_task) == 200 and not not_once
    assert len(per_worker) == 2 and min(per_worker.values()) > 0

import pytest

from conftest import count, record, wait_until
from spike import journal
from spike.port import TaskCall


# new_version: v2 deployed under a new application version (DBOS's default model).
# same_version_sweeper: both deploys share one application version and our control-plane
# sweeper hands the dead worker's in-flight tasks over. (DBOS's own auto_upgrade needs an
# enterprise key, so it is not an option for an MIT-licensed product.)
@pytest.mark.parametrize("mode", ["new_version", "same_version_sweeper"])
def test_t03_deploy_mid_drain(client, workers, run, engine_name, mode):
    v1, v2 = ("v1", "v2") if mode == "new_version" else ("same", "same")
    workers.start("w-v1", version=v1, slots=2)
    for i in range(50):
        client.submit(TaskCall(task="work", args={"ms": 1000}, idempotency_key=f"{run}-{i}"), run)
    wait_until(lambda: count(run, "end") >= 10, timeout=60, what="10 done on the old deploy")
    workers.kill("w-v1")
    workers.start("w-v2", version=v2, slots=2)
    if mode == "same_version_sweeper":
        journal.execute("insert into spike.recover_request (dead_worker) values ('w-v1')")
    try:
        wait_until(lambda: journal.scalar(
            "select count(distinct task_id) from spike.exec_log where run = %s and event = 'end'",
            (run,)) >= 50, timeout=150, what="all 50 done")
    except TimeoutError:
        pass

    rows = journal.query(
        "select task_id, count(*) filter (where event = 'end') as ends,"
        " max(worker) filter (where event = 'end') as worker"
        " from spike.exec_log where run = %s group by task_id", (run,))
    done = sum(1 for r in rows if r["ends"] >= 1)
    dup = sum(1 for r in rows if r["ends"] > 1)
    by_worker: dict[str, int] = {}
    for r in rows:
        if r["ends"]:
            by_worker[r["worker"]] = by_worker.get(r["worker"], 0) + 1
    record(engine_name, f"T3 {mode}", done=done, stranded=50 - done, duplicated=dup,
           completed_by_worker=by_worker)
    assert done == 50 and dup == 0

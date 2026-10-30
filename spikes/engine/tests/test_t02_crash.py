import time

from conftest import count, events, record, seconds, wait_until
from spike import journal
from spike.port import TaskCall


def _effect_counts(run: str) -> dict[str, int]:
    rows = journal.query(
        "select effect_key, count(*) as n from spike.effects where run = %s group by 1", (run,))
    return {r["effect_key"].rsplit(":", 1)[1]: r["n"] for r in rows}


def _chunk_execs(run: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for e in events(run, "chunk_exec"):
        out[e["info"]["chunk"]] = out.get(e["info"]["chunk"], 0) + 1
    return out


def _submit_and_kill_mid_chunk(client, workers, run) -> object:
    workers.start("w1")
    client.submit(TaskCall(task="chunks", args={"n": 3, "ms": 3000}, idempotency_key=f"{run}-t"), run)
    wait_until(lambda: any(e["info"]["chunk"] == "c1" for e in events(run, "chunk_exec")),
               timeout=60, what="chunk c1 running")
    time.sleep(1)
    killed_at = journal.scalar("select clock_timestamp()")
    workers.kill("w1")
    return killed_at


def test_t02a_crash_then_same_worker_restarts(client, workers, run, engine_name):
    killed_at = _submit_and_kill_mid_chunk(client, workers, run)
    restart_s = workers.start("w1")
    wait_until(lambda: count(run, "end") >= 1, timeout=120, what="task completion")

    resumed = [e["at"] for e in events(run) if e["at"] > killed_at][0]
    effects, execs = _effect_counts(run), _chunk_execs(run)
    record(engine_name, "T2a", effects=effects, chunk_execs=execs, worker_restart_s=restart_s,
           kill_to_resume_s=seconds(killed_at, resumed))
    assert effects == {"c0": 1, "c1": 1, "c2": 1}


def test_t02b_crash_then_other_worker_takes_over(client, workers, run, engine_name):
    killed_at = _submit_and_kill_mid_chunk(client, workers, run)
    workers.start("w2")
    auto = True
    try:
        wait_until(lambda: count(run, "end") >= 1, timeout=60, what="automatic takeover")
    except TimeoutError:
        auto = False
        swept_at = journal.scalar("select clock_timestamp()")
        journal.execute("insert into spike.recover_request (dead_worker) values ('w1')")
        wait_until(lambda: count(run, "end") >= 1, timeout=60, what="takeover after sweeper")

    ended = events(run, "end")[0]["at"]
    effects, execs = _effect_counts(run), _chunk_execs(run)
    record(engine_name, "T2b", automatic_takeover=auto, effects=effects, chunk_execs=execs,
           kill_to_done_s=seconds(killed_at, ended),
           sweep_to_done_s=None if auto else seconds(swept_at, ended))
    assert effects == {"c0": 1, "c1": 1, "c2": 1}


def _resume_dead_executor(client, executor: str) -> list[str]:
    """What the sweeper would do, using only DBOS's public client API."""
    dead = client._c.list_workflows(status="PENDING", executor_id=executor,
                                    load_input=False, load_output=False)
    for w in dead:
        client._c.resume_workflow(w.workflow_id, queue_name=w.queue_name)
    return [w.workflow_id for w in dead]


def test_t02c_public_api_takeover(client, workers, run, engine_name):
    """T2b again, but the control plane recovers the work with public API calls, from outside any worker."""
    killed_at = _submit_and_kill_mid_chunk(client, workers, run)
    workers.start("w2")
    swept_at = journal.scalar("select clock_timestamp()")
    resumed = _resume_dead_executor(client, "w1")
    wait_until(lambda: count(run, "end") >= 1, timeout=60, what="takeover after public resume")

    ended = events(run, "end")[0]["at"]
    effects, execs = _effect_counts(run), _chunk_execs(run)
    record(engine_name, "T2c public resume", resumed=len(resumed), effects=effects, chunk_execs=execs,
           kill_to_done_s=seconds(killed_at, ended), sweep_to_done_s=seconds(swept_at, ended))
    assert resumed == [f"{run}-t"]
    assert effects == {"c0": 1, "c1": 1, "c2": 1}
    assert execs["c0"] == 1  # the completed chunk is not redone


def test_t02d_public_api_takeover_keeps_key_order(client, workers, run, engine_name):
    """A dead worker's keyed task holds its key until resumed; the next task on that key then waits its turn."""
    workers.start("w1")
    client.submit(TaskCall(task="chunks", args={"n": 3, "ms": 3000}, idempotency_key=f"{run}-a",
                           concurrency_key="k1"), run)
    wait_until(lambda: any(e["info"]["chunk"] == "c1" for e in events(run, "chunk_exec")),
               timeout=60, what="chunk c1 running")
    client.submit(TaskCall(task="work", args={"ms": 10}, idempotency_key=f"{run}-b",
                           concurrency_key="k1"), run)
    time.sleep(1)
    workers.kill("w1")
    workers.start("w2")
    time.sleep(5)
    blocked = count(run, "start", f"{run}-b") == 0  # the dead worker still holds the key
    resumed = _resume_dead_executor(client, "w1")
    wait_until(lambda: count(run, "end") >= 2, timeout=60, what="both tasks finished")

    ends = {e["task_id"]: e["at"] for e in events(run, "end")}
    b_start = [e["at"] for e in events(run, "start") if e["task_id"] == f"{run}-b"][0]
    record(engine_name, "T2d public resume, keyed", blocked_until_resume=blocked, resumed=len(resumed),
           b_started_after_a_ended=b_start >= ends[f"{run}-a"], effects=_effect_counts(run))
    assert blocked
    assert b_start >= ends[f"{run}-a"]
    assert all(n == 1 for n in _effect_counts(run).values())

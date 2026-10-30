from conftest import count, events, record, seconds, wait_until
from spike.port import RetryPolicy, TaskCall

POLICY = RetryPolicy(max_attempts=5, first_delay_s=1.0, backoff=2.0)


def test_t06_retry_timing(client, workers, run, engine_name):
    workers.start("w1")
    ok, bad = f"{run}-ok", f"{run}-bad"
    client.submit(TaskCall(task="flaky", args={"fail_times": 3}, idempotency_key=ok, retry=POLICY), run)
    client.submit(TaskCall(task="flaky", args={"permanent": True}, idempotency_key=bad, retry=POLICY), run)
    wait_until(lambda: count(run, "end", ok) >= 1, timeout=90, what="flaky task to succeed")
    wait_until(lambda: count(run, "fail", bad) + count(run, "end", bad) >= 1, timeout=60,
               what="permanent failure recorded")

    tries = [e["at"] for e in events(run, "try") if e["task_id"] == ok]
    gaps = [round(seconds(a, b), 2) for a, b in zip(tries, tries[1:])]
    bad_tries = sum(1 for e in events(run, "try") if e["task_id"] == bad)
    record(engine_name, "T6", attempts=len(tries), gaps_s=gaps, expected_gaps_s=[1, 2, 4],
           permanent_attempts=bad_tries, permanent_failed=count(run, "fail", bad))
    assert len(tries) == 4
    for gap, expected in zip(gaps, [1, 2, 4]):
        assert 0.5 * expected <= gap <= 1.5 * expected + 0.5
    assert bad_tries == 1

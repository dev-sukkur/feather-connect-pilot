from conftest import count, record, wait_until
from spike import budget, journal
from spike.port import TaskCall


def test_t09_shared_budget_with_reserve(client, workers, run, engine_name):
    name = f"vendor-{run}"
    budget.define(name, limit_units=10, period_s=2.0, reserve_high=0.6)
    workers.start("w1", slots=10)
    for prio in ("high", "low"):
        for i in range(4):
            client.submit(TaskCall(task="budgeted", args={"budget": name, "units": 20, "priority": prio},
                                   idempotency_key=f"{run}-{prio}-{i}"), run)
    wait_until(lambda: count(run, "end") >= 8, timeout=180, what="all budgeted tasks")

    windows = journal.query(
        "select window_start, count(*) as total, count(*) filter (where priority = 'high') as high"
        " from spike.budget_grant where name = %s group by 1 order by 1", (name,))
    max_total = max(w["total"] for w in windows)
    max_low = max(w["total"] - w["high"] for w in windows)
    # Saturated windows are the contended ones: there high must have had its reserve.
    saturated = [w for w in windows if w["total"] == 10]
    min_high = min((w["high"] for w in saturated), default=0)
    failures = count(run, "fail")
    record(engine_name, "T9", windows=len(windows), max_units_per_window=max_total,
           max_low_per_window=max_low, saturated_windows=len(saturated),
           min_high_in_saturated_window=min_high, failures=failures)
    assert max_total <= 10 and max_low <= 4
    assert saturated and min_high >= 6 and failures == 0

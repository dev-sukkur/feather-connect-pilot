from conftest import count, events, record, wait_until
from spike.port import TaskCall


def test_t01_priority_and_one_at_a_time(client, workers, run, engine_name):
    workers.start("w1", slots=4)
    workers.kill("w1")  # queues now exist; nothing runs while we fill them

    calls = [
        TaskCall(task="work", args={"ms": 300}, idempotency_key=f"{run}-{key}-{i}-p{i % 3}",
                 concurrency_key=f"{run}-{key}", priority=i % 3)
        for key in ("A", "B") for i in range(9)
    ]
    for c in calls:
        client.submit(c, run)
    workers.start("w1", slots=4)
    wait_until(lambda: count(run, "end") >= len(calls), timeout=120, what="all tasks")

    spans: dict[str, list[tuple]] = {"A": [], "B": []}
    by_task: dict[str, dict] = {}
    for e in events(run):
        by_task.setdefault(e["task_id"], {})[e["event"]] = e["at"]
    for task_id, ev in by_task.items():
        key, prio = task_id.split("-")[-3], int(task_id.split("-p")[-1])
        spans[key].append((ev["start"], ev["end"], prio))

    overlaps, orders = 0, {}
    for key, items in spans.items():
        items.sort()
        overlaps += sum(1 for a, b in zip(items, items[1:]) if b[0] < a[1])
        orders[key] = [p for _, _, p in items]
    record(engine_name, "T1", overlaps=overlaps, priority_order=orders)
    assert overlaps == 0
    for key, prios in orders.items():
        assert prios == sorted(prios, reverse=True), f"key {key} ran in order {prios}"

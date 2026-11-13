"""M3.1 plan 1 O1: kill the only worker and `worker_missing` fires, critical, from the server; start one again and it
clears. Alerts come from the server, so this needs no worker to say that there is none."""

from typing import Any

import pytest

pytestmark = [pytest.mark.postgres, pytest.mark.chaos]


def firing(world: Any) -> list[Any]:
    return [a for a in world.store.alerts.list_alerts("firing") if a.rule == "worker_missing"]


@pytest.mark.timeout(480)
def test_killing_the_only_worker_fires_worker_missing_and_a_new_one_clears_it(world: Any) -> None:
    world.store.alerts.set_rule("worker_missing", True, {"after": "15s"}, [])  # the default is 2m
    worker = world.start_worker()
    world.start_server()
    world.wait(lambda: world.store.alerts.rules(), 10, "the rule")
    assert firing(world) == []

    world.kill(worker)
    world.wait(lambda: firing(world), 150, "worker_missing to fire")
    [alert] = firing(world)
    assert alert.severity == "critical" and alert.subject.startswith("environment:")

    world.start_worker(worker)
    world.wait(lambda: not firing(world), 150, "worker_missing to clear")

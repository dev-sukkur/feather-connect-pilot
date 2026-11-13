"""M3.1 plan 1 F3 under chaos: a sink with an availability window (H19).

A run whose sink's window is closed waits, visibly, and a worker killed while it waits, and again mid-apply once the
window has opened, loses and duplicates nothing. Webhook events that arrive while the window is closed are accepted and
spooled at once, and reach the sink once each when it opens.

The window opens at a whole minute two to three minutes ahead, which leaves time to start a worker before it.
"""

import json
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

pytestmark = [pytest.mark.postgres, pytest.mark.chaos]

EVENTS = 30
KEYS = {str(n) for n in range(1, 101)}


def soon() -> datetime:
    return (datetime.now(UTC) + timedelta(minutes=3)).replace(second=0, microsecond=0)


def waits(world: Any, connection: str) -> list[Any]:
    return world.store.budgets.waits(f"window:{connection}")


def post(world: Any, event: dict[str, Any]) -> int:
    request = urllib.request.Request(
        f"{world.server_address}/ingress/default/chaos-project/hooks",
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {world.hook_token}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status


@pytest.mark.timeout(540)
def test_a_worker_killed_while_waiting_for_a_window_and_again_mid_apply_writes_each_record_once(world):
    opens = world.with_window("win_sink", soon())
    point = "apply:win_sink:2"
    world.arm(point)
    worker = world.start_worker()
    run = world.start_run("windowed")

    world.wait(lambda: waits(world, "win_sink"), 120, "the wait for the window to be noted")
    [wait] = waits(world, "win_sink")
    assert wait.until == opens and wait.run_id == run
    assert datetime.now(UTC) < opens and world.sink_keys("win_sink") == set()  # nothing written while closed
    world.kill(worker)  # while it sleeps until the window opens
    world.start_worker(worker)

    [who] = world.wait_for_point(point, timeout=300)
    assert datetime.now(UTC) >= opens  # it waited for the window, through the restart
    assert world.worker_of(who) == worker
    world.kill(worker)  # mid-apply, inside the window
    world.start_worker(worker)

    assert world.wait_for_run(run, timeout=200) == "succeeded", world.state()
    assert world.sink_keys("win_sink") == KEYS
    assert world.deliveries("win_sink") == ["applied"]
    assert world.max_writes_per_key() == 1
    [first, *again] = world.attempts("win_sink", 2)
    assert first == (who, False) and again and again[-1] == (world.who(worker), True)


@pytest.mark.timeout(480)
def test_webhook_events_are_spooled_at_once_and_delivered_once_when_the_sink_s_window_opens(world):
    opens = world.with_window("push_sink", soon())
    world.start_worker()
    world.start_server()
    assert [post(world, {"id": n}) for n in range(1, EVENTS + 1)] == [202] * EVENTS  # accepted while closed

    world.wait(lambda: waits(world, "push_sink"), 120, "the wait for the window to be noted")
    assert all(wait.until == opens for wait in waits(world, "push_sink"))
    sealed = [
        v for v in world.store.versions.list_versions(world.name("hooks"), limit=100) if v.status == "sealed"
    ]
    assert sum(v.upserts for v in sealed) == EVENTS  # spooled and sealed, not lost while the sink is closed
    assert datetime.now(UTC) < opens and world.sink_keys("push_sink") == set()

    keys = {str(n) for n in range(1, EVENTS + 1)}
    world.wait(lambda: world.sink_keys("push_sink") == keys, 300, "every event to reach the sink")
    assert datetime.now(UTC) >= opens
    assert world.max_writes_per_key() == 1, world.state()
    world.wait(lambda: set(world.deliveries("push_sink")) == {"applied"}, 60, "every delivery to be applied")

"""M3 plan 1 P2: NFR-05, a server restart costs a running task at most one retry, and a slow network between the
workers and Postgres loses and duplicates nothing, even when a worker is killed mid-apply on it.

The Postgres restart is `test_kill_mid_apply.test_postgres_restart_during_apply`.
"""

from typing import Any

import pytest
from real_stack import kill_abruptly

pytestmark = [pytest.mark.postgres, pytest.mark.chaos]

ROUTES = ("slow_a", "slow_b", "slow_c")  # as in test_kill_mid_apply
EXPECTED_KEYS = {str(n) for n in range(1, 101)}
CHUNK = 2
POINT = f"apply:slow_a:{CHUNK}"


def assert_delivered_once_everywhere(world: Any) -> None:
    for route in ROUTES:
        assert world.sink_keys(route) == EXPECTED_KEYS, route
        assert world.deliveries(route) == ["applied"], route
    assert world.max_writes_per_key() == 1


def assert_landed_mid_apply(world: Any, who: str) -> None:
    assert world.attempts("slow_a", CHUNK) == [(who, False)]
    assert world.rows_of_chunk("slow_a", CHUNK) == 10


@pytest.mark.timeout(420)
def test_a_server_restart_costs_a_running_task_at_most_one_retry(world: Any) -> None:
    world.start_server()
    world.arm(POINT)
    worker = world.start_worker()
    run = world.start_run("main")

    [who] = world.wait_for_point(POINT)
    assert_landed_mid_apply(world, who)
    kill_abruptly(world.procs["server"])  # the server goes, mid-task; the worker holds the task
    world.start_server("server")  # and comes back on the same port
    world.release(POINT)

    assert world.wait_for_run(run, timeout=300) == "succeeded", world.state()
    assert_delivered_once_everywhere(world)
    attempts = world.attempts("slow_a", CHUNK)
    assert len(attempts) <= 2 and attempts[-1][1], attempts  # NFR-05: done, with at most one retry
    assert world.procs[worker].poll() is None  # the worker lived through it


@pytest.mark.timeout(900)
def test_a_slow_network_to_postgres_loses_and_duplicates_nothing_through_a_kill(
    slow_world: tuple[Any, Any],
) -> None:
    world, proxy = slow_world
    world.arm(POINT)
    worker = world.start_worker()
    run = world.start_run("main")

    [who] = world.wait_for_point(POINT, timeout=300)
    assert_landed_mid_apply(world, who)
    world.kill(worker)
    world.start_worker(worker)

    assert world.wait_for_run(run, timeout=600) == "succeeded", world.state()
    assert_delivered_once_everywhere(world)
    assert world.attempts("slow_a", CHUNK)[-1] == (
        world.who(worker),
        True,
    )  # attempted again, by the new process
    assert proxy.chunks > 1000  # the network really was in the way

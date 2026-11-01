"""M1's exit criterion: a worker killed mid-task loses and duplicates nothing, on real workers running Feather's `Runtime`
on real DBOS.

A test acts when a connector says it is at a known line (`World.wait_for_point`), and never after a sleep, and asserts
that the moment it wanted is the one it got. `max_writes_per_key` counts effective writes, as `SlowSink` explains, and
each test also asserts that a chunk was really attempted again, so that none can pass by re-running nothing.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from feather_connect.runtime.sweeper import SweepReport, sweep_once

pytestmark = [pytest.mark.postgres, pytest.mark.chaos]

ROUTES = ("slow_a", "slow_b", "slow_c")
EXPECTED_KEYS = {str(n) for n in range(1, 101)}
CHUNK = 2
POINT = f"apply:slow_a:{CHUNK}"


def assert_delivered_once_everywhere(world: Any) -> None:
    for route in ROUTES:
        assert world.sink_keys(route) == EXPECTED_KEYS, route
        assert world.deliveries(route) == ["applied"], route
    assert world.max_writes_per_key() == 1


def assert_landed_mid_apply(world: Any, who: str) -> None:
    """The kill or the recovery found the one call of the chunk half done, in the process that was stopped there."""
    assert world.attempts("slow_a", CHUNK) == [(who, False)]
    assert world.rows_of_chunk("slow_a", CHUNK) == 10  # of 20: the sink holds half of the chunk


@pytest.mark.timeout(300)
def test_kill_9_mid_apply_loses_and_duplicates_nothing(world):
    world.arm(POINT)
    worker = world.start_worker()
    run = world.runtime.start_run("main")

    [who] = world.wait_for_point(POINT)
    assert world.worker_of(who) == worker
    assert_landed_mid_apply(world, who)
    world.kill(worker)
    assert_landed_mid_apply(world, who)  # and it died there
    assert world.deliveries("slow_a") == ["running"]

    world.start_worker(worker)
    assert world.wait_for_run(run) == "succeeded", world.state()

    assert_delivered_once_everywhere(world)
    [first, *again] = world.attempts("slow_a", CHUNK)
    assert first == (who, False)
    assert again and again[-1] == (
        world.who(worker),
        True,
    )  # the chunk was attempted again, by the new process


@pytest.mark.timeout(300)
def test_kill_9_with_no_restart_is_recovered_by_the_sweeper(world):
    world.arm(POINT)
    workers = {world.start_worker(), world.start_worker()}
    run = world.runtime.start_run("main")

    [who] = world.wait_for_point(POINT)
    victim = world.worker_of(who)
    assert_landed_mid_apply(world, who)
    world.kill(victim)
    [survivor] = workers - {victim}

    reports: list[SweepReport] = []

    def sweep_until_dead() -> bool:
        # 20 s against a heartbeat every 5 s, so that the survivor isn't called dead. Production uses 30 s.
        reports.append(
            sweep_once(
                world.store,
                world.runtime,
                world.engine,
                now=datetime.now(UTC),
                dead_after=timedelta(seconds=20),
            )
        )
        return any(victim in report.dead_workers for report in reports)

    world.wait(sweep_until_dead, 90, "the sweeper to declare the worker dead")
    assert not any(survivor in report.dead_workers for report in reports)
    recovered = [task for report in reports for task in report.recovered_tasks]
    assert any(task.startswith("apply:slow_a:") for task in recovered)

    assert world.wait_for_run(run) == "succeeded", world.state()
    assert_delivered_once_everywhere(world)
    attempted_by = [process for process, _ in world.attempts("slow_a", CHUNK)]
    assert (
        attempted_by[0] == who and world.who(survivor) in attempted_by[1:]
    )  # the survivor attempted it again


@pytest.mark.timeout(300)
def test_worker_wrongly_declared_dead_causes_no_duplicates(world):
    world.arm(POINT)
    first, second = world.start_worker(), world.start_worker()
    run = world.runtime.start_run("main")

    [who] = world.wait_for_point(POINT)
    live = world.worker_of(who)
    assert_landed_mid_apply(world, who)
    recovered = world.engine.recover_worker(live)  # it is alive, and stopped in the sink
    assert any(task.startswith("apply:slow_a:") for task in recovered)

    # The second execution overtakes the first, which is still in the sink, and finishes the delivery.
    world.wait(lambda: world.deliveries("slow_a") == ["applied"], 90, "the second execution to apply")
    attempts = world.attempts("slow_a", CHUNK)
    assert attempts[0] == (who, False) and attempts[-1][1]  # both ran: the first is still in there
    assert world.procs[live].poll() is None

    world.release(POINT)  # the first wakes up and writes the rest of its chunk, which the sink has already
    world.wait(lambda: world.attempts("slow_a", CHUNK)[0][1], 30, "the first execution to finish its chunk")
    [ref] = [task for task in recovered if task.startswith("apply:slow_a:")]
    world.wait(lambda: world.parked(live, ref), 60, "DBOS to stop the first execution at its step record")
    assert world.wait_for_run(run) == "succeeded", world.state()
    assert_delivered_once_everywhere(world)
    assert world.procs[live].poll() is None and {first, second} == set(world.procs)


@pytest.mark.timeout(300)
def test_kill_during_extract_seals_one_clean_version(world):
    point = "read:source:2"
    world.arm(point)
    worker = world.start_worker()
    run = world.runtime.start_run("main")

    [who] = world.wait_for_point(point)
    [version_id] = world.claimed_versions()
    assert len(world.spool.list_chunks(version_id, "full")) == 3  # the kill leaves a part of the read behind
    assert world.store.versions.sealed_for_run(run, "main_rows") == []
    world.kill(worker)

    world.start_worker(worker)
    assert world.wait_for_run(run) == "succeeded", world.state()

    [sealed] = world.store.versions.sealed_for_run(run, "main_rows")
    assert (sealed.version_id, sealed.upserts, sealed.deletes) == (version_id, 100, 0)
    assert world.store.versions.staged_for_run(run, "main_rows") == []
    chunks = world.spool.list_chunks(sealed.version_id)
    assert len(chunks) == 5 and sum(chunk.rows for chunk in chunks) == 100
    reads = world.reads("source")
    assert reads[0] == who and reads[-1] == world.who(worker)  # the new process really read it again
    assert_delivered_once_everywhere(world)


@pytest.mark.timeout(420)
def test_postgres_restart_during_apply(world_on_own_server):
    world, server = world_on_own_server
    world.arm(POINT)
    worker = world.start_worker()
    run = world.runtime.start_run("main")

    [who] = world.wait_for_point(POINT)
    assert_landed_mid_apply(world, who)
    world.conn.close()  # the test's own session goes with the server
    server.stop()
    server.start()
    world.reconnect()
    assert world.procs[worker].poll() is None  # the worker lived through it

    assert world.wait_for_run(run, timeout=300) == "succeeded", world.state()
    assert_delivered_once_everywhere(world)
    assert world.procs[worker].poll() is None
    attempts = world.attempts("slow_a", CHUNK)
    assert attempts[0] == (who, False) and attempts[-1] == (who, True) and len(attempts) >= 2

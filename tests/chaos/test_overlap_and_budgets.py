"""Two concerns of the F4 and F12 fixes that only real DBOS can answer: durable sleeps and retries, and step records that
two executions of one workflow share."""

import time

import pytest

from feather_connect.storage.port import BudgetSpec, Window

pytestmark = [pytest.mark.postgres, pytest.mark.chaos]

KEYS = {str(n) for n in range(1, 101)}


@pytest.mark.timeout(300)
def test_a_recovered_extract_waits_for_the_stopped_one_and_the_run_succeeds(world):
    """F4's liveness concern, as hosts now settle it: an extract holds an advisory lock on its dataset's partition
    (flow packages §7.1), so the copy `recover_worker` starts elsewhere can't read beside the stopped one. It waits;
    the stopped one, refused its next step record because it no longer owns the workflow, is parked by DBOS and lets
    go; the copy then goes on from the steps recorded, seals one whole version, and no attempt is used up."""
    point = "read:source:1"
    world.arm(point)  # the first execution only: the copy goes through
    world.start_worker(), world.start_worker()
    run = world.start_run("main")

    [older] = world.wait_for_point(point)
    recovered = world.engine.recover_worker(world.host_of(world.worker_of(older)))
    [extract_ref] = [task for task in recovered if task.startswith("extract:")]
    time.sleep(10)  # time for the copy to start on either host: it waits for the lock, and reads nothing
    assert len(world.reads("source")) == 1, world.state()

    world.release(
        point
    )  # its next step is refused, and DBOS parks it, which lets go of the lock; the copy goes on
    world.wait(
        lambda: world.parked(world.worker_of(older), extract_ref), 60, "the older execution to be parked"
    )

    assert world.wait_for_run(run) == "succeeded", world.state()
    [extract] = [task for task in world.store.runs.list_tasks(run) if task.kind == "extract"]
    assert (extract.status, extract.attempt) == ("succeeded", 1), world.state()  # neither used up an attempt
    assert len(world.reads("source")) <= 2  # never beside each other: the copy read after, if at all
    [version] = world.store.versions.sealed_for_run(run, world.name("main_rows"))
    assert (version.upserts, version.deletes) == (100, 0)
    assert sum(chunk.rows for chunk in world.spool.list_chunks(version.version_id)) == 100
    for route in ("slow_a", "slow_b", "slow_c"):
        assert world.sink_keys(route) == KEYS, route
        assert world.deliveries(route) == ["applied"], route
    assert world.max_writes_per_key() == 1


@pytest.mark.timeout(420)
def test_a_multi_call_read_that_fits_its_budget_finishes_after_waiting(world):
    """F12: the read needs two calls of a window that holds two. It starts with none left, so it is sent away with nothing
    taken, then after one call, and it can only finish because the second wait, which DBOS sleeps, is the whole window.
    Over a minute a call, so that it is the runtime that waits, and not the handle.
    """
    world.start_worker()  # first, since a worker defines the budgets of the project
    world.store.budgets.define(BudgetSpec("fits_budget", (Window(2, 130),)))
    assert world.store.budgets.acquire("fits_budget", 2).granted
    run = world.start_run("fits")

    assert world.wait_for_run(run, timeout=330) == "succeeded", world.state()
    assert world.sink_keys("fits_sink") == {"1", "2"}
    assert len(world.reads("fits_source")) >= 3, world.state()  # each deferral starts the read again


@pytest.mark.timeout(180)
def test_a_read_bigger_than_its_budget_fails_for_good_and_names_the_budget(world):
    """F12: five calls, of a window that holds three."""
    world.start_worker()
    run = world.start_run("too_big")

    assert world.wait_for_run(run, timeout=120) == "failed", world.state()
    reason = world.store.runs.get_run(run).reason
    assert reason is not None and "budget 'small_budget'" in reason and "holds 3" in reason
    [extract] = [task for task in world.store.runs.list_tasks(run) if task.kind == "extract"]
    assert (extract.status, extract.attempt) == ("failed", 1)  # no retry: it can never fit
    assert len(world.reads("big_source")) == 1
    assert world.sink_keys("big_sink") == set() and world.deliveries("big_sink") == []

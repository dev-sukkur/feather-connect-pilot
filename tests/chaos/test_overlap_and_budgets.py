"""Two concerns of the F4 and F12 fixes that only real DBOS can answer: durable sleeps and retries, and step records that
two executions of one workflow share."""

import pytest

from feather_connect.storage.port import BudgetSpec, Window

pytestmark = [pytest.mark.postgres, pytest.mark.chaos]

KEYS = {str(n) for n in range(1, 101)}


@pytest.mark.timeout(300)
@pytest.mark.parametrize("older_goes_first", [True, False], ids=["older_first", "newer_first"])
def test_overlapping_extracts_end_with_the_run_succeeded(world, older_goes_first):
    """F4's liveness concern: each execution claims the version as it starts reading, so could a stale copy's retry claim
    it back, and the two use up their attempts shutting each other out?

    On real DBOS, no. `recover_worker` takes the workflow from the older execution, and DBOS refuses the step record of
    an execution that no longer owns its workflow, with a `BaseException` that no task code catches, and parks it. So the
    older one is refused once and never retries, whichever way round the two finish, and no attempt is used up.
    """
    point = "read:source:1"
    world.arm(point, holds=2)
    world.start_worker(), world.start_worker()
    run = world.runtime.start_run("main")

    [older] = world.wait_for_point(point)
    recovered = world.engine.recover_worker(world.worker_of(older))
    [extract_ref] = [task for task in recovered if task.startswith("extract:")]
    world.wait_for_point(
        point, count=2
    )  # the newer one is stopped too, having claimed the version after the older

    older_is_parked = lambda: world.parked(world.worker_of(older), extract_ref)
    sealed = lambda: world.store.versions.sealed_for_run(run, "main_rows")
    if older_goes_first:
        world.release(point, nth=0)  # its next chunk is refused (`StaleWriter`), and DBOS parks it
        world.wait(older_is_parked, 60, "the older execution to be parked")
        assert not sealed() and world.store.runs.get_run(run).status == "running"
        world.release(point, nth=1)
    else:
        world.release(point, nth=1)  # the newer one seals, and the older one then finds the version sealed
        world.wait(sealed, 60, "the newer execution to seal")
        world.release(point, nth=0)
        world.wait(older_is_parked, 60, "the older execution to be parked")

    assert world.wait_for_run(run) == "succeeded", world.state()
    [extract] = [task for task in world.store.runs.list_tasks(run) if task.kind == "extract"]
    assert (extract.status, extract.attempt) == ("succeeded", 1), world.state()  # neither used up an attempt
    assert (
        len(world.reads("source")) == 2
    )  # the two executions read once each: the overlap happened, and no retry
    [version] = sealed()
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
    run = world.runtime.start_run("fits")

    assert world.wait_for_run(run, timeout=330) == "succeeded", world.state()
    assert world.sink_keys("fits_sink") == {"1", "2"}
    assert len(world.reads("fits_source")) >= 3, world.state()  # each deferral starts the read again


@pytest.mark.timeout(180)
def test_a_read_bigger_than_its_budget_fails_for_good_and_names_the_budget(world):
    """F12: five calls, of a window that holds three."""
    world.start_worker()
    run = world.runtime.start_run("too_big")

    assert world.wait_for_run(run, timeout=120) == "failed", world.state()
    reason = world.store.runs.get_run(run).reason
    assert reason is not None and "budget 'small_budget'" in reason and "holds 3" in reason
    [extract] = [task for task in world.store.runs.list_tasks(run) if task.kind == "extract"]
    assert (extract.status, extract.attempt) == ("failed", 1)  # no retry: it can never fit
    assert len(world.reads("big_source")) == 1
    assert world.sink_keys("big_sink") == set() and world.deliveries("big_sink") == []

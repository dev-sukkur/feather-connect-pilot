import threading
import time
from collections.abc import Callable
from datetime import timedelta

import pytest

from feather_connect.clock import FrozenClock
from feather_connect.runtime.leader import AlwaysLeader, leader_key
from feather_connect.storage.postgres.leader import PostgresLeaderLock
from feather_server.app import AppState
from feather_server.loops import LoopRunner, default_loops

KEY = 424_242


def wait_until(check: Callable[[], bool], timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.01)
    raise AssertionError("timed out waiting")


def counting_runner(pg_url: str, name: str = "x") -> LoopRunner:
    return LoopRunner(name, lambda: None, 0.05, PostgresLeaderLock(pg_url, KEY))


def test_a_loop_runs_each_interval_until_stopped() -> None:
    runner = LoopRunner("x", lambda: None, 0.01, AlwaysLeader())
    runner.start()
    wait_until(lambda: runner.count >= 3)
    runner.stop()
    stopped_at = runner.count
    time.sleep(0.1)
    assert runner.count == stopped_at and runner.last_error is None


def test_a_raising_loop_keeps_running_and_records_the_error() -> None:
    calls: list[int] = []

    def raises_twice_then_ok() -> None:
        calls.append(1)
        if len(calls) <= 2:
            raise RuntimeError("boom")

    runner = LoopRunner("x", raises_twice_then_ok, 0.01, AlwaysLeader())
    runner.start()
    wait_until(lambda: runner.count >= 3)
    runner.stop()
    assert "boom" in str(runner.last_error)


def test_a_loop_that_does_not_lead_does_not_run() -> None:
    class Never(AlwaysLeader):
        def try_acquire(self) -> bool:
            return False

    runner = LoopRunner("x", lambda: None, 0.01, Never())
    runner.start()
    time.sleep(0.1)
    runner.stop()
    assert runner.count == 0 and not runner.leading


def test_stop_waits_for_the_pass_that_is_running() -> None:
    started, release = threading.Event(), threading.Event()
    finished: list[bool] = []

    def slow() -> None:
        started.set()
        release.wait(5)
        finished.append(True)

    runner = LoopRunner("x", slow, 0.01, AlwaysLeader())
    runner.start()
    assert started.wait(5)
    threading.Timer(0.1, release.set).start()
    runner.stop()
    assert finished == [True]


def test_default_loops_are_the_sweeper_and_retention(state: AppState) -> None:
    loops = default_loops(state)
    assert {(loop.name, loop.interval_s) for loop in loops} == {
        ("sweeper", 5.0),
        ("retention", 3600.0),
    }


def test_the_sweeper_and_retention_loops_do_their_work(state: AppState, clock: FrozenClock) -> None:
    state.store.workers.heartbeat("w1", {}, "sha", 1)
    clock.advance(timedelta(minutes=5))
    by_name = {loop.name: loop for loop in default_loops(state)}
    by_name["sweeper"].start()
    wait_until(lambda: state.store.workers.list_workers()[0].state == "dead")
    by_name["sweeper"].stop()
    by_name["retention"].start()
    wait_until(lambda: by_name["retention"].count >= 1)
    by_name["retention"].stop()
    assert by_name["retention"].last_error is None


@pytest.mark.postgres
def test_only_one_loop_runner_leads(pg_url: str) -> None:
    a, b = counting_runner(pg_url), counting_runner(pg_url)
    a.start()
    b.start()
    try:
        time.sleep(1)
        assert (a.count > 0) != (b.count > 0)
    finally:
        a.stop()
        b.stop()


@pytest.mark.postgres
def test_a_new_leader_takes_over(pg_url: str) -> None:
    a, b = counting_runner(pg_url), counting_runner(pg_url)
    a.start()
    time.sleep(0.5)
    b.start()
    try:
        assert a.count > 0 and b.count == 0
        a.stop()
        time.sleep(1)
        assert b.count > 0
    finally:
        a.stop()
        b.stop()


@pytest.mark.postgres
def test_loops_with_different_names_lead_independently(pg_url: str) -> None:
    assert leader_key("scheduler") != leader_key("sweeper")
    a = LoopRunner("one", lambda: None, 0.05, PostgresLeaderLock(pg_url, leader_key("one")))
    b = LoopRunner("two", lambda: None, 0.05, PostgresLeaderLock(pg_url, leader_key("two")))
    a.start()
    b.start()
    try:
        wait_until(lambda: a.count > 0 and b.count > 0)
    finally:
        a.stop()
        b.stop()


@pytest.mark.postgres
def test_a_lock_can_be_taken_again_after_release(pg_url: str) -> None:
    first, second = PostgresLeaderLock(pg_url, KEY + 1), PostgresLeaderLock(pg_url, KEY + 1)
    assert first.try_acquire() and first.try_acquire()  # holding it, and asking again, is fine
    assert not second.try_acquire()
    first.release()
    assert second.try_acquire()
    second.release()


@pytest.mark.postgres
def test_a_lock_whose_session_was_killed_is_lost(pg_url: str) -> None:
    import psycopg

    lock = PostgresLeaderLock(pg_url, KEY + 2)
    assert lock.try_acquire()
    with psycopg.connect(pg_url, autocommit=True) as admin:
        admin.execute(
            "select pg_terminate_backend(pid) from pg_locks where locktype = 'advisory' and objid = %s",
            (KEY + 2,),
        )
    other = PostgresLeaderLock(pg_url, KEY + 2)
    assert other.try_acquire()  # the server freed it
    assert not lock.try_acquire()  # and the first holder finds out, without raising
    other.release()
    lock.release()

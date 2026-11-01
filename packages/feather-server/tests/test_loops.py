import logging
import threading
import time
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.records import Record
from feather_connect.engine.local import LocalEngine
from feather_connect.project import Project
from feather_connect.runtime.leader import AlwaysLeader, leader_key
from feather_connect.runtime.retention import RetentionReport
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.sweeper import SweepReport
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import VersionDraft
from feather_connect.storage.postgres.leader import PostgresLeaderLock
from feather_connect.storage.spool import MemorySpool, records_to_table
from feather_server.app import AppState
from feather_server.loops import LoopRunner, SweepLog, default_loops, log_retention
from feather_server.settings import Settings

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


def test_default_loops_are_the_scheduler_sweeper_and_retention(state: AppState) -> None:
    loops = default_loops(state)
    assert {(loop.name, loop.interval_s) for loop in loops} == {
        ("scheduler", 1.0),
        ("sweeper", 5.0),
        ("retention", 3600.0),
        ("sealer", 0.25),
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


def test_the_scheduler_loop_starts_the_runs_that_are_due(clock: FrozenClock) -> None:
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    dataset = fc.Dataset("d", source=Source(rows=[{"id": 1}]), key="id", strategy=fc.strategy.replace())
    flow = fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("a"))], triggers=[fc.every("1m")])
    runtime = Runtime([flow], store, spool, engine, code_sha="sha", clock=clock)
    state = AppState(
        Settings(),
        Project("demo", Path("."), "dev", (flow,), {}, {}, {}, "sha"),
        store,
        spool,
        engine,
        runtime,
        clock,
    )
    scheduler = next(loop for loop in default_loops(state) if loop.name == "scheduler")
    scheduler.start()
    wait_until(lambda: scheduler.count >= 1)  # the first pass only notes the time
    clock.advance(timedelta(minutes=1))
    wait_until(lambda: store.runs.list_runs(flow="f") != [])
    scheduler.stop()
    assert store.runs.list_runs(flow="f")[0].trigger == "every:60s"


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


def resubmits(*deliveries: tuple[str, str]) -> SweepReport:
    return SweepReport(resubmitted_deliveries=list(deliveries))


def test_a_retention_report_warns_of_each_kept_day_with_why(caplog: pytest.LogCaptureFixture) -> None:  # F14
    with caplog.at_level(logging.INFO, logger="feather_server.loops"):
        log_retention(RetentionReport(kept_days={date(2026, 10, 1): "2 versions not sealed, such as v1"}))
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        ("WARNING", "retention kept 2026-10-01: 2 versions not sealed, such as v1")
    ]


def test_a_quiet_retention_pass_logs_nothing(caplog: pytest.LogCaptureFixture) -> None:  # F14
    with caplog.at_level(logging.INFO, logger="feather_server.loops"):
        log_retention(RetentionReport())
    assert caplog.records == []


def test_what_a_sweep_put_right_or_gave_up_on_is_a_warning(caplog: pytest.LogCaptureFixture) -> None:  # F14
    report = SweepReport(
        dead_workers=["w1"],
        recovered_tasks=["t1"],
        stranded_deliveries=[("warehouse", "v2")],
        discarded_versions=["v3"],
    )
    with caplog.at_level(logging.INFO, logger="feather_server.loops"):
        SweepLog()(report)
    assert {r.levelname for r in caplog.records} == {"WARNING"}
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "w1" in text and "t1" in text and "warehouse" in text and "v2" in text and "v3" in text


def test_a_quiet_sweep_logs_nothing(caplog: pytest.LogCaptureFixture) -> None:  # F14
    with caplog.at_level(logging.INFO, logger="feather_server.loops"):
        SweepLog()(SweepReport())
    assert caplog.records == []


def test_resubmits_are_logged_at_info_only_when_they_change(caplog: pytest.LogCaptureFixture) -> None:  # F14
    sweep_log = SweepLog()
    with caplog.at_level(logging.INFO, logger="feather_server.loops"):
        sweep_log(resubmits(("warehouse", "v1")))
        sweep_log(resubmits(("warehouse", "v1")))  # the same again 5 s later: already said
        assert len(caplog.records) == 1 and caplog.records[0].levelname == "INFO"
        assert "v1" in caplog.records[0].getMessage()
        sweep_log(resubmits(("warehouse", "v1"), ("warehouse", "v2")))
        assert len(caplog.records) == 2
        sweep_log(resubmits())  # it cleared, which is news too
        assert len(caplog.records) == 3
        sweep_log(resubmits())
        assert len(caplog.records) == 3


def test_a_long_list_is_logged_by_its_size_and_first_few(caplog: pytest.LogCaptureFixture) -> None:  # F14
    with caplog.at_level(logging.INFO, logger="feather_server.loops"):
        SweepLog()(SweepReport(discarded_versions=[f"v{n}" for n in range(1000)]))
    message = caplog.records[0].getMessage()
    assert "1000" in message and "v0" in message and "v999" not in message


def test_the_retention_loop_logs_its_report(
    state: AppState, clock: FrozenClock, caplog: pytest.LogCaptureFixture
) -> None:  # F14
    state.store.versions.stage(VersionDraft("v1", "d", "p", "r1", True, 1, 0, "h", None, None, "sha"))
    state.spool.put_chunk(
        "v1", 0, "data", records_to_table([Record("a", "upsert", {"x": 1}, 1)])
    )  # never sealed
    clock.advance(timedelta(days=15))
    retention = next(loop for loop in default_loops(state) if loop.name == "retention")
    with caplog.at_level(logging.INFO, logger="feather_server.loops"):
        retention.start()
        wait_until(lambda: retention.count >= 1)
        retention.stop()
    assert any(r.levelname == "WARNING" and "retention kept" in r.getMessage() for r in caplog.records)


def test_a_kept_inbox_day_is_logged_with_its_reason(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        log_retention(
            RetentionReport(kept_inbox_days={date(2026, 10, 1): "2 events not sealed, such as event 5"})
        )
    assert "inbox day 2026-10-01: 2 events not sealed, such as event 5" in caplog.text

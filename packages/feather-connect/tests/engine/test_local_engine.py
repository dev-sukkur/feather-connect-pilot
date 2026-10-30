from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any

import pytest

from feather_connect.clock import FrozenClock
from feather_connect.core import errors
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.engine import port
from feather_connect.engine.local import LocalEngine
from feather_connect.engine.port import RetryPolicy, TaskCall

START = datetime(2026, 10, 18, 9, 0, tzinfo=UTC)


def call(key: str, args: dict[str, Any] | None = None, task: str = "t", **fields: Any) -> TaskCall:
    return TaskCall(task, args or {}, idempotency_key=key, **fields)


def test_same_idempotency_key_runs_once():
    runs = []

    def body(ctx, args):
        runs.append((ctx.task_ref, ctx.attempt, args["n"], e.status(ctx.task_ref)))

    e = LocalEngine()
    e.register({"t": body})
    first = e.submit(call("k", {"n": 1}))
    second = e.submit(call("k", {"n": 2}))
    e.run_until_idle()
    third = e.submit(call("k", {"n": 3}))  # the task has finished, but its key is still taken
    e.run_until_idle()
    assert first == second == third == "k"
    assert runs == [("k", 1, 1, "running")]  # one run, on the first call's args
    assert e.status("k") == "succeeded"


def test_one_at_a_time_per_key_higher_priority_first():
    log = []

    def body(ctx, args):
        log.append("start:" + args["name"])
        log.append("end:" + args["name"])

    e = LocalEngine()
    e.register({"t": body})
    e.submit(call("low", {"name": "low"}, concurrency_key="A", priority=0))
    e.submit(call("high", {"name": "high"}, concurrency_key="A", priority=2))
    e.run_until_idle()
    assert log == ["start:high", "end:high", "start:low", "end:low"]


def test_a_parked_task_keeps_its_concurrency_key():
    log = []

    def body(ctx, args):
        ctx.step("start", lambda: log.append("start:" + args["name"]))
        if args["nap"]:
            ctx.sleep(timedelta(minutes=1))
        ctx.step("end", lambda: log.append("end:" + args["name"]))

    e = LocalEngine()
    e.register({"t": body})
    e.submit(call("first", {"name": "first", "nap": True}, concurrency_key="A"))
    e.submit(call("second", {"name": "second", "nap": False}, concurrency_key="A"))
    e.submit(call("other", {"name": "other", "nap": False}, concurrency_key="B"))
    e.run_until_idle()
    # "second" waits until "first" has ended, although "first" is asleep. "other" has its own key.
    assert log == ["start:first", "start:other", "end:other", "end:first", "start:second", "end:second"]


def test_retry_gaps_follow_policy():
    clock = FrozenClock(START)
    starts = []

    def body(ctx, args):
        starts.append(clock.now())
        if len(starts) <= 3:
            raise TransientError("busy")

    e = LocalEngine(clock)
    e.register({"t": body})
    e.submit(call("k", retry=RetryPolicy(first_delay=timedelta(seconds=1))))
    e.run_until_idle()
    gaps = [(later - earlier).total_seconds() for earlier, later in pairwise(starts)]
    assert gaps == [1.0, 2.0, 4.0]  # the whole delay each time: the engine takes the jitter at its maximum
    assert e.status("k") == "succeeded"


def test_permanent_error_is_not_retried():
    clock = FrozenClock(START)
    attempts = []

    def body(ctx, args):
        attempts.append(ctx.attempt)
        raise PermanentError("bad credentials")

    e = LocalEngine(clock)
    e.register({"t": body})
    e.submit(call("k"))
    e.run_until_idle()  # a failed task is not an error of the engine
    assert attempts == [1] and e.status("k") == "failed"
    assert clock.now() == START  # and there was no retry delay to wait out


@pytest.mark.parametrize("error", [TransientError("busy"), ValueError("not one of ours")])
def test_a_task_that_keeps_failing_ends_failed_after_its_last_attempt(error):
    clock = FrozenClock(START)
    attempts = []

    def body(ctx, args):
        attempts.append(ctx.attempt)
        raise error

    e = LocalEngine(clock)
    e.register({"t": body})
    e.submit(call("k", retry=RetryPolicy(max_attempts=3)))
    e.run_until_idle()
    assert attempts == [1, 2, 3] and e.status("k") == "failed"
    assert clock.now() - START == timedelta(seconds=30)  # waits of 10 s and 20 s, and none after the last


def test_succeeded_step_is_not_rerun_on_retry():
    calls_a = 0
    attempts = []

    def a():
        nonlocal calls_a
        calls_a += 1
        return "A"

    def body(ctx, args):
        attempts.append(ctx.attempt)
        assert ctx.step("a", a) == "A"  # the retry gets the saved result
        if ctx.attempt == 1:
            raise TransientError("try again")

    e = LocalEngine()
    e.register({"t": body})
    e.submit(call("k"))
    e.run_until_idle()
    assert calls_a == 1 and attempts == [1, 2]
    assert e.status("k") == "succeeded"


def test_a_step_that_failed_runs_again_on_retry():
    tries = []

    def flaky():
        tries.append(len(tries) + 1)
        if len(tries) == 1:
            raise TransientError("busy")
        return "done"

    def body(ctx, args):
        ctx.step("flaky", flaky)

    e = LocalEngine()
    e.register({"t": body})
    e.submit(call("k"))
    e.run_until_idle()
    assert tries == [1, 2] and e.status("k") == "succeeded"


def test_a_sleep_that_ended_is_not_repeated_on_retry():
    clock = FrozenClock(START)

    def body(ctx, args):
        ctx.sleep(timedelta(hours=1))
        if ctx.attempt == 1:
            raise TransientError("busy")

    e = LocalEngine(clock)
    e.register({"t": body})
    e.submit(call("k"))
    e.run_until_idle()
    assert e.status("k") == "succeeded"
    assert clock.now() - START == timedelta(hours=1, seconds=10)  # the hour once, then the first retry delay


def test_crash_then_recover_replays_saved_steps():
    calls = []

    def body(ctx, a):
        ctx.step("s1", lambda: calls.append("s1"))
        ctx.step("s2", lambda: calls.append("s2"))

    e = LocalEngine()
    e.register({"t": body})
    e.crash_before("s2")
    e.submit(TaskCall("t", {}, idempotency_key="k"))
    e.run_until_idle()
    assert calls == ["s1"] and e.status("k") == "crashed"
    e.recover()
    e.run_until_idle()
    assert calls == ["s1", "s2"] and e.status("k") == "succeeded"  # s1 came from the saved result


def test_a_crashed_task_holds_its_key_until_recovered():
    log = []

    def body(ctx, args):
        ctx.step("work", lambda: log.append(args["name"]))

    e = LocalEngine()
    e.register({"t": body})
    e.crash_before("work")
    e.submit(call("first", {"name": "first"}, concurrency_key="A"))
    e.submit(call("second", {"name": "second"}, concurrency_key="A"))
    e.run_until_idle()
    assert log == [] and (e.status("first"), e.status("second")) == ("crashed", "queued")
    e.recover()
    e.run_until_idle()
    assert log == ["first", "second"]


def test_sleep_parks_the_task_and_others_run():
    clock = FrozenClock(START)
    order = []
    b_saw = []
    a_attempts = []

    def a(ctx, args):
        a_attempts.append(ctx.attempt)
        ctx.step("before", lambda: order.append("a:before"))
        ctx.sleep(timedelta(hours=1))
        ctx.step("after", lambda: order.append("a:after"))

    def b(ctx, args):
        ctx.step("run", lambda: order.append("b"))
        ctx.step("clock", lambda: b_saw.append(clock.now()))

    e = LocalEngine(clock)
    e.register({"a": a, "b": b})
    e.submit(call("a", task="a"))
    e.submit(call("b", task="b"))
    e.run_until_idle()
    clock_moved = clock.now() - START
    assert order == ["a:before", "b", "a:after"] and clock_moved == timedelta(hours=1)
    assert b_saw == [START]  # the clock moves only when nothing can run, and "b" could
    assert a_attempts == [1, 1]  # "a" ran twice, from the top each time, but a replay is not a retry
    assert e.status("a") == e.status("b") == "succeeded"


def test_each_engine_has_its_own_virtual_clock_by_default():
    first, second = LocalEngine(), LocalEngine()
    start = first.clock.now()
    assert second.clock.now() == start  # both begin at the same fixed instant
    first.register({"nap": lambda ctx, args: ctx.sleep(timedelta(hours=1))})
    first.submit(call("k", task="nap"))
    first.run_until_idle()
    assert first.clock.now() - start == timedelta(hours=1)
    assert second.clock.now() == start


def test_signal_wakes_a_waiter_and_timeout_returns_none():
    clock = FrozenClock(START)
    woke = {}

    def waiter(ctx, args):
        payload = ctx.wait_signal("go", timedelta(minutes=10))
        woke[args["name"]] = (payload, clock.now() - START)

    def sender(ctx, args):
        ctx.step("send", lambda: e.signal("signalled", "go", {"n": 1}))

    e = LocalEngine(clock)
    e.register({"waiter": waiter, "sender": sender})
    e.submit(call("signalled", {"name": "signalled"}, task="waiter"))
    e.submit(call("silent", {"name": "silent"}, task="waiter"))
    e.submit(call("sender", task="sender"))
    e.run_until_idle()
    assert woke == {
        "signalled": ({"n": 1}, timedelta(0)),  # woken by the signal, with no time passing
        "silent": (None, timedelta(minutes=10)),  # nothing came, so it woke when the timeout ran out
    }


def test_a_signal_sent_before_the_wait_is_kept_in_order():
    got = []

    def body(ctx, args):
        results = [ctx.wait_signal("go", timedelta(minutes=1)) for _ in range(3)]
        ctx.step("record", lambda: got.extend(results))

    e = LocalEngine()
    e.register({"t": body})
    e.submit(call("k"))
    e.signal("k", "go", {"n": 1})
    e.signal("k", "go", {"n": 2})
    e.signal("k", "other", {"n": 9})  # another key: this task never waits for it
    e.run_until_idle()
    assert got == [{"n": 1}, {"n": 2}, None]  # the third wait finds nothing and times out


def test_cancel_stops_a_queued_task():
    ran = []
    e = LocalEngine()
    e.register({"t": lambda ctx, args: ran.append(args["name"])})
    e.submit(call("first", {"name": "first"}))
    e.submit(call("second", {"name": "second"}))
    e.cancel("first")
    e.run_until_idle()
    assert ran == ["second"]
    assert (e.status("first"), e.status("second")) == ("cancelled", "succeeded")
    e.cancel("second")  # a task that has ended stays as it ended
    assert e.status("second") == "succeeded"


def test_cancel_stops_a_parked_task_and_frees_its_key():
    clock = FrozenClock(START)
    log = []

    def napper(ctx, args):
        ctx.step("before", lambda: log.append("before"))
        ctx.sleep(timedelta(hours=1))
        ctx.step("after", lambda: log.append("after"))

    def canceller(ctx, args):
        ctx.step("cancel", lambda: e.cancel("napper"))

    def follower(ctx, args):
        ctx.step("run", lambda: log.append("follower"))

    e = LocalEngine(clock)
    e.register({"napper": napper, "canceller": canceller, "follower": follower})
    e.submit(call("napper", task="napper", concurrency_key="A"))
    e.submit(call("canceller", task="canceller"))
    e.submit(call("follower", task="follower", concurrency_key="A"))
    e.run_until_idle()
    assert log == ["before", "follower"]  # the key was free for "follower" once "napper" was cancelled
    assert e.status("napper") == "cancelled"
    assert clock.now() == START  # and its wake-up went with it: nothing was left to wait for


def test_recover_worker_returns_parked_crashed_tasks():
    done = []

    def crashy(ctx, args):
        ctx.step("work", lambda: done.append(args["name"]))

    def fine(ctx, args):
        ctx.step("other work", lambda: done.append(args["name"]))

    e = LocalEngine()
    e.register({"crashy": crashy, "fine": fine})
    e.crash_before("work")
    e.crash_before("work")  # each call arms one crash
    e.submit(call("a", {"name": "a"}, task="crashy"))
    e.submit(call("c", {"name": "c"}, task="fine"))
    e.submit(call("b", {"name": "b"}, task="crashy"))
    e.run_until_idle()
    assert done == ["c"]
    assert [e.status(k) for k in ("a", "b", "c")] == ["crashed", "crashed", "succeeded"]
    # One worker runs everything here, so any worker id recovers the crashed tasks, in submit order.
    assert e.recover_worker("some-worker") == ["a", "b"]
    assert [e.status(k) for k in ("a", "b", "c")] == ["queued", "queued", "succeeded"]
    e.run_until_idle()
    assert done == ["c", "a", "b"]
    assert [e.status(k) for k in ("a", "b", "c")] == ["succeeded"] * 3
    assert e.recover_worker("some-worker") == []


def test_a_runaway_loop_fails_loudly():
    started = []

    def body(ctx, args):
        n = args["n"]
        started.append(n)
        ctx.step("again", lambda: e.submit(call(f"t{n + 1}", {"n": n + 1})))

    e = LocalEngine()
    e.register({"t": body})
    e.submit(call("t0", {"n": 0}))
    with pytest.raises(RuntimeError, match="max_tasks=5"):
        e.run_until_idle(max_tasks=5)
    assert started == [0, 1, 2, 3, 4]  # five runs were allowed, and the sixth refused


def test_an_unregistered_task_fails_loudly():
    e = LocalEngine()
    e.submit(call("k", task="nobody"))
    with pytest.raises(LookupError, match="nobody"):
        e.run_until_idle()
    assert e.status("k") == "queued"  # nothing was lost: registering it lets the task run
    e.register({"nobody": lambda ctx, args: None})
    e.run_until_idle()
    assert e.status("k") == "succeeded"


def test_health_reports_the_queue_depth_and_counts_by_status():
    def work(ctx, args):
        ctx.step("work", lambda: None)

    e = LocalEngine()
    e.register({"work": work, "plain": lambda ctx, args: None})
    e.crash_before("work")
    e.submit(call("holder", task="work", concurrency_key="A"))
    e.submit(call("blocked", task="work", concurrency_key="A"))
    e.submit(call("ok", task="plain"))
    e.submit(call("dropped", task="plain"))
    e.cancel("dropped")
    e.run_until_idle()
    assert e.health() == {
        "queue_depth": 1,
        "by_status": {
            "queued": 1,
            "running": 0,
            "parked": 0,
            "succeeded": 1,
            "failed": 0,
            "cancelled": 1,
            "crashed": 1,
        },
    }


def test_submitted_and_task_ids_follow_submit_and_start_order():
    e = LocalEngine()
    e.register({"t": lambda ctx, args: None, "nap": lambda ctx, args: ctx.sleep(timedelta(minutes=1))})
    calls = [
        call("low", priority=0),
        call("high", priority=2),
        call("normal-1"),
        call("nap", task="nap"),
        call("normal-2"),
    ]
    for c in calls:
        e.submit(c)
    e.submit(call("high", {"again": True}))  # a repeated key is not a new task
    assert e.submitted() == calls and e.task_ids() == []  # nothing has started yet
    e.run_until_idle()
    assert e.submitted() == calls
    # Highest priority first, then submit order. "nap" starts once, although it runs again after its sleep.
    assert e.task_ids() == ["high", "normal-1", "nap", "normal-2", "low"]


def test_the_port_re_exports_permanent_error():
    assert port.PermanentError is errors.PermanentError


def test_retry_delay_doubles_up_to_the_cap():
    policy = RetryPolicy(first_delay=timedelta(seconds=10), max_delay=timedelta(seconds=60))
    delays = [policy.delay(n, rand=lambda: 1.0).total_seconds() for n in range(1, 6)]
    assert delays == [10, 20, 40, 60, 60]


def test_retry_delay_jitter_runs_from_half_to_all_of_the_delay():
    policy = RetryPolicy(first_delay=timedelta(seconds=10))
    assert policy.delay(1, rand=lambda: 0.0) == timedelta(seconds=5)
    assert policy.delay(1, rand=lambda: 0.5) == timedelta(seconds=7.5)
    assert policy.delay(1, rand=lambda: 1.0) == timedelta(seconds=10)
    assert timedelta(seconds=5) <= policy.delay(1) <= timedelta(seconds=10)  # random by default


def test_retry_delay_for_a_huge_attempt_number_is_the_cap():
    policy = RetryPolicy(max_attempts=1000)  # 10 s * 2**999 does not fit in a timedelta
    assert policy.delay(1000, rand=lambda: 1.0) == policy.max_delay

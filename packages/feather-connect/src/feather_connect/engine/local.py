"""LocalEngine: the engine for tests and local runs. It runs in one process, keeps everything in memory and is
not durable.

It follows the rules of a durable engine all the same. Whenever a task resumes, after a sleep, a wait for a
signal, a retry or a recovery, its body runs again from the top, and a `ctx.step` that has succeeded returns
the result it saved instead of running again. Tasks run one at a time on the caller's thread. Time is virtual:
the clock moves only when no task can run, and then it jumps to the earliest wake-up, so a sleep of an hour
takes no real time.
"""

import logging
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Literal, get_args

from feather_connect.clock import FrozenClock
from feather_connect.engine.port import Engine, PermanentError, TaskCall, TaskContext, TaskFn

log = logging.getLogger(__name__)

# queued: ready to run, or held back by a concurrency key. running: its body is executing.
# parked: asleep, waiting for a signal, or waiting out a retry delay.
# crashed: stopped by `crash_before`, until it is recovered. The other three have ended.
TaskStatus = Literal["queued", "running", "parked", "succeeded", "failed", "cancelled", "crashed"]
_ENDED: frozenset[TaskStatus] = frozenset({"succeeded", "failed", "cancelled"})

_Mailbox = dict[str, deque[Mapping[str, Any]]]  # the signals sent to a task and not yet received, by key

# Where the virtual clock starts when the engine is not given one.
_START = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(eq=False)
class _Wait:
    """A sleep or a wait for a signal that a task has reached. Once `done`, `result` is what it returned."""

    key: str | None  # the signal waited for, or None for a sleep
    done: bool = False
    result: Mapping[str, Any] | None = None

    def end(self, result: Mapping[str, Any] | None) -> None:
        self.done = True
        self.result = result


class _Park(BaseException):
    """Raised by `sleep` and `wait_signal` to stop the body and park the task until `wake_at`.

    It is a BaseException so that the `except Exception` in a body can't swallow it.
    """

    def __init__(self, wake_at: datetime, wait: _Wait) -> None:
        super().__init__()
        self.wake_at = wake_at
        self.wait = wait


class _Crash(BaseException):
    """Raised just before a step to play out the worker dying there. A body can't catch it either."""


@dataclass(eq=False)
class _Task:
    """What the engine keeps of a task. It stands in for the records of a durable engine."""

    call: TaskCall
    status: TaskStatus = "queued"
    attempt: int = 1
    started: bool = False
    wake_at: datetime | None = None  # while parked: when the task wakes if nothing wakes it sooner
    waiting: _Wait | None = None  # while parked in a sleep or a wait for a signal, not in a retry delay
    # Every sleep and wait for a signal that the body has reached, in the order it reached them.
    waits: list[_Wait] = field(default_factory=list[_Wait])
    mailbox: _Mailbox = field(default_factory=_Mailbox)

    @property
    def ref(self) -> str:
        return self.call.idempotency_key


class _Context(TaskContext):
    """What a task body is given. Every run of the body gets a new one, so waits are counted from the top."""

    def __init__(
        self, task: _Task, clock: FrozenClock, steps: dict[tuple[str, str], Any], crashes: list[str]
    ) -> None:
        self.task_ref = task.ref
        self.attempt = task.attempt
        self._task = task
        self._clock = clock
        self._steps = steps
        self._crashes = crashes
        self._waits_reached = 0

    def step[T](self, name: str, fn: Callable[[], T]) -> T:
        key = (self.task_ref, name)
        if key in self._steps:
            return self._steps[key]
        if name in self._crashes:  # a crash that is armed goes off only when the step would really run
            self._crashes.remove(name)
            raise _Crash
        result = fn()
        self._steps[key] = result
        return result

    def sleep(self, duration: timedelta) -> None:
        self._wait(None, duration)

    def wait_signal(self, key: str, timeout: timedelta) -> Mapping[str, Any] | None:
        return self._wait(key, timeout)

    def _wait(self, key: str | None, span: timedelta) -> Mapping[str, Any] | None:
        """Return what the next wait of the body returned, or park the task if the wait has yet to end.

        The first time a body reaches a wait, it is recorded on the task. After it has ended, every later
        run of the body, whether a replay or a retry, gets the same result without waiting again.
        """
        task = self._task
        index = self._waits_reached
        self._waits_reached += 1
        if index == len(task.waits):  # the body has not been this far before
            task.waits.append(_Wait(key))
            signals = task.mailbox.get(key) if key is not None else None
            if signals:  # the signal came before the body started to wait for it
                task.waits[index].end(signals.popleft())
        wait = task.waits[index]
        if not wait.done:
            raise _Park(self._clock.now() + span, wait)
        return wait.result


class LocalEngine(Engine):
    """An engine that runs tasks in this process, one at a time, on a virtual clock.

    `register` says which function each task name stands for, and `run_until_idle` runs what has been
    submitted. A task with a `concurrency_key` holds it from its first run until it ends, while it is parked
    or crashed too. `labels` are ignored, because there is only one worker.

    `clock` is the virtual clock. The engine moves it, so it has to be a `FrozenClock`. It starts at a fixed
    instant when none is given.
    """

    worker_id: ClassVar[str] = "local"

    def __init__(self, clock: FrozenClock | None = None) -> None:
        self.clock = FrozenClock(_START) if clock is None else clock
        self._registry: dict[str, TaskFn] = {}
        self._tasks: dict[str, _Task] = {}  # by reference, in submit order
        self._steps: dict[tuple[str, str], Any] = {}  # the saved results, by task reference and step name
        self._holders: dict[str, str] = {}  # concurrency key -> reference of the task that holds it
        self._crashes: list[str] = []  # names of the steps that are to crash, one entry per crash
        self._start_order: list[str] = []

    def register(self, tasks: Mapping[str, TaskFn]) -> None:
        """Make `tasks` the functions that run for their names. A name registered again is replaced."""
        self._registry.update(tasks)

    def crash_before(self, step_name: str) -> None:
        """Make the next task to reach a step called `step_name` crash just before the step runs.

        It plays out a worker that dies there. The task stays `crashed`, and keeps its concurrency key, until
        `recover` or `recover_worker`. Each call arms one crash.
        """
        self._crashes.append(step_name)

    def submit(self, call: TaskCall) -> str:
        """Queue the task and return its reference, which is its idempotency key.

        Submitting a key again queues nothing, whatever became of its task: the first call stands.
        """
        ref = call.idempotency_key
        if ref not in self._tasks:
            self._tasks[ref] = _Task(call)
        return ref

    def cancel(self, ref: str) -> None:
        """Cancel the task, wherever it waits: it never runs again, and it gives up its concurrency key.

        A task that has ended stays as it ended. So does one that is running, which can only be the caller.
        """
        task = self._tasks[ref]
        if task.status not in _ENDED and task.status != "running":
            self._end(task, "cancelled")

    def signal(self, ref: str, key: str, payload: Mapping[str, Any]) -> None:
        """Send `payload` to the task under `key`.

        A task that is waiting for `key` wakes with it. Otherwise it is kept, in order with the other signals
        sent under `key`, for the next time the task waits for `key`.
        """
        task = self._tasks[ref]
        if task.status == "parked" and task.waiting is not None and task.waiting.key == key:
            self._wake(task, payload)
        else:
            task.mailbox.setdefault(key, deque()).append(payload)

    def recover_worker(self, worker_id: str) -> list[str]:
        """Re-queue the tasks that crashed and return their references, in submit order.

        There is only one worker, so `worker_id` makes no difference.
        """
        crashed = [task for task in self._tasks.values() if task.status == "crashed"]
        for task in crashed:
            task.status = "queued"
        return [task.ref for task in crashed]

    def recover(self) -> list[str]:
        """Recover the worker, as `recover_worker` does."""
        return self.recover_worker(self.worker_id)

    def health(self) -> Mapping[str, Any]:
        """Return `queue_depth`, the number of queued tasks, and `by_status`, a count for each status."""
        by_status = dict.fromkeys(get_args(TaskStatus), 0)
        for task in self._tasks.values():
            by_status[task.status] += 1
        return {"queue_depth": by_status["queued"], "by_status": by_status}

    def status(self, ref: str) -> TaskStatus:
        """Return the status of the task."""
        return self._tasks[ref].status

    def submitted(self) -> list[TaskCall]:
        """Return the calls that made a task, in submit order. A call with a taken key made none."""
        return [task.call for task in self._tasks.values()]

    def task_ids(self) -> list[str]:
        """Return the references of the tasks that have started, in the order they first started."""
        return list(self._start_order)

    def run_until_idle(self, max_tasks: int = 10_000) -> None:
        """Run tasks until none can run and none is waiting for the clock.

        When nothing can run, the clock jumps to the earliest wake-up of a sleep, a wait for a signal or a
        retry delay, and the task that wakes runs again from the top. A task that fails has ended, so that
        doesn't raise. Tasks held back by the concurrency key of a crashed task stay queued.

        A run is one go at a body, and a task that sleeps or retries takes several. Needing more than
        `max_tasks` runs means a loop that never ends, and raises `RuntimeError`. A task nobody registered
        raises `LookupError` and stays queued.
        """
        runs = 0
        while True:
            self._wake_due()
            task = self._next_runnable()
            if task is None:
                wake_at = self._earliest_wake()
                if wake_at is None:
                    return
                self.clock.advance(wake_at - self.clock.now())
                continue
            if runs == max_tasks:
                raise RuntimeError(
                    f"run_until_idle stopped at max_tasks={max_tasks} with tasks still ready: is one looping?"
                )
            runs += 1
            self._run(task)

    def _wake_due(self) -> None:
        now = self.clock.now()
        for task in self._tasks.values():
            if task.status == "parked" and task.wake_at is not None and task.wake_at <= now:
                self._wake(task)

    def _earliest_wake(self) -> datetime | None:
        wake_times = [
            t.wake_at for t in self._tasks.values() if t.status == "parked" and t.wake_at is not None
        ]
        return min(wake_times, default=None)

    def _next_runnable(self) -> _Task | None:
        """Return the queued task to run next: the highest priority, and the earliest submitted among those.

        A task whose concurrency key is held by another task has to wait.
        """
        best: _Task | None = None
        for task in self._tasks.values():
            if task.status != "queued" or self._held_by_another(task):
                continue
            if best is None or task.call.priority > best.call.priority:
                best = task
        return best

    def _held_by_another(self, task: _Task) -> bool:
        key = task.call.concurrency_key
        return key is not None and self._holders.get(key, task.ref) != task.ref

    def _run(self, task: _Task) -> None:
        """Run the body of the task once, from the top, and record how it ended."""
        name = task.call.task
        if name not in self._registry:
            raise LookupError(f"no task is registered as {name!r}")
        if not task.started:
            task.started = True
            self._start_order.append(task.ref)
        if task.call.concurrency_key is not None:
            self._holders[task.call.concurrency_key] = task.ref
        task.status = "running"
        ctx = _Context(task, self.clock, self._steps, self._crashes)
        try:
            self._registry[name](ctx, task.call.args)
        except _Park as park:
            self._park(task, park.wake_at, park.wait)
        except _Crash:
            task.status = "crashed"
        except PermanentError as error:
            self._end(task, "failed", error)
        except Exception as error:  # noqa: BLE001 - a body may raise anything, and its retry policy decides
            self._retry_or_fail(task, error)
        else:
            self._end(task, "succeeded")

    def _retry_or_fail(self, task: _Task, error: Exception) -> None:
        policy = task.call.retry
        if task.attempt >= policy.max_attempts:
            self._end(task, "failed", error)
            return
        # The delay is on the virtual clock, and it is the whole delay: nothing in a test should be random.
        delay = policy.delay(task.attempt, rand=lambda: 1.0)
        task.attempt += 1
        self._park(task, self.clock.now() + delay, None)

    def _park(self, task: _Task, wake_at: datetime, wait: _Wait | None) -> None:
        task.status = "parked"
        task.wake_at = wake_at
        task.waiting = wait

    def _wake(self, task: _Task, result: Mapping[str, Any] | None = None) -> None:
        """End the wait, or the retry delay, that the task is parked in, so that it runs again."""
        if task.waiting is not None:
            task.waiting.end(result)
        task.status = "queued"
        task.wake_at = task.waiting = None

    def _end(self, task: _Task, status: TaskStatus, error: Exception | None = None) -> None:
        task.status = status
        task.wake_at = task.waiting = None
        key = task.call.concurrency_key
        if key is not None and self._holders.get(key) == task.ref:
            del self._holders[key]
        if error is not None:
            log.warning("task %s failed", task.ref, exc_info=error)

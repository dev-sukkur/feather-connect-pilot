"""LocalEngine: the engine for tests and local runs. It runs in one process, keeps everything in memory and is
not durable.

It follows the rules of a durable engine all the same. Whenever a task resumes, after a sleep, a wait for a
signal, a retry or a recovery, its body runs again from the top. Within an attempt the engine keeps a journal
of what the body did, in order, as a durable engine keeps its operation records: each sleep, each wait for a
signal and each step with its outcome. A replay walks the body through the journal. A step that succeeded
returns its saved result, a step that failed raises its saved error again, and a wait that ended returns what
ended it. A body that does anything else, because it read something that changed outside `ctx.step`, raises
`NotDurableError`. A retry is a new attempt with an empty journal, so its waits happen again, and only the
results of steps that succeeded carry over.

What goes into or out of a task is pickled, as a durable engine saves it: the args, the payload of a signal,
and the result or the error of a step. The body gets a fresh copy every time, and `NotDurableError` says when
something can't be pickled.

Tasks run one at a time on the caller's thread. Time is virtual: the clock moves only when no task can run,
and then it jumps to the earliest wake-up, so a sleep of an hour takes no real time.
"""

import logging
import pickle
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

_Mailbox = dict[str, deque[bytes]]  # the pickled signals sent to a task and not yet received, by key

# Where the virtual clock starts when the engine is not given one.
_START = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(eq=False)
class _Wait:
    """A sleep or a wait for a signal that the body reached. Once `done`, `result` is what it returned."""

    key: str | None  # the signal waited for, or None for a sleep
    done: bool = False
    result: bytes | None = None  # the pickled payload of the signal that ended it, if one did

    def end(self, result: bytes | None) -> None:
        self.done = True
        self.result = result


@dataclass(eq=False)
class _Step:
    """A step that the body ran. `error` is the pickled exception it raised, or None if it succeeded."""

    name: str
    error: bytes | None = None


_Entry = _Wait | _Step


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


class NotDurableError(BaseException):
    """A task would not survive on a durable engine.

    Either its body took another path when the engine replayed it, or something it handed over can't be
    pickled. It is raised out of `run_until_idle`, `submit` or `signal`. It is a BaseException so that the
    `except Exception` in a body, or in the engine, can't swallow it.
    """


def _pickled(value: object, subject: str) -> bytes:
    """Return `value` pickled, as a durable engine saves it. `subject` says what it is, for the error."""
    try:
        blob = pickle.dumps(value)
        pickle.loads(blob)  # a value that pickles but can't be rebuilt would only fail on some later replay
    except Exception as error:
        raise NotDurableError(
            f"{subject} can't be pickled, which a durable engine needs: {error!r}"
        ) from error
    return blob


def _waited(key: str | None) -> str:
    return "slept" if key is None else f"waited for the signal {key!r}"


def _describe(entry: _Entry) -> str:
    return _waited(entry.key) if isinstance(entry, _Wait) else f"ran step {entry.name!r}"


@dataclass(eq=False)
class _Task:
    """What the engine keeps of a task. It stands in for the records of a durable engine."""

    call: TaskCall
    args: bytes  # call.args, pickled when the task was submitted, so that every run gets a fresh copy
    status: TaskStatus = "queued"
    attempt: int = 1
    started: bool = False
    wake_at: datetime | None = None  # while parked: when the task wakes if nothing wakes it sooner
    waiting: _Wait | None = None  # while parked in a sleep or a wait for a signal, not in a retry delay
    # What the body did in this attempt, in order. A replay of the attempt is walked through it.
    journal: list[_Entry] = field(default_factory=list[_Entry])
    mailbox: _Mailbox = field(default_factory=_Mailbox)

    @property
    def ref(self) -> str:
        return self.call.idempotency_key


class _Context(TaskContext):
    """What a task body is given. Every run of the body gets a new one, and walks the journal from the top."""

    def __init__(
        self, task: _Task, clock: FrozenClock, steps: dict[tuple[str, str], bytes], crashes: list[str]
    ) -> None:
        self.task_ref = task.ref
        self.attempt = task.attempt
        self._task = task
        self._clock = clock
        self._steps = steps
        self._crashes = crashes
        self._position = 0  # how far along the journal this run of the body has got
        self._in_step = False

    def step[T](self, name: str, fn: Callable[[], T]) -> T:
        if self._in_step:  # on a durable engine too, a step called inside a step is an ordinary call
            return fn()
        key = (self.task_ref, name)
        entry = self._replayed()
        if entry is None:  # this call is new in this attempt
            if key in self._steps:  # an earlier attempt finished this step
                self._record(_Step(name))
                return pickle.loads(self._steps[key])
            return self._run_step(name, fn)
        if not isinstance(entry, _Step) or entry.name != name:
            raise self._diverged(f"ran step {name!r}", entry)
        if entry.error is not None:
            raise pickle.loads(entry.error)
        return pickle.loads(self._steps[key])

    def sleep(self, duration: timedelta) -> None:
        self._wait(None, duration)

    def wait_signal(self, key: str, timeout: timedelta) -> Mapping[str, Any] | None:
        return self._wait(key, timeout)

    def _run_step[T](self, name: str, fn: Callable[[], T]) -> T:
        """Run a step that is new in this attempt, and journal how it ended. A result is saved by name."""
        if name in self._crashes:  # a crash that is armed goes off only when the step would really run
            self._crashes.remove(name)
            raise _Crash
        self._in_step = True
        try:
            result = fn()
        except Exception as error:
            self._record(_Step(name, _pickled(error, f"the error step {name!r} raised")))
            raise
        finally:
            self._in_step = False
        blob = _pickled(result, f"the value step {name!r} returned")
        self._steps[(self.task_ref, name)] = blob
        self._record(_Step(name))
        return pickle.loads(blob)

    def _wait(self, key: str | None, span: timedelta) -> Mapping[str, Any] | None:
        """Return what this wait of the body returned, or park the task until it ends."""
        entry = self._replayed()
        if entry is None:  # this wait is new in this attempt
            wait = self._record(_Wait(key))
            signals = self._task.mailbox.get(key) if key is not None else None
            if signals:  # the signal came before the body began to wait for it
                wait.end(signals.popleft())
        elif isinstance(entry, _Wait) and entry.key == key:
            wait = entry
        else:
            raise self._diverged(_waited(key), entry)
        if not wait.done:
            raise _Park(self._clock.now() + span, wait)
        return None if wait.result is None else pickle.loads(wait.result)

    def _replayed(self) -> _Entry | None:
        """Return the journal entry that the body is repeating, or None once it is past the journal's end."""
        journal = self._task.journal
        if self._position == len(journal):
            return None
        entry = journal[self._position]
        self._position += 1
        return entry

    def _record[E: (_Wait, _Step)](self, entry: E) -> E:
        self._task.journal.append(entry)
        self._position += 1
        return entry

    def _diverged(self, now: str, before: _Entry) -> NotDurableError:
        return NotDurableError(
            f"task {self.task_ref!r} took another path on a replay: it {now}, "
            f"where before it {_describe(before)}. A body has to do the same every time it runs, "
            "so anything that can change has to be read inside ctx.step."
        )


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
        self._steps: dict[tuple[str, str], bytes] = {}  # the pickled results, by task reference and step name
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

        Submitting a key again queues nothing, whatever became of its task: the first call stands. The args
        are pickled, and `NotDurableError` says when they can't be.
        """
        ref = call.idempotency_key
        if ref not in self._tasks:
            self._tasks[ref] = _Task(call, _pickled(call.args, f"the args of task {call.task!r}"))
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
        sent under `key`, for the next time the task waits for `key`. The payload is pickled, and
        `NotDurableError` says when it can't be.
        """
        task = self._tasks[ref]
        blob = _pickled(payload, f"the payload of the signal {key!r} sent to task {ref!r}")
        if task.status == "parked" and task.waiting is not None and task.waiting.key == key:
            self._wake(task, blob)
        else:
            task.mailbox.setdefault(key, deque()).append(blob)

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
            self._registry[name](ctx, pickle.loads(task.args))
        except _Park as park:
            self._park(task, park.wake_at, park.wait)
        except _Crash:
            task.status = "crashed"
        except NotDurableError:
            self._end(task, "failed")
            raise
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
        # A retry is a new attempt: its waits happen again and its failures are not replayed. Only the steps
        # that succeeded carry over, by name.
        task.journal.clear()
        self._park(task, self.clock.now() + delay, None)

    def _park(self, task: _Task, wake_at: datetime, wait: _Wait | None) -> None:
        task.status = "parked"
        task.wake_at = wake_at
        task.waiting = wait

    def _wake(self, task: _Task, result: bytes | None = None) -> None:
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

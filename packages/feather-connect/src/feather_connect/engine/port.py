"""The engine port: how the runtime submits work to an engine and what a task body may use while it runs.

The port has no wall-clock task timeout. Long waits are durable sleeps, such as a budget deferral until
tomorrow, so a task's age says nothing about whether it is stuck.
"""

import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Protocol, TypeVar

from feather_connect.core.errors import PermanentError

__all__ = [
    "Engine",
    "PermanentError",
    "RetryPolicy",
    "TaskCall",
    "TaskContext",
    "TaskFn",
    "WorkerHandle",
    "WorkerRuntime",
]

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """When to try a failed task again.

    A task gets `max_attempts` attempts in all. The wait after a failure starts at `first_delay`, doubles with
    each failure up to `max_delay`, and is cut by equal jitter to between half and all of that wait.
    """

    max_attempts: int = 6
    first_delay: timedelta = timedelta(seconds=10)
    max_delay: timedelta = timedelta(minutes=10)

    def delay(self, failed_attempt: int, rand: Callable[[], float] = random.random) -> timedelta:
        """Return how long to wait after attempt number `failed_attempt` has failed. The first attempt is 1.

        `rand` returns a number from 0 to 1 that sets the jitter: 0 gives half of the wait, 1 all of it.
        """
        try:
            wait = min(self.first_delay * 2 ** (failed_attempt - 1), self.max_delay)
        except OverflowError:  # so many doublings that the wait no longer fits in a timedelta
            wait = self.max_delay
        return wait * (0.5 + rand() / 2)


@dataclass(frozen=True)
class TaskCall:
    task: str  # "feather.extract", "feather.apply", ...
    args: Mapping[str, Any]  # ids only, never data (≤ 64 KB)
    idempotency_key: str
    concurrency_key: str | None = None  # one running per key; the rest queue
    labels: Mapping[str, str] = field(default_factory=dict[str, str])  # routing to a worker pool
    priority: int = 1  # 0 low, 1 normal, 2 high
    retry: RetryPolicy = RetryPolicy()


class TaskContext(Protocol):
    """What a task body may use. Everything here survives a worker restart.

    `task_ref` equals the idempotency key of the task's call, and the runtime relies on that to find the task's row.
    """

    task_ref: str
    attempt: int

    # Once a step succeeds, a retry or a replay returns the result it saved.
    def step(self, name: str, fn: Callable[[], T]) -> T: ...
    def sleep(self, duration: timedelta) -> None: ...
    def wait_signal(self, key: str, timeout: timedelta) -> Mapping[str, Any] | None: ...


TaskFn = Callable[[TaskContext, Mapping[str, Any]], Mapping[str, Any] | None]


class Engine(Protocol):
    """The control-plane side: submits and manages work, never executes it."""

    def submit(self, call: TaskCall) -> str: ...  # returns the engine's task reference
    def cancel(self, ref: str) -> None: ...
    def signal(self, ref: str, key: str, payload: Mapping[str, Any]) -> None: ...
    def recover_worker(self, worker_id: str) -> list[str]: ...  # re-queues a dead worker's tasks
    def has_ended(self, ref: str) -> bool: ...  # the task exists and will not run again; False if unknown
    def health(self) -> Mapping[str, Any]: ...


class WorkerHandle(Protocol):
    """A running worker."""

    def stop(self) -> None:
        """Stop taking new tasks and wait a limited time for the ones that are running to end."""
        ...


class WorkerRuntime(Protocol):
    """The worker side: executes registered tasks until stopped."""

    def run(
        self, tasks: Mapping[str, TaskFn], worker_id: str, labels: Mapping[str, str], slots: int
    ) -> WorkerHandle:
        """Start executing `tasks` and return at once. The worker runs until the handle is stopped."""
        ...

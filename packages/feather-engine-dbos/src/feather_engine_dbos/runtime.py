"""DbosWorkerRuntime: the worker side of the engine port. It runs `feather_task` workflows from the queues.

The workflow body is the retry loop of the engine port: it runs the task's function, and after a failure that
is not a `PermanentError` it sleeps durably and tries again, up to the policy's attempts. Everything a task does
outside itself goes through `ctx.step`, which DBOS checkpoints, so a worker that dies mid-task is recovered by
running the workflow again from the top and replaying the steps that finished.
"""

import threading
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

from dbos import DBOS, DBOSConfig, StepOptions

from feather_connect.engine.port import (
    PermanentError,
    RetryPolicy,
    TaskContext,
    TaskFn,
    WorkerHandle,
    WorkerRuntime,
)
from feather_engine_dbos.engine import FREE, KEYED, WORKFLOW, queue_name

APPLICATION_VERSION = "feather-1"  # fixed (design §6.12): a deploy is a new worker, never a new version

# The functions that tasks run, by name. It is set before DBOS launches and read by the workflow.
_tasks: dict[str, TaskFn] = {}
_lock = threading.Lock()


class _DbosContext(TaskContext):
    """What a task body is given. One lives as long as one execution of the workflow, retries included."""

    def __init__(self, task_ref: str) -> None:
        self.task_ref = task_ref
        self.attempt = 1
        self._results: dict[str, Any] = {}  # the results of the steps that succeeded, by name
        self._in_step = False

    def step[T](self, name: str, fn: Callable[[], T]) -> T:
        if self._in_step:  # a step called inside a step is an ordinary call
            return fn()
        if name in self._results:  # an earlier attempt finished this step: no new DBOS step for it
            return self._results[name]
        self._in_step = True
        try:
            result = DBOS.run_step(StepOptions(name=name), fn)
        finally:
            self._in_step = False
        self._results[name] = result
        return result

    def sleep(self, duration: timedelta) -> None:
        DBOS.sleep(duration.total_seconds())

    def wait_signal(self, key: str, timeout: timedelta) -> Mapping[str, Any] | None:
        return DBOS.recv(topic=key, timeout_seconds=timeout.total_seconds())


@DBOS.workflow(name=WORKFLOW)
def feather_task(task: str, args: dict[str, Any], retry: dict[str, Any]) -> dict[str, Any]:
    policy = RetryPolicy(
        retry["max_attempts"],
        timedelta(seconds=retry["first_delay_s"]),
        timedelta(seconds=retry["max_delay_s"]),
    )
    if task not in _tasks:
        raise PermanentError(f"this worker has no task named {task!r}")
    ctx = _DbosContext(DBOS.workflow_id or "?")
    for attempt in range(1, policy.max_attempts + 1):
        ctx.attempt = attempt
        try:
            return dict(_tasks[task](ctx, args) or {})
        except PermanentError:
            raise
        except Exception:
            if attempt == policy.max_attempts:
                raise
            DBOS.sleep(policy.delay(attempt).total_seconds())
    raise AssertionError("unreachable: the last attempt returns or raises")


class _Handle(WorkerHandle):
    def __init__(self, drain_s: float) -> None:
        self._drain_s = drain_s

    def stop(self) -> None:
        DBOS.destroy(workflow_completion_timeout_sec=int(self._drain_s))


class DbosWorkerRuntime(WorkerRuntime):
    """Runs tasks from the DBOS queues of the system database in `schema`, in this process.

    There is one DBOS per process, so a process runs one worker. `poll_s` is how often the queues are looked at.
    `drain_s` is how long `stop` waits for running tasks.
    """

    def __init__(
        self,
        database_url: str,
        *,
        poll_s: float = 0.5,
        schema: str = "dbos",
        application_version: str = APPLICATION_VERSION,
        drain_s: float = 30,
    ) -> None:
        self._url = database_url
        self._poll_s = poll_s
        self._schema = schema
        self._version = application_version
        self._drain_s = drain_s

    def run(
        self, tasks: Mapping[str, TaskFn], worker_id: str, labels: Mapping[str, str], slots: int
    ) -> WorkerHandle:
        with _lock:
            _tasks.clear()
            _tasks.update(tasks)
        pool = labels.get("pool")
        keyed, free = queue_name(KEYED, pool), queue_name(FREE, pool)
        config: DBOSConfig = {
            "name": "feather",
            "system_database_url": self._url,
            "dbos_system_schema": self._schema,
            "application_version": self._version,
            "executor_id": worker_id,
            "console_log_level": "WARNING",
        }
        DBOS(config=config)
        DBOS.listen_queues([keyed, free])
        DBOS.launch()
        DBOS.register_queue(
            keyed, worker_concurrency=slots, partition_concurrency=1, polling_interval_sec=self._poll_s
        )
        DBOS.register_queue(free, worker_concurrency=slots, polling_interval_sec=self._poll_s)
        return _Handle(self._drain_s)

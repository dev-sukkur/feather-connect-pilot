"""DBOS adapter for the spike.

Mapping onto the port:
- a task is a DBOS workflow (`feather_task`) dequeued from a queue;
- concurrency key -> partitioned queue (partition_concurrency=1);
- labels {"site": x} -> per-site queue that only that site's workers listen to;
- idempotency key -> workflow id with reuse policy "return-existing";
- ctx.chunk -> DBOS.run_step (checkpointed), ctx.sleep -> DBOS.sleep, signals -> recv/send;
- retry policy -> loop in the workflow with durable sleeps between attempts.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import asdict
from typing import Any, Callable

from dbos import DBOS, DBOSClient, DBOSConfig, EnqueueOptions, StepOptions

from spike import journal
from spike.config import PG_URL
from spike.port import PermanentError, RetryPolicy, TaskCall
from spike.tasks import TASKS

KEYED, FREE = "feather-keyed", "feather-free"
SCHEMA = "dbos"
POLL_S = float(os.environ.get("SPIKE_DBOS_POLL_S", "0.2"))
DBOS_PRIORITY = {2: 1, 1: 2, 0: 3}  # DBOS dequeues the lowest number first

_worker = {"name": "?", "version": "?"}


def qname(base: str, site: str | None) -> str:
    return base if not site or site == "cloud" else f"{base}@{site}"


class _Ctx:
    def __init__(self, run: str, task_id: str, task: str) -> None:
        self.run, self.task_id, self.task = run, task_id, task
        self.worker, self.version = _worker["name"], _worker["version"]
        self.attempt = 1

    def log(self, event: str, **info: Any) -> None:
        journal.log(self.run, self.task_id, self.task, self.attempt, self.worker, self.version,
                    event, **info)

    def chunk(self, name: str, fn: Callable[[], Any]) -> Any:
        return DBOS.run_step(StepOptions(name=f"chunk:{name}"), fn)

    def sleep(self, seconds: float) -> None:
        DBOS.sleep(seconds)

    def wait_signal(self, key: str, timeout_s: float) -> Any:
        return DBOS.recv(topic=key, timeout_seconds=timeout_s)


@DBOS.workflow(name="feather_task")
def feather_task(task: str, args: dict, run: str, retry: dict) -> dict:
    policy = RetryPolicy(**retry)
    ctx = _Ctx(run, DBOS.workflow_id or "?", task)
    for attempt in range(1, policy.max_attempts + 1):
        ctx.attempt = attempt
        ctx.log("start")
        try:
            out = TASKS[task](ctx, args) or {}
            ctx.log("end")
            return out
        except PermanentError as e:
            ctx.log("fail", error=str(e), permanent=True)
            raise
        except Exception as e:  # retryable
            if attempt == policy.max_attempts:
                ctx.log("fail", error=str(e))
                raise
            delay = policy.delay(attempt)
            ctx.log("retry_wait", error=str(e), delay=delay)
            DBOS.sleep(delay)
    raise AssertionError("unreachable")


def _recovery_loop(stop: threading.Event) -> None:
    """Stands in for the control plane's sweeper: recovers a dead worker's PENDING workflows."""
    while not stop.wait(1.0):
        for row in journal.query(
            "select id, dead_worker from spike.recover_request where done_at is null"
        ):
            handles = DBOS._recover_pending_workflows([row["dead_worker"]])
            journal.execute(
                "update spike.recover_request set done_at = clock_timestamp() where id = %s",
                (row["id"],),
            )
            DBOS.logger.warning(f"recovered {len(handles)} workflows of {row['dead_worker']}")


def serve(name: str, labels: dict, slots: int, version: str, auto_upgrade: bool = False) -> None:
    _worker.update(name=name, version=version)
    site = labels.get("site", "cloud")
    config: DBOSConfig = {
        "name": "feather-spike",
        "system_database_url": PG_URL,
        "dbos_system_schema": SCHEMA,
        "application_version": version,
        "executor_id": name,
        "auto_upgrade": auto_upgrade,
        "auto_upgrade_interval_sec": 2.0,
        "auto_upgrade_grace_sec": 5.0,
        "console_log_level": "WARNING",
    }
    DBOS(config=config)
    DBOS.listen_queues([qname(KEYED, site), qname(FREE, site)])
    DBOS.launch()
    DBOS.register_queue(qname(KEYED, site), worker_concurrency=slots, partition_concurrency=1,
                        polling_interval_sec=POLL_S)
    DBOS.register_queue(qname(FREE, site), worker_concurrency=slots, polling_interval_sec=POLL_S)
    stop = threading.Event()
    threading.Thread(target=_recovery_loop, args=(stop,), daemon=True).start()
    journal.mark_ready(name)
    while True:
        time.sleep(3600)


class DbosClient:
    name = "dbos"

    def __init__(self) -> None:
        self._c = DBOSClient(system_database_url=PG_URL, dbos_system_schema=SCHEMA)

    def submit(self, call: TaskCall, run: str) -> str:
        site = call.labels.get("site")
        opts: EnqueueOptions = {
            "workflow_name": "feather_task",
            "queue_name": qname(KEYED if call.concurrency_key else FREE, site),
            "workflow_id": call.idempotency_key,
            "workflow_id_reuse_policy": "return-existing",
            "priority": DBOS_PRIORITY[call.priority],
        }
        if call.concurrency_key:
            opts["queue_partition_key"] = call.concurrency_key
        handle = self._c.enqueue(opts, call.task, dict(call.args), run, asdict(call.retry))
        return handle.workflow_id

    def submit_many(self, calls: list[TaskCall], run: str) -> None:
        for call in calls:  # DBOSClient has no bulk enqueue
            self.submit(call, run)

    def signal(self, task_ref: str, key: str, payload: Any) -> None:
        self._c.send(task_ref, payload, topic=key)

    def close(self) -> None:
        self._c.destroy()

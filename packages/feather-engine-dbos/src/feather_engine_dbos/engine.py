"""DbosEngine: the control-plane side of the engine port, on a DBOS system database.

A task is a DBOS workflow named `feather_task`, started by whichever worker dequeues it (see `runtime.py`). The
mapping follows design §6.8:

- A task with no concurrency key goes to the queue `feather-free`. One with a key goes to `feather-keyed`, a
  partitioned queue that runs one task per partition key, and the key is the partition key.
- The idempotency key is the workflow id, and submitting it again returns the task that has it.
- Our priorities 2, 1 and 0 (high, normal, low) are DBOS priorities 1, 2 and 3, since DBOS runs the lowest first.
- A label `pool=x` sends the task to the queues `feather-free@x` and `feather-keyed@x`, which only workers of
  that pool listen to.
"""

import json
from collections.abc import Mapping
from typing import Any

from dbos import DBOS, DBOSClient, EnqueueOptions

from feather_connect.engine.port import Engine, RetryPolicy, TaskCall

KEYED = "feather-keyed"
FREE = "feather-free"
WORKFLOW = "feather_task"
MAX_ARGS_BYTES = 64 * 1024  # invariant I8: a task carries ids, never data
_DBOS_PRIORITY = {2: 1, 1: 2, 0: 3}


def queue_name(base: str, pool: str | None) -> str:
    """Return the queue for `base` in `pool`: the shared queue when there is no pool."""
    return base if not pool else f"{base}@{pool}"


def retry_input(policy: RetryPolicy) -> dict[str, Any]:
    """Return the policy as plain numbers, which is how it travels in the workflow's input."""
    return {
        "max_attempts": policy.max_attempts,
        "first_delay_s": policy.first_delay.total_seconds(),
        "max_delay_s": policy.max_delay.total_seconds(),
    }


class DbosEngine(Engine):
    """Submits and manages tasks through the DBOS system database. It never runs a task itself.

    It creates, or brings up to date, the system database (schema `schema`) when it is made, because the DBOS
    client does not.
    """

    def __init__(self, database_url: str, *, schema: str = "dbos") -> None:
        DBOS.migrate(database_url, schema=schema)
        self._client = DBOSClient(system_database_url=database_url, dbos_system_schema=schema)

    def submit(self, call: TaskCall) -> str:
        args = dict(call.args)
        if len(json.dumps(args).encode()) > MAX_ARGS_BYTES:
            raise ValueError(f"the args of task {call.task!r} are over 64 KB: pass ids, never data")
        pool = call.labels.get("pool")
        options: EnqueueOptions = {
            "workflow_name": WORKFLOW,
            "queue_name": queue_name(KEYED if call.concurrency_key else FREE, pool),
            "workflow_id": call.idempotency_key,
            "workflow_id_reuse_policy": "return-existing",
            "priority": _DBOS_PRIORITY[call.priority],
        }
        if call.concurrency_key:
            options["queue_partition_key"] = call.concurrency_key
        handle = self._client.enqueue(options, call.task, args, retry_input(call.retry))
        return handle.workflow_id

    def cancel(self, ref: str) -> None:
        self._client.cancel_workflow(ref)

    def signal(self, ref: str, key: str, payload: Mapping[str, Any]) -> None:
        self._client.send(ref, dict(payload), topic=key)

    def recover_worker(self, worker_id: str) -> list[str]:
        """Put the tasks that `worker_id` was running back on their queues, and return their references."""
        running = self._client.list_workflows(
            status="PENDING", executor_id=worker_id, load_input=False, load_output=False
        )
        for workflow in running:
            self._client.resume_workflow(workflow.workflow_id, queue_name=workflow.queue_name)
        return [workflow.workflow_id for workflow in running]

    def health(self) -> Mapping[str, Any]:
        """Return `queue_depth`, the number of tasks waiting on a queue, and `queues`, the depth of each."""
        waiting = self._client.list_workflows(status="ENQUEUED", load_input=False, load_output=False)
        queues: dict[str, int] = {}
        for workflow in waiting:
            name = workflow.queue_name or "?"
            queues[name] = queues.get(name, 0) + 1
        return {"queue_depth": len(waiting), "queues": queues}

    def close(self) -> None:
        self._client.destroy()

"""Hatchet adapter for the spike.

Mapping onto the port:
- tasks are three standalone Hatchet tasks: feather_free, feather_keyed (concurrency
  expression on input.concurrency_key, max 1 run per key) and feather_durable (for bodies
  that sleep or wait for signals);
- idempotency key -> task idempotency on input.key;
- labels {"site": x} -> required desired worker label per run; workers carry a site label;
- retry policy -> native task retries (static per task, not per call);
- ctx.chunk -> our Postgres memo table (Hatchet does not memoise plain code);
- ctx.sleep / ctx.wait_signal -> DurableContext.aio_sleep_for / aio_wait_for, bridged from
  the worker thread that runs the synchronous body; signals are pushed events.
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from pydantic import BaseModel

from spike import journal
from spike.port import PermanentError, TaskCall
from spike.tasks import TASKS


def _load_env() -> None:
    env = Path(__file__).resolve().parents[1] / os.environ.get("SPIKE_ENV_FILE", ".env")
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    os.environ.setdefault("HATCHET_CLIENT_TLS_STRATEGY", "none")


_load_env()

from hatchet_sdk import (  # noqa: E402  (needs the env above)
    ConcurrencyExpression,
    ConcurrencyLimitStrategy,
    Context,
    DedupeViolationError,
    DesiredWorkerLabel,
    DurableContext,
    Hatchet,
    IdempotencyCollisionError,
    NonRetryableException,
    TriggerWorkflowOptions,
    TTLBasedIdempotencyConfig,
    UserEventCondition,
)

hatchet = Hatchet()

DURABLE = {"nap", "budgeted", "remote_relay"}
MAX_ATTEMPTS = 5
HATCHET_PRIORITY = {0: 1, 1: 2, 2: 3}  # Hatchet: 1 low .. 3 high
_worker = {"name": "?", "version": "?"}


class FeatherInput(BaseModel):
    task: str
    args: dict
    run: str
    key: str
    concurrency_key: str = ""


COMMON: dict[str, Any] = dict(
    input_validator=FeatherInput,
    retries=MAX_ATTEMPTS - 1,
    backoff_factor=2.0,
    backoff_max_seconds=60,
    execution_timeout=timedelta(minutes=10),
    schedule_timeout=timedelta(hours=1),
)
# A/B switches used to isolate the T13 engine stall.
if os.environ.get("SPIKE_HATCHET_IDEMPOTENCY", "1") == "1":
    COMMON["idempotency"] = TTLBasedIdempotencyConfig(key_expression="input.key", ttl=timedelta(hours=1))
REGISTER_KEYED = os.environ.get("SPIKE_HATCHET_KEYED", "1") == "1"


class _Ctx:
    def __init__(self, inp: FeatherInput, attempt: int, dctx: DurableContext | None = None,
                 loop: asyncio.AbstractEventLoop | None = None) -> None:
        self.run, self.task_id, self.task = inp.run, inp.key, inp.task
        self.worker, self.version = _worker["name"], _worker["version"]
        self.attempt = attempt
        self._dctx, self._loop = dctx, loop

    def log(self, event: str, **info: Any) -> None:
        journal.log(self.run, self.task_id, self.task, self.attempt, self.worker, self.version,
                    event, **info)

    def chunk(self, name: str, fn: Callable[[], Any]) -> Any:
        hit, result = journal.memo_get(self.run, self.task_id, name)
        if hit:
            return result
        result = fn()
        journal.memo_put(self.run, self.task_id, name, result)
        return result

    def _await(self, coro: Any, timeout: float | None = None) -> Any:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def sleep(self, seconds: float) -> None:
        if self._dctx is None:
            time.sleep(seconds)  # not durable outside durable tasks
            return
        self._await(self._dctx.aio_sleep_for(timedelta(seconds=seconds)))

    def wait_signal(self, key: str, timeout_s: float) -> Any:
        event_key = f"{key}:{self.task_id}"
        since = datetime.now(timezone.utc) - timedelta(minutes=5)
        result = self._await(self._dctx.aio_wait_for(
            event_key, UserEventCondition(event_key=event_key, readable_data_key="data",
                                          consider_events_since=since)), timeout_s + 30)
        return _unwrap(result)


def _unwrap(result: Any) -> Any:
    """aio_wait_for returns condition results keyed by name (lists of events); dig out the payload."""
    while True:
        if isinstance(result, dict) and len(result) == 1 and not {"rows"} & result.keys():
            result = next(iter(result.values()))
        elif isinstance(result, list) and len(result) == 1:
            result = result[0]
        else:
            return result


def _run(inp: FeatherInput, attempt: int, dctx: DurableContext | None = None,
         loop: asyncio.AbstractEventLoop | None = None) -> dict:
    ctx = _Ctx(inp, attempt, dctx, loop)
    ctx.log("start")
    try:
        out = TASKS[inp.task](ctx, inp.args) or {}
    except PermanentError as e:
        ctx.log("fail", error=str(e), permanent=True)
        raise NonRetryableException(str(e)) from e
    except Exception as e:
        ctx.log("fail" if attempt >= MAX_ATTEMPTS else "retry_wait", error=str(e))
        raise
    ctx.log("end")
    return out


@hatchet.task(name="feather_free", **COMMON)
def feather_free(inp: FeatherInput, c: Context) -> dict:
    return _run(inp, c.retry_count + 1)


@hatchet.task(
    name="feather_keyed",
    concurrency=ConcurrencyExpression(expression="input.concurrency_key", max_runs=1,
                                      limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN),
    **COMMON,
)
def feather_keyed(inp: FeatherInput, c: Context) -> dict:
    return _run(inp, c.retry_count + 1)


@hatchet.durable_task(name="feather_durable", **COMMON)
async def feather_durable(inp: FeatherInput, c: DurableContext) -> dict:
    loop = asyncio.get_running_loop()
    return await asyncio.to_thread(_run, inp, c.retry_count + 1, c, loop)


def serve(name: str, labels: dict, slots: int, version: str, auto_upgrade: bool = False) -> None:
    import signal
    import sys

    if sys.platform == "win32" and not hasattr(signal, "SIGQUIT"):
        signal.SIGQUIT = signal.SIGBREAK  # spike-only shim: hatchet-sdk's worker assumes POSIX signals
    _worker.update(name=name, version=version)
    site = labels.get("site", "cloud")

    async def lifespan():  # runs once the worker is up
        journal.mark_ready(name)
        yield

    workflows = [feather_free, feather_durable] + ([feather_keyed] if REGISTER_KEYED else [])
    worker = hatchet.worker(name, slots=slots, durable_slots=slots,
                            labels={"site": site, "version": version},
                            workflows=workflows, lifespan=lifespan)
    worker.start()


class HatchetClient:
    name = "hatchet"

    @staticmethod
    def _prepare(call: TaskCall, run: str) -> tuple[Any, FeatherInput, TriggerWorkflowOptions]:
        inp = FeatherInput(task=call.task, args=dict(call.args), run=run,
                           key=call.idempotency_key, concurrency_key=call.concurrency_key or "")
        task = (feather_durable if call.task in DURABLE
                else feather_keyed if call.concurrency_key else feather_free)
        opts = TriggerWorkflowOptions(
            priority=HATCHET_PRIORITY[call.priority],
            additional_metadata={"run": run},
            desired_worker_label={"site": DesiredWorkerLabel(
                value=call.labels.get("site", "cloud"), required=True)},
        )
        return task, inp, opts

    def submit(self, call: TaskCall, run: str) -> str:
        task, inp, opts = self._prepare(call, run)
        try:
            task.run_no_wait(inp, options=opts)
        except (IdempotencyCollisionError, DedupeViolationError):
            pass  # duplicate submission: the first run stands
        return call.idempotency_key

    def submit_many(self, calls: list[TaskCall], run: str) -> None:
        by_task: dict[Any, list] = {}
        for call in calls:
            task, inp, opts = self._prepare(call, run)
            by_task.setdefault(task, []).append(task.create_bulk_run_item(input=inp, options=opts))
        for task, items in by_task.items():
            task.run_many_no_wait(items)

    def signal(self, task_ref: str, key: str, payload: Any) -> None:
        hatchet.event.push(f"{key}:{task_ref}", payload)

    def close(self) -> None:
        pass

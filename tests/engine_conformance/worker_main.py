"""A worker process of the conformance suite. Its settings come from the environment (see `conftest.py`), and it
runs until it is killed. The task bodies write what they do to the journal `conformance.exec_log`."""

import json
import os
import threading
import time
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from feather_connect.engine.port import PermanentError, TaskContext, TaskFn
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.store import PostgresStore
from feather_engine_dbos import DbosWorkerRuntime

URL = os.environ["FEATHER_CONF_DB"]
WORKER = os.environ["FEATHER_CONF_WORKER"]

_pool = ConnectionPool(
    URL, min_size=1, max_size=20, kwargs={"autocommit": True, "row_factory": dict_row}, open=True
)
_store: PostgresStore | None = None
_store_lock = threading.Lock()


def log(ctx: TaskContext, args: Mapping[str, Any], event: str, **info: Any) -> None:
    with _pool.connection() as conn:
        conn.execute(
            "insert into conformance.exec_log (run, task_id, attempt, worker, event, info)"
            " values (%s, %s, %s, %s, %s, %s::jsonb)",
            (args["run"], ctx.task_ref, ctx.attempt, WORKER, event, json.dumps(info)),
        )


def effect_once(args: Mapping[str, Any], task_id: str, chunk: str) -> bool:
    """Write a chunk's effect and mark it done in one transaction. False if it was already done."""
    with _pool.connection() as conn, conn.transaction():
        marked = conn.execute(
            "insert into conformance.checkpoint (run, task_id, chunk) values (%s, %s, %s)"
            " on conflict do nothing returning 1",
            (args["run"], task_id, chunk),
        ).fetchone()
        if marked is None:
            return False
        conn.execute(
            "insert into conformance.effects (run, effect_key, worker) values (%s, %s, %s)",
            (args["run"], f"{task_id}:{chunk}", WORKER),
        )
        return True


def bump(args: Mapping[str, Any], task_id: str) -> int:
    with _pool.connection() as conn:
        row = conn.execute(
            "insert into conformance.counter (run, task_id, n) values (%s, %s, 1)"
            " on conflict (run, task_id) do update set n = conformance.counter.n + 1 returning n",
            (args["run"], task_id),
        ).fetchone()
    assert row is not None
    return row["n"]


def work(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    log(ctx, args, "start")
    time.sleep(args.get("ms", 100) / 1000)
    log(ctx, args, "end")
    return {"worker": WORKER}


def chunks(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    log(ctx, args, "start")
    for i in range(args.get("n", 3)):
        name = f"c{i}"

        def body(name: str = name) -> bool:
            log(ctx, args, "chunk_exec", chunk=name)
            time.sleep(args.get("ms", 3000) / 1000)
            return effect_once(args, ctx.task_ref, name)

        ctx.step(name, body)
    log(ctx, args, "end")
    return {}


def flaky(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    n = bump(args, ctx.task_ref)
    log(ctx, args, "try", n=n)
    if args.get("permanent"):
        raise PermanentError("bad record")
    if n <= args.get("fail_times", 3):
        raise RuntimeError(f"transient failure {n}")
    log(ctx, args, "end")
    return {"n": n}


def step_then_fail(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    """Runs a step, then fails until the third attempt. The step must run once, whatever the attempts."""
    ctx.step("count", lambda: bump(args, ctx.task_ref))
    log(ctx, args, "try")
    if ctx.attempt < 3:
        raise RuntimeError("try again")
    log(ctx, args, "end")
    return {}


def nap(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    ctx.step("nap_start", lambda: log(ctx, args, "nap_start"))
    ctx.sleep(timedelta(seconds=args["seconds"]))
    ctx.step("nap_end", lambda: log(ctx, args, "nap_end"))
    return {}


def hold(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    log(ctx, args, "start")
    time.sleep(args.get("seconds", 10))
    return {}


def budgeted(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    global _store
    with _store_lock:
        if _store is None:
            _store, _ = connect(URL, max_size=5)
    store = _store
    got, i = 0, 0
    while got < args["units"]:
        i += 1

        def acquire() -> float | None:
            grant = store.budgets.acquire(args["budget"], 1, args["priority"])
            if not grant.granted:
                return grant.wait.total_seconds()
            log(ctx, args, "grant", priority=args["priority"])
            return None

        wait_s = ctx.step(f"acquire{i}", acquire)
        if wait_s is None:
            got += 1
        else:
            ctx.sleep(timedelta(seconds=wait_s))
    log(ctx, args, "end")
    return {"units": got}


def relay(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    ctx.step("waiting", lambda: log(ctx, args, "waiting"))
    result = ctx.wait_signal("agent_result", timedelta(seconds=60))
    ctx.step("done", lambda: log(ctx, args, "relay_done", result=result))
    log(ctx, args, "end")
    return {"result": result}


TASKS: dict[str, TaskFn] = {
    "work": work,
    "chunks": chunks,
    "flaky": flaky,
    "step_then_fail": step_then_fail,
    "nap": nap,
    "hold": hold,
    "budgeted": budgeted,
    "relay": relay,
}


def main() -> None:
    runtime = DbosWorkerRuntime(
        URL,
        poll_s=float(os.environ["FEATHER_CONF_POLL_S"]),
        application_version=os.environ["FEATHER_CONF_VERSION"],
    )
    runtime.run(TASKS, WORKER, {}, int(os.environ["FEATHER_CONF_SLOTS"]))
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(
            "insert into conformance.ready (worker) values (%s)"
            " on conflict (worker) do update set at = clock_timestamp()",
            (WORKER,),
        )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()

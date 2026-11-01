"""A worker process for the tests: `python echo_worker.py DATABASE_URL WORKER_ID`. It runs until it is killed."""

import sys
import time
from collections.abc import Mapping
from typing import Any

from feather_connect.engine.port import PermanentError, TaskContext
from feather_engine_dbos import DbosWorkerRuntime

STEP_RUNS: dict[str, int] = {}


def echo(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    first = ctx.step("first", lambda: args["n"] * 2)
    return {"first": first, "attempt": ctx.attempt}


def flaky(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    """Runs a step once, then fails until the third attempt. The step must not run again on a retry."""
    runs = ctx.step(
        "count", lambda: STEP_RUNS.update(count=STEP_RUNS.get("count", 0) + 1) or STEP_RUNS["count"]
    )
    if ctx.attempt < 3:
        raise RuntimeError("try again")
    return {"attempt": ctx.attempt, "step_runs": runs}


def refuse(ctx: TaskContext, args: Mapping[str, Any]) -> None:
    raise PermanentError("no")


def waits(ctx: TaskContext, args: Mapping[str, Any]) -> Mapping[str, Any]:
    from datetime import timedelta

    return {"got": ctx.wait_signal("go", timedelta(seconds=30))}


if __name__ == "__main__":
    runtime = DbosWorkerRuntime(sys.argv[1], poll_s=0.1)
    runtime.run(
        {"t.echo": echo, "t.flaky": flaky, "t.refuse": refuse, "t.waits": waits},
        sys.argv[2],
        {},
        4,
    )
    time.sleep(3600)

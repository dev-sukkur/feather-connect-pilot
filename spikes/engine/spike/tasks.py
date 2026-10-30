"""Engine-agnostic task bodies used by the conformance tests.

Bodies follow the runtime's replay rules: side effects go through ctx.chunk, waits through
ctx.sleep / ctx.wait_signal, and everything else is deterministic.
"""

from __future__ import annotations

import time
from typing import Any, Callable

from spike import budget, journal
from spike.port import Ctx, PermanentError


def work(ctx: Ctx, args: dict) -> dict:
    time.sleep(args.get("ms", 100) / 1000)
    return {"worker": ctx.worker}


def chunks(ctx: Ctx, args: dict) -> dict:
    for i in range(args.get("n", 3)):
        name = f"c{i}"

        def body(name: str = name) -> bool:
            ctx.log("chunk_exec", chunk=name)
            time.sleep(args.get("ms", 3000) / 1000)
            return journal.checkpoint_effect(ctx.run, ctx.task_id, name, ctx.worker)

        ctx.chunk(name, body)
    return {}


def flaky(ctx: Ctx, args: dict) -> dict:
    def attempt_once() -> int:
        n = journal.bump_counter(ctx.run, ctx.task_id)
        ctx.log("try", n=n)
        if args.get("permanent"):
            raise PermanentError("bad record")
        if n <= args.get("fail_times", 3):
            raise RuntimeError(f"transient failure {n}")
        return n

    return {"n": ctx.chunk(f"try{ctx.attempt}", attempt_once)}


def nap(ctx: Ctx, args: dict) -> dict:
    ctx.chunk("nap_start", lambda: ctx.log("nap_start", seconds=args["seconds"]))
    ctx.sleep(args["seconds"])
    ctx.chunk("nap_end", lambda: ctx.log("nap_end"))
    return {}


def hold(ctx: Ctx, args: dict) -> dict:
    time.sleep(args.get("seconds", 10))
    return {}


def budgeted(ctx: Ctx, args: dict) -> dict:
    got, i = 0, 0
    while got < args["units"]:
        i += 1
        granted, wait_s = ctx.chunk(
            f"acq{i}", lambda: budget.acquire(args["budget"], 1, args["priority"], ctx.run)
        )
        if granted:
            got += 1
        else:
            ctx.sleep(wait_s)
    return {"units": got}


def remote_relay(ctx: Ctx, args: dict) -> dict:
    ctx.chunk(
        "post", lambda: journal.create_agent_task(ctx.run, ctx.task_id, dict(args))
    )
    result = ctx.wait_signal("agent_result", args.get("timeout_s", 60))
    ctx.chunk("done", lambda: ctx.log("relay_done", result=result))
    return {"result": result}


TASKS: dict[str, Callable[[Ctx, dict], Any]] = {
    "work": work,
    "chunks": chunks,
    "flaky": flaky,
    "nap": nap,
    "hold": hold,
    "budgeted": budgeted,
    "remote_relay": remote_relay,
}

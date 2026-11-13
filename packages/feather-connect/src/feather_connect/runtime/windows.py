"""Waiting for a connection's availability window (M3.1 plan 1 F3).

A task that would read or write a windowed connection outside its window waits, durably, until the window opens.
The wait is noted where a budget wait is, under the name `window:<connection>`, so the connection page and the
overview can say what waits and until when: a wait is never silent (FR-E4).
"""

import logging
from datetime import datetime, timedelta
from functools import partial

from feather_connect.core.windows import AvailabilityWindow, Use
from feather_connect.engine.port import TaskContext
from feather_connect.runtime.deps import RuntimeDeps

log = logging.getLogger(__name__)
WAIT_PREFIX = "window:"


def wait_name(connection: str) -> str:
    """The name a window wait is noted under, beside budget waits."""
    return f"{WAIT_PREFIX}{connection}"


def wait_for_window(
    ctx: TaskContext,
    deps: RuntimeDeps,
    connection: str,
    use: Use,
    run_id: str | None,
    label: str,
    *,
    overrun: bool = False,
) -> None:
    """Return once `connection` may be used for `use`, sleeping until its window opens if it is closed.

    With `overrun`, the task already started inside the window and is at a chunk boundary: it waits only if the
    window's `max_overrun` has run out. Each check is a step named after `label`, so a replay takes the same path.
    """
    window = deps.windows.get(connection)
    if window is None or not window.gates(use):
        return
    for check in range(1_000):  # a window opens every day: this only stops a clock that never moves
        name = f"window:{label}:{check}"

        opens = ctx.step(name, partial(_closed_until, window, deps, overrun))
        if opens is None:
            return
        at = datetime.fromisoformat(opens)
        wait = max(at - deps.clock.now(), timedelta(seconds=1))
        ctx.step(
            f"{name}:noted", partial(_note, deps, ctx.task_ref, run_id, connection, at)
        )  # once, not per replay
        log.info(
            "connection %s is outside its window (%s): the %s waits until %s",
            connection,
            window.describe(),
            "write" if use == "write" else "read",
            opens,
        )
        ctx.sleep(wait)
        overrun = False  # once it has waited, the task starts again, so it needs the window open
    raise RuntimeError(f"connection {connection!r}: its window never opened")


def _closed_until(window: AvailabilityWindow, deps: RuntimeDeps, overrun: bool) -> str | None:
    """When the window opens, if the task has to wait for it; None if it may go on."""
    now = deps.clock.now()
    closed = window.overrun(now) if overrun else not window.is_open(now)
    return window.opens_at(now).isoformat() if closed else None


def _note(deps: RuntimeDeps, task_ref: str, run_id: str | None, connection: str, until: datetime) -> None:
    try:
        deps.store.budgets.note_wait(wait_name(connection), task_ref, run_id, until)
    except Exception:
        log.exception("could not note that task %s waits for the window of %s", task_ref, connection)

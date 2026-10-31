"""The scheduler: turns the cron and interval triggers of the flows into runs.

`tick` is called often (once a second by the server's loop) and is cheap when nothing is due. It remembers the last tick
of each trigger in the store (`ScheduleStore`), so a restart neither repeats a tick nor forgets one. Every tick that
is due writes a run, a skipped one included, with the reason (I6).
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from cronsim import CronSim

from feather_connect.core.flow import Flow
from feather_connect.core.triggers import Cron, Every, Trigger
from feather_connect.runtime.runtime import Runtime

log = logging.getLogger(__name__)

_ACTOR = "scheduler"
# Most runs that one tick starts for a trigger when it catches up, whatever `catch_up` asks. The older ticks collapse.
MAX_CATCH_UP = 1000
# Most cron times that one tick looks through, which is a month of a schedule that fires every minute.
_SCAN_LIMIT = 100_000


@dataclass(frozen=True)
class TickResult:
    """What a due tick did: the run it wrote, with the status of that run and, if it was skipped or failed, why."""

    flow: str
    trigger_id: str
    due_at: datetime
    run_id: str
    status: str
    reason: str | None


def trigger_id(trigger: Trigger) -> str | None:
    """Return the stable id of a scheduled trigger, such as `cron:*/15 * * * *:Asia/Kolkata`. Others have none."""
    match trigger:
        case Cron(expr, tz):
            return f"cron:{expr}:{tz}"
        case Every(interval):
            return f"every:{interval.total_seconds():g}s"
        case _:
            return None


def _due(trigger: Trigger, last: datetime, now: datetime) -> tuple[list[datetime], int]:
    """Return the times after `last`, up to `now`, at which the trigger fires, oldest first, and how many older ones
    are left out of the list. An interval lists only its newest `MAX_CATCH_UP` + 1 times, and cron the first
    `_SCAN_LIMIT`."""
    match trigger:
        case Cron(expr, tz):
            found: list[datetime] = []
            for at in CronSim(expr, last.astimezone(ZoneInfo(tz))):
                if at > now or len(found) >= _SCAN_LIMIT:
                    break
                found.append(at.astimezone(UTC))
            return found, 0
        case Every(interval):
            count = int((now - last) / interval)
            first = max(1, count - MAX_CATCH_UP)
            return [last + interval * n for n in range(first, count + 1)], first - 1
        case _:
            return [], 0


def tick(runtime: Runtime, *, now: datetime) -> list[TickResult]:
    """Start the runs that are due as of `now`, and return what became of each tick.

    A trigger seen for the first time only notes `now`: nothing before the start is made up. The last tick is
    stored before any run is started, so a crash in between loses a tick, which shows in the log, and never repeats
    one. A flow that can't be ticked is logged and left, so the others carry on.
    """
    results: list[TickResult] = []
    for flow in runtime.flows():
        for trigger in flow.triggers:
            if (tid := trigger_id(trigger)) is None:
                continue
            try:
                results += _tick_trigger(runtime, flow, trigger, tid, now)
            except Exception:
                log.exception("could not tick trigger %s of flow %s", tid, flow.name)
    return results


def _tick_trigger(
    runtime: Runtime, flow: Flow, trigger: Trigger, tid: str, now: datetime
) -> list[TickResult]:
    schedules = runtime.store.schedules
    last = schedules.last_tick(flow.name, tid)
    if last is None:
        schedules.set_last_tick(flow.name, tid, now)
        return []
    due, left_out = _due(trigger, last, now)
    if not due:
        return []
    schedules.set_last_tick(flow.name, tid, due[-1])
    wanted = due[-1:] if flow.catch_up == "latest" else due[-MAX_CATCH_UP:]
    collapsed = left_out + len(due) - len(wanted)
    results: list[TickResult] = []
    if collapsed:
        params = {"collapsed": collapsed, "from": due[0].isoformat(), "to": wanted[0].isoformat()}
        results.append(_skip(runtime, flow, tid, due[0], "catch_up_collapsed", params))
    results += [_fire(runtime, flow, tid, at) for at in wanted]
    return results


def _skip(
    runtime: Runtime,
    flow: Flow,
    tid: str,
    due_at: datetime,
    reason: str,
    params: dict[str, Any] | None = None,
) -> TickResult:
    run_id = runtime.store.runs.create_run(
        flow.name,
        tid,
        _ACTOR,
        {"due_at": due_at.isoformat(), **(params or {})},
        status="skipped",
        reason=reason,
    )
    return TickResult(flow.name, tid, due_at, run_id, "skipped", reason)


def _fire(runtime: Runtime, flow: Flow, tid: str, due_at: datetime) -> TickResult:
    """Start the run for one due tick, or record why it is not started."""
    if not flow.enabled:
        return _skip(runtime, flow, tid, due_at, "disabled")
    active = runtime.active_runs(flow.name)
    if active and flow.overlap == "skip":
        return _skip(runtime, flow, tid, due_at, "overlap")
    if flow.overlap == "cancel_previous":
        for run in active:
            runtime.cancel_run(run.run_id, actor=_ACTOR, reason="cancel_previous: the next tick is due")
    try:
        run_id = runtime.start_run(flow.name, trigger=tid, actor=_ACTOR)
    except Exception as error:
        log.exception("could not start flow %s for tick %s", flow.name, tid)
        reason = f"could not start: {error}"
        run_id = runtime.store.runs.create_run(
            flow.name, tid, _ACTOR, {"due_at": due_at.isoformat()}, status="failed", reason=reason
        )
        return TickResult(flow.name, tid, due_at, run_id, "failed", reason)
    run = runtime.store.runs.get_run(run_id)
    return TickResult(flow.name, tid, due_at, run_id, run.status, run.reason)

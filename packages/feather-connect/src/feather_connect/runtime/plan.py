"""Planning: which partitions of each dataset a run reads."""

import inspect
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import PermanentError
from feather_connect.core.flow import Flow
from feather_connect.core.readiness import ReadyInfo
from feather_connect.core.strategies import ByStamp
from feather_connect.runtime.deps import RuntimeDeps

_ALL_PARTITIONS = "*"
_PUSH = "push"
MAX_BACKFILL_DAYS = 366
BACKFILL = "backfill"  # a run's param: the days a backfill of a packaged flow covers, which its host makes partitions of


def _date_of(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else None


def backfill_partitions(flow: Flow, start: date, end: date, timezone: str) -> list[str]:
    """The partitions of `flow` that cover the days from `start` to `end` (both included).

    Each dataset's scheme names its own partitions, so this asks for the ones planned around each day and keeps those
    whose date limits overlap the range. Every dataset of the flow must come out with the same partitions, because a run
    reads one list of them.
    """
    if start > end:
        raise ValueError("start must be on or before end")
    if (end - start).days + 1 > MAX_BACKFILL_DAYS:
        raise ValueError(f"a backfill covers at most {MAX_BACKFILL_DAYS} days")
    zone = ZoneInfo(timezone)
    found: list[set[str]] = []
    for dataset in flow.datasets:
        scheme, keys = dataset.partition, set[str]()
        for offset in range((end - start).days + 3):
            moment = datetime.combine(start + timedelta(days=offset - 1), time(12), zone)
            planned = scheme.recent(moment)
            if planned is None:
                raise ValueError(
                    f"dataset {dataset.name!r} can't list its partitions, so it can't be backfilled"
                )
            for key in planned:
                for bound in scheme.bounds(key).values():
                    low, high = _date_of(bound.low), _date_of(bound.high)
                    if low is None or high is None:
                        continue
                    if (low == high and start <= low <= end) or (low != high and low <= end and high > start):
                        keys.add(key)
        if not keys:
            raise ValueError(
                f"dataset {dataset.name!r} is not partitioned by date, so it can't be backfilled"
            )
        found.append(keys)
    if any(keys != found[0] for keys in found):
        raise ValueError("the datasets of this flow are partitioned differently, so it can't be backfilled")
    return sorted(found[0])


def _backfill_of(flow: Flow, days: Mapping[str, Any]) -> list[str]:
    """The partitions of a backfill queued by the days it covers: the server, which has no package code, can't name
    them, so the host does as it plans."""
    try:
        return backfill_partitions(
            flow, date.fromisoformat(days["start"]), date.fromisoformat(days["end"]), str(days["timezone"])
        )
    except (KeyError, ValueError) as error:
        raise PermanentError(f"the backfill can't be planned: {error}") from error


def _planned(dataset: Dataset, now: datetime, ready: ReadyInfo | None) -> list[str]:
    """The planner's partitions. A planner that takes `ready=` is told what a readiness firing is for (`ctx.ready`
    in the readiness design §5), and None on a run that no firing started."""
    planner = cast("Callable[..., list[str]]", dataset.planner)
    try:
        takes_ready = "ready" in inspect.signature(planner).parameters
    except (TypeError, ValueError):  # a builtin has no signature to read
        takes_ready = False
    return planner(now, ready=ready) if takes_ready else planner(now)


def _partitions_of(
    dataset: Dataset, explicit: Sequence[str] | None, deps: RuntimeDeps, ready: ReadyInfo | None = None
) -> list[str]:
    """Return what to read: the run's own list, or else the planner's, or else the scheme's recent window."""
    if isinstance(dataset.strategy, ByStamp):  # it reads once per run, across partitions, from its watermark
        return [_ALL_PARTITIONS]
    now = deps.clock.now()
    if explicit is not None:
        chosen = explicit
    elif dataset.planner is not None:
        chosen = _planned(dataset, now, ready)
    else:
        recent = dataset.partition.recent(now)
        if recent is None:
            raise PermanentError(
                f"dataset {dataset.name!r} is partitioned by a scheme that can't list its partitions: pass planner="
            )
        chosen = recent
    return list(dict.fromkeys(chosen))


def run_plan(flow: Flow, run_id: str, deps: RuntimeDeps, tick: str | None = None) -> dict[str, list[str]]:
    """Return the partitions the run reads of each dataset of `flow`, and register the slices of a multi-slice read.

    A complete dataset read in more than one slice is registered, and that is what later makes its extracts stage
    their versions for `run_seal`. Calling this again registers nothing new.

    A `tick` of a stream run can't do that: slices are registered per run, and a stream run holds a day of ticks.
    """
    params = deps.store.runs.get_run(run_id).params
    explicit = params.get("partitions")
    if explicit is None and isinstance(params.get(BACKFILL), Mapping):
        explicit = _backfill_of(flow, params[BACKFILL])
    planned: dict[str, list[str]] = {}
    for dataset in flow.datasets:
        if (
            dataset.strategy.name == _PUSH
        ):  # nothing to read: its events come in, and the sealer makes its versions
            continue
        partitions = _partitions_of(dataset, explicit, deps, ReadyInfo.from_params(params))
        if tick is not None and dataset.strategy.complete and len(partitions) > 1:
            raise PermanentError(
                f"dataset {dataset.name!r} reads {len(partitions)} slices as a whole, which a high-frequency poller "
                "can't do: use the by_stamp strategy, one partition, or an interval of 5 minutes or more"
            )
        if dataset.strategy.complete and len(partitions) > 1:
            deps.store.versions.register_slices(run_id, dataset.name, partitions)
        planned[dataset.name] = partitions
    return planned

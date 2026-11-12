"""Readiness: run a flow when the data it needs is complete (readiness design §5, M2 plan 1).

`fc.ready(...)` names a readiness set and `fc.on_ready(...)` is the trigger that waits for it. The server works out
when a set is ready from the store and the flow's catalogue spec, with no package code (plan §1), so a set names its
datasets and routes, which `spec()` gives as JSON. `readiness_problems` checks a project's sets against its flows.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from feather_connect.core.dataset import Dataset
from feather_connect.core.partitions import scheme_spec, spec_fields
from feather_connect.core.strategies import parse_duration

if TYPE_CHECKING:
    from feather_connect.core.flow import Flow, Problem

Each = Literal["run", "hour", "day"]
OnLate = Literal["rerun", "warn", "ignore"]
QuarantineRule = Literal["passes", "blocks"]
EACH: tuple[str, ...] = ("run", "hour", "day")
ON_LATE: tuple[str, ...] = ("rerun", "warn", "ignore")
PUSH_SETTLE = timedelta(
    minutes=5
)  # design §4: how long after T a push partition counts as complete by the clock


@dataclass(frozen=True)
class Ready:
    """A readiness set: datasets, by name, each optionally to be delivered to a route, split by a field."""

    datasets: tuple[str, ...]
    delivered_to: Mapping[str, str]  # dataset -> route
    by: str | None = None
    settle: timedelta | None = None
    quarantine: QuarantineRule = "passes"

    @property
    def name(self) -> str:
        """The set's name within its flow: its datasets, so that the same set in two triggers is one."""
        return "+".join(self.datasets) + (f"@{self.by}" if self.by else "")

    def spec(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "datasets": list(self.datasets),
            "delivered_to": dict(self.delivered_to),
            "by": self.by,
            "settle": None if self.settle is None else self.settle.total_seconds(),
            "quarantine": self.quarantine,
        }


@dataclass(frozen=True)
class OnReady:
    """Starts a run when `ready` is ready: once per upstream run, hour or day, and per scope of its `by` field."""

    ready: Ready
    each: Each
    tz: str | None = None
    at_most_every: timedelta | None = None
    on_late: OnLate = "rerun"
    start: datetime | None = None

    def spec(self) -> dict[str, Any]:
        return {
            "kind": "on_ready",
            "ready": self.ready.spec(),
            "each": self.each,
            "tz": self.tz,
            "at_most_every": None if self.at_most_every is None else self.at_most_every.total_seconds(),
            "on_late": self.on_late,
            "start": None if self.start is None else self.start.isoformat(),
        }


def _name_of(dataset: Dataset | str) -> str:
    return dataset if isinstance(dataset, str) else dataset.name


def ready(
    datasets: Dataset | str | Sequence[Dataset | str],
    *,
    delivered_to: str | Mapping[str, str] | None = None,
    by: str | None = None,
    settle: str | None = None,
    quarantine: QuarantineRule = "passes",
) -> Ready:
    """Return a readiness set. `datasets` are Dataset objects or names; a dataset of another package is named
    `<package>/<dataset>`. `delivered_to` is a route that each must have been applied to, or a route per dataset. `by`
    is a partition field, so that each of its values is ready, and fires, on its own. `settle`, such as `"10m"`,
    overrides how long a period is given for late records (design §4)."""
    listed = [datasets] if isinstance(datasets, Dataset | str) else list(datasets)
    names = tuple(_name_of(d) for d in listed)
    if not names:
        raise ValueError("ready needs at least one dataset")
    if len(set(names)) != len(names):
        raise ValueError(f"ready names a dataset twice: {names}")
    if isinstance(delivered_to, str):
        routes = dict.fromkeys(names, delivered_to)
    else:
        routes = dict(delivered_to or {})
        if unknown := sorted(set(routes) - set(names)):
            raise ValueError(f"delivered_to names datasets that aren't in the set: {unknown}")
    if quarantine not in ("passes", "blocks"):
        raise ValueError(f"quarantine is 'passes' or 'blocks', not {quarantine!r}")
    wait = None if settle is None else parse_duration(settle)
    if wait is not None and wait < timedelta(0):
        raise ValueError(f"settle can't be negative, got {settle!r}")
    return Ready(names, routes, by, wait, quarantine)


def on_ready(
    ready: Ready,
    *,
    each: Each = "run",
    tz: str | None = None,
    at_most_every: str | None = None,
    on_late: OnLate = "rerun",
    start: str | datetime | date | None = None,
) -> OnReady:
    """Return a trigger that runs the flow when `ready` is ready: after each upstream run (`each="run"`), or once an
    hour or a day of the time zone `tz` is complete. `at_most_every`, such as `"4h"`, gathers what got ready meanwhile
    into one run. `on_late` says what a version sealed for a period that fired already does: rerun it, warn, or
    nothing. `start` is the first period to fire, so that a new flow doesn't fire for all of history."""
    if each not in EACH:
        raise ValueError(f"each is one of {', '.join(EACH)}, not {each!r}")
    if on_late not in ON_LATE:
        raise ValueError(f"on_late is one of {', '.join(ON_LATE)}, not {on_late!r}")
    if tz is not None:
        try:
            zone = ZoneInfo(tz)
        except (ValueError, ZoneInfoNotFoundError):
            raise ValueError(f"{tz!r} is not a time zone") from None
    else:
        zone = None
    gather = None if at_most_every is None else parse_duration(at_most_every)
    if gather is not None and gather <= timedelta(0):
        raise ValueError(f"at_most_every needs an interval above zero, got {at_most_every!r}")
    first: datetime | None
    if start is None:
        first = None
    elif isinstance(start, datetime):
        first = start
    elif isinstance(start, date):
        first = datetime(start.year, start.month, start.day, tzinfo=zone)
    else:
        first = datetime.fromisoformat(start)
    if first is not None and first.tzinfo is None:
        first = first.replace(tzinfo=zone)
    return OnReady(ready, each, tz, gather, on_late, first)


def prefixed(trigger: OnReady, package: str) -> OnReady:
    """The trigger with the set's own package's datasets and routes named `<package>/<name>`; a name that has a `/`
    is another package's already."""

    def name(n: str) -> str:
        return n if "/" in n else f"{package}/{n}"

    found = trigger.ready
    sets = replace(
        found,
        datasets=tuple(name(d) for d in found.datasets),
        delivered_to={name(d): name(r) for d, r in found.delivered_to.items()},
    )
    return replace(trigger, ready=sets)


def dataset_spec(dataset: Dataset) -> dict[str, Any]:
    """What readiness needs of a dataset in the catalogue (plan §1.1): its partitions as data and its lookback."""
    lookback = getattr(dataset.strategy, "lookback", None)
    return {
        "partition": scheme_spec(dataset.partition),
        "lookback": lookback.total_seconds() if isinstance(lookback, timedelta) else None,
    }


def readiness_problems(flows: "Sequence[Flow]") -> "list[Problem]":
    """The mistakes in the flows' readiness sets that show without running them (design §5): a set naming a dataset
    no flow reads, a `delivered_to` route that doesn't take its dataset, `by` a field a dataset isn't partitioned on,
    `each="run"` with more than one dataset, `hour` or `day` with no `tz`, and a cycle of flows waiting on each other.
    A dataset named `<package>/<name>` of another package is checked when the flow is activated."""
    from feather_connect.core.flow import Problem

    readers: dict[str, list[Flow]] = {}
    for flow in flows:
        for dataset in flow.datasets:
            readers.setdefault(dataset.name, []).append(flow)
    problems: list[Problem] = []
    waits: dict[str, set[str]] = {}
    for flow in flows:
        for i, trigger in enumerate(flow.triggers):
            if not isinstance(trigger, OnReady):
                continue
            path = f"{flow.name}.triggers[{i}]"
            sets = trigger.ready
            if trigger.each == "run" and len(sets.datasets) > 1:
                problems.append(
                    Problem(
                        "error",
                        "ready_run_needs_one_dataset",
                        f"each='run' fires after each run of one dataset, but the set has {len(sets.datasets)}",
                        path,
                        "use each='hour' or each='day' for a set of several datasets, or split the set",
                    )
                )
            if trigger.each in ("hour", "day") and trigger.tz is None:
                problems.append(
                    Problem(
                        "error",
                        "ready_needs_tz",
                        f"each={trigger.each!r} counts {trigger.each}s in a time zone, and none is given",
                        path,
                        "pass tz=, such as tz='Asia/Kolkata'",
                    )
                )
            for name in sets.datasets:
                found = readers.get(name)
                if found is None:
                    if "/" not in name:
                        problems.append(
                            Problem(
                                "error",
                                "ready_dataset_unread",
                                f"the set waits for dataset {name!r}, which no flow reads",
                                path,
                                "name a dataset a flow of the project reads, or another package's as <package>/<name>",
                            )
                        )
                    continue
                waits.setdefault(flow.name, set()).update(f.name for f in found)
                route = sets.delivered_to.get(name)
                if route is not None and not any(r.name == route for f in found for r in f.routes):
                    problems.append(
                        Problem(
                            "error",
                            "ready_route_unknown",
                            f"the set waits for {name!r} to be delivered to {route!r}, a route that doesn't take it",
                            path,
                            f"name a route that delivers {name!r}",
                        )
                    )
                if sets.by is not None:
                    dataset = next(d for d in found[0].datasets if d.name == name)
                    if sets.by not in spec_fields(scheme_spec(dataset.partition)):
                        problems.append(
                            Problem(
                                "error",
                                "ready_by_unpartitioned",
                                f"by={sets.by!r}, but dataset {name!r} isn't partitioned on {sets.by!r}",
                                path,
                                f"partition {name!r} with partition.by_field({sets.by!r}) or by_fields, or drop by=",
                            )
                        )
    if cycle := _cycle(waits):
        problems.append(
            Problem(
                "error",
                "ready_cycle",
                "flows wait for each other's data: " + " -> ".join(cycle),
                f"{cycle[0]}.triggers",
                "a flow can't wait, through readiness, for data that only it lets through",
            )
        )
    return problems


def _cycle(waits: Mapping[str, set[str]]) -> list[str] | None:
    """A path that comes back to where it started, through 'waits for data read by', or None."""
    state: dict[str, int] = {}
    path: list[str] = []

    def visit(node: str) -> list[str] | None:
        state[node] = 1
        path.append(node)
        for upstream in sorted(waits.get(node, ())):
            if state.get(upstream) == 1:
                return [*path[path.index(upstream) :], upstream]
            if upstream not in state and (found := visit(upstream)):
                return found
        path.pop()
        state[node] = 2
        return None

    for node in sorted(waits):
        if node not in state and (found := visit(node)):
            return found
    return None


@dataclass(frozen=True)
class ReadyInfo:
    """What a run a firing started is for (design §5, `ctx.ready`): the period, the scope, and for `each="run"` the
    upstream runs. A planner that takes `ready=` is given it."""

    period_start: datetime | None
    period_end: datetime | None
    scope: Mapping[str, str]
    runs: tuple[str, ...]
    versions: Mapping[str, Any]

    @classmethod
    def from_params(cls, params: Mapping[str, Any]) -> "ReadyInfo | None":
        raw = params.get("ready")
        if not isinstance(raw, Mapping):
            return None
        found = cast("Mapping[str, Any]", raw)

        def moment(key: str) -> datetime | None:
            value = found.get(key)
            return datetime.fromisoformat(value) if isinstance(value, str) else None

        return cls(
            moment("start"),
            moment("end"),
            dict(found.get("scope") or {}),
            tuple(found.get("runs") or ()),
            dict(found.get("versions") or {}),
        )

"""Flows: what a flow reads, where it delivers and what starts it, and the checks it can run on itself."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import pyarrow as pa

from feather_connect.core.connector import RouteMode, Sink
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset, Priority
from feather_connect.core.partitions import none
from feather_connect.core.records import Record
from feather_connect.core.triggers import OnEvent, Trigger

Overlap = Literal["queue", "skip", "cancel_previous"]
CatchUp = Literal["latest", "each"]
Severity = Literal["error", "warning"]

# A record map gives one payload, several (a record split in two, say) or none (the record is filtered out).
# A batch map, marked by `batch_map`, takes a whole batch as an Arrow table and returns one.
RecordMap = Callable[[Record, Context], Mapping[str, Any] | list[dict[str, Any]] | None]
BatchMap = Callable[[pa.Table, Context], pa.Table]

# Whether a scheme can list its own partitions doesn't depend on the time, so any time will do.
_ANY_TIME = datetime(2000, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class RouteSpec:
    """A path from the datasets of a flow to one destination.

    `name` identifies the route, since a flow may deliver to the same connection twice. `map` changes records
    on the way to `sink`, `mode` says how the sink applies them and `priority` how its calls rank for a
    shared budget.
    """

    name: str
    sink: Sink
    map: RecordMap | BatchMap | None
    mode: RouteMode
    priority: Priority


def to(
    sink: Sink,
    *,
    map: RecordMap | BatchMap | None = None,
    mode: RouteMode = "upsert",
    priority: Priority = "normal",
    name: str | None = None,
) -> RouteSpec:
    """Return a route that delivers to `sink`.

    The route is named after the sink's connection unless `name` says otherwise, so two routes to one
    connection need a name each.
    """
    return RouteSpec(sink.connection if name is None else name, sink, map, mode, priority)


@dataclass(frozen=True)
class Problem:
    """Something wrong with a flow that shows without running it.

    `path` says where, in terms of the arguments to `Flow` (`deliver[0].mode`), and `hint` what to do about
    it. An error means the flow can't run as written. A warning points at something probably not meant.
    """

    severity: Severity
    code: str
    message: str
    path: str
    hint: str


def _dataset_problems(dataset: Dataset, path: str) -> list[Problem]:
    problems: list[Problem] = []
    source = dataset.source.spec
    strategy = dataset.strategy
    if strategy.name not in source.strategies:
        offered = ", ".join(sorted(source.strategies))
        problems.append(
            Problem(
                "error",
                "strategy_unsupported",
                f"{source.name} can't read dataset {dataset.name!r} with the {strategy.name} strategy",
                f"{path}.strategy",
                f"use one of: {offered}" if offered else f"{source.name} declares no strategies",
            )
        )
    if dataset.planner is None and dataset.partition.recent(_ANY_TIME) is None:
        problems.append(
            Problem(
                "error",
                "planner_required",
                f"dataset {dataset.name!r} is partitioned by a scheme that can't list its partitions",
                f"{path}.planner",
                "pass planner=, a function from the run time to the partition keys to read, "
                "such as ['store_id=1509/date=2026-10-18']",
            )
        )
    if strategy.complete and not dataset.keep_state and dataset.partition != none():
        problems.append(
            Problem(
                "warning",
                "moves_need_state",
                f"dataset {dataset.name!r} is read in full by partition without record state, so a record "
                "that moves to another partition may be wrongly deleted when its old partition is read alone",
                f"{path}.keep_state",
                "set keep_state=True so that moves are recognised",
            )
        )
    return problems


def _route_problems(route: RouteSpec, datasets: Sequence[Dataset], path: str) -> list[Problem]:
    problems: list[Problem] = []
    sink = route.sink.spec
    if route.mode not in sink.modes:
        problems.append(
            Problem(
                "error",
                "mode_unsupported",
                f"route {route.name!r}: {sink.name} can't apply in mode {route.mode!r}",
                f"{path}.mode",
                f"use one of: {', '.join(sorted(sink.modes))}",
            )
        )
    if route.mode == "replace_partition":
        for dataset in datasets:
            if not dataset.strategy.complete:
                problems.append(
                    Problem(
                        "error",
                        "mode_needs_complete",
                        f"route {route.name!r} replaces partitions, but dataset {dataset.name!r} uses "
                        f"{dataset.strategy.name}, whose versions are not complete",
                        f"{path}.mode",
                        "read the dataset with snapshot_diff or replace, or deliver in another mode",
                    )
                )
    return problems


class Flow:
    """The deployable unit: the datasets it reads, the routes it delivers to and the triggers that start it.

    `overlap` says what happens when a trigger fires while the last run is still going. `catch_up` says
    what happens after downtime: `"latest"` makes one run cover the gap and `"each"` runs every missed tick.
    """

    def __init__(
        self,
        name: str,
        *,
        reads: Dataset | Sequence[Dataset],
        deliver: Sequence[RouteSpec],
        triggers: Sequence[Trigger] = (),
        owner: str | None = None,
        tags: Sequence[str] = (),
        overlap: Overlap = "queue",
        catch_up: CatchUp = "latest",
    ) -> None:
        self.name = name
        self.datasets: list[Dataset] = [reads] if isinstance(reads, Dataset) else list(reads)
        self.routes: list[RouteSpec] = list(deliver)
        self.triggers = tuple(triggers)
        self.owner = owner
        self.tags = tuple(tags)
        self.overlap = overlap
        self.catch_up = catch_up

    def validate(self) -> list[Problem]:
        """Return every mistake in the flow that shows without running it. A sound flow has none.

        It compares the flow with what its connectors declare in their specs: a route whose sink can't apply
        its mode is one such mistake. Nothing is run and no connection is opened.
        """
        problems: list[Problem] = []
        for i, dataset in enumerate(self.datasets):
            problems += _dataset_problems(dataset, f"reads[{i}]")
        names: set[str] = set()
        for i, route in enumerate(self.routes):
            problems += _route_problems(route, self.datasets, f"deliver[{i}]")
            if route.name in names:
                problems.append(
                    Problem(
                        "error",
                        "route_name_duplicate",
                        f"two routes are named {route.name!r}",
                        f"deliver[{i}].name",
                        "a route is named after its connection unless you pass name=, so name them apart",
                    )
                )
            names.add(route.name)
        for i, trigger in enumerate(self.triggers):
            if isinstance(trigger, OnEvent) and trigger.dataset.strategy.name != "push":
                problems.append(
                    Problem(
                        "error",
                        "trigger_needs_push",
                        f"on_event waits for events, but dataset {trigger.dataset.name!r} uses "
                        f"{trigger.dataset.strategy.name}",
                        f"triggers[{i}]",
                        "give the dataset strategy.push(), or start the flow with cron or every",
                    )
                )
        return problems

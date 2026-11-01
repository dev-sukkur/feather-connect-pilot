"""What the dashboard and the API show. Nothing here changes anything, and nothing here holds a secret: a credential
appears as `***` or not at all.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from cronsim import CronSim

from feather_connect.core.flow import Flow
from feather_connect.core.triggers import Cron, Every, Manual, OnEvent, Trigger
from feather_connect.project import ConfigValue, effective_config
from feather_connect.storage.port import (
    Delivery,
    DeliveryStatus,
    FailureGroup,
    PartitionRow,
    QuarantineRow,
    Run,
    TaskRow,
    Version,
    WorkerRow,
)

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

STALE_AFTER_RUNS = (
    2  # a dataset is stale when its newest version is older than this many intervals of its flow
)
_RECENT_RUNS = 10
_RECENT_VERSIONS = 15
_FAILED_RUNS_SHOWN = 100
_BLOCKED_SHOWN = 2


@dataclass(frozen=True)
class RouteSummary:
    name: str
    connection: str
    paused: bool


@dataclass(frozen=True)
class DatasetSummary:
    name: str
    connection: str
    strategy: str
    priority: str


@dataclass(frozen=True)
class FlowSummary:
    name: str
    owner: str | None
    tags: tuple[str, ...]
    triggers: tuple[str, ...]
    enabled: bool
    paused: bool
    datasets: tuple[DatasetSummary, ...]
    routes: tuple[RouteSummary, ...]
    last_run: Run | None
    last_sealed_at: datetime | None = (
        None  # when any of its datasets last got a version: how fresh the flow is
    )


@dataclass(frozen=True)
class FlowDetail:
    """A flow with its recent runs and the configuration that applies to it, each value with the layer that set it."""

    summary: FlowSummary
    config: tuple[ConfigValue, ...]
    recent_runs: tuple[Run, ...]
    recent_versions: tuple[Version, ...] = ()
    code_shas: tuple[str, ...] = ()  # more than one: part of the fleet runs other code than the rest


@dataclass(frozen=True)
class StaleDataset:
    dataset: str
    flow: str
    last_sealed_at: datetime | None  # None: no version has been sealed yet
    expected_every: timedelta


@dataclass(frozen=True)
class WindowUse:
    units: int
    seconds: int
    used: float


@dataclass(frozen=True)
class BudgetUse:
    name: str
    windows: tuple[WindowUse, ...]


@dataclass(frozen=True)
class Overview:
    deliveries: Mapping[str, int]
    failed_deliveries: int
    open_quarantine: int
    failed_runs: int
    running_runs: int
    paused: tuple[str, ...]
    stale_datasets: tuple[StaleDataset, ...]
    budgets: tuple[BudgetUse, ...]
    workers: tuple[WorkerRow, ...]


@dataclass(frozen=True)
class GridCell:
    route: str
    status: str | None  # None: the route has no delivery of the partition's latest version


@dataclass(frozen=True)
class GridRow:
    partition: str
    label: str
    version_id: str
    records: int
    sealed_at: datetime
    cells: tuple[GridCell, ...]


@dataclass(frozen=True)
class Grid:
    dataset: str
    routes: tuple[str, ...]
    rows: tuple[GridRow, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class RunDetail:
    run: Run
    tasks: tuple[TaskRow, ...]
    versions: tuple[Version, ...]
    deliveries: tuple[
        Delivery, ...
    ] = ()  # of the run's versions on each route of its flow, with their attempts


@dataclass(frozen=True)
class Blocked:
    """A partition a failed delivery holds up: `waiting` later deliveries of it queue behind the failed one on the
    route."""

    dataset: str
    partition: str
    waiting: int


@dataclass(frozen=True)
class FailureView:
    """A group of failed deliveries and the partitions it blocks (up to `_BLOCKED_SHOWN` of them)."""

    group: FailureGroup
    blocked: tuple[Blocked, ...]


@dataclass(frozen=True)
class QuarantineEntry:
    """An open quarantine row with the dataset and partition it belongs to, found through its version if need be."""

    row: QuarantineRow
    dataset: str | None
    partition: str | None


@dataclass(frozen=True)
class ConnectionStatus:
    """A connection as the project defines it. `credentials` names the settings that are secrets, never their values."""

    name: str
    connector: str
    site: str | None
    budget: str | None
    credentials: tuple[str, ...]
    flows: tuple[str, ...]
    last_check_at: datetime | None = None
    last_check_ok: bool | None = None
    last_check_message: str | None = None  # already redacted when it was stored


def _describe(trigger: Trigger) -> str:
    match trigger:
        case Cron(expr, tz):
            return f"cron {expr} ({tz})"
        case Every(interval):
            return f"every {interval}"
        case Manual():
            return "manual"
        case OnEvent(dataset):
            return f"on events for {dataset.name}"


def _interval(flow: Flow, now: datetime) -> timedelta | None:
    """The shortest gap between the flow's scheduled runs, or None if nothing schedules it."""
    gaps: list[timedelta] = []
    for trigger in flow.triggers:
        match trigger:
            case Every(interval):
                gaps.append(interval)
            case Cron(expr, tz):
                times = CronSim(expr, now.astimezone(ZoneInfo(tz)))
                first, second = next(times), next(times)
                gaps.append(second - first)
            case _:
                pass
    return min(gaps, default=None)


class Reads:
    """The reads of the dashboard and the API, over one `AppState`."""

    def __init__(self, state: "AppState") -> None:
        self._state = state
        self._rt = state.runtime
        self._store = state.store

    # Flows.

    def _summary(self, flow: Flow) -> FlowSummary:
        runs = self._store.runs.list_runs(flow=flow.name, limit=1)
        return FlowSummary(
            flow.name,
            flow.owner,
            flow.tags,
            tuple(_describe(t) for t in flow.triggers),
            flow.enabled,
            self._rt.is_flow_paused(flow.name),
            tuple(
                DatasetSummary(d.name, d.source.connection, d.strategy.name, d.priority)
                for d in flow.datasets
            ),
            tuple(
                RouteSummary(r.name, r.sink.connection, self._rt.is_route_paused(r.name)) for r in flow.routes
            ),
            runs[0] if runs else None,
            self._freshness(flow),
        )

    def _freshness(self, flow: Flow) -> datetime | None:
        sealed = [
            v.sealed_at
            for d in flow.datasets
            for v in self._store.versions.list_versions(d.name, limit=1)
            if v.sealed_at is not None
        ]
        return max(sealed, default=None)

    def flows(self) -> list[FlowSummary]:
        return [self._summary(flow) for flow in self._rt.flows()]

    def flow_detail(self, name: str) -> FlowDetail:
        """Raises `KeyError` for an unknown flow."""
        flow = self._rt.flow(name)
        used = {d.source.connection for d in flow.datasets} | {r.sink.connection for r in flow.routes}
        prefixes = (f"flows.{name}.", "settings.", *(f"connections.{c}." for c in sorted(used)))
        config = tuple(
            value
            for value in effective_config(self._state.project)
            if value.path.startswith(prefixes) or value.path == f"flows.{name}"
        )
        recent = self._store.runs.list_runs(flow=name, limit=_RECENT_RUNS)
        versions = sorted(
            (
                v
                for d in flow.datasets
                for v in self._store.versions.list_versions(d.name, limit=_RECENT_VERSIONS)
            ),
            key=lambda v: v.seal_seq or 0,
            reverse=True,
        )[:_RECENT_VERSIONS]
        live = {w.code_sha for w in self._store.workers.list_workers() if w.state != "dead"}
        shas = tuple(sorted(live | {v.code_sha for v in versions}))
        return FlowDetail(self._summary(flow), config, tuple(recent), tuple(versions), shas)

    # Overview.

    def _stale(self) -> list[StaleDataset]:
        now = self._state.clock.now()
        stale: list[StaleDataset] = []
        for flow in self._rt.flows():
            every = _interval(flow, now)
            if every is None or not flow.enabled or self._rt.is_flow_paused(flow.name):
                continue
            for dataset in flow.datasets:
                newest = self._store.versions.list_versions(dataset.name, limit=1)
                sealed = [v for v in newest if v.sealed_at is not None]
                last = sealed[0].sealed_at if sealed else None
                if last is None or now - last > every * STALE_AFTER_RUNS:
                    stale.append(StaleDataset(dataset.name, flow.name, last, every))
        return stale

    def _budgets(self) -> list[BudgetUse]:
        found: list[BudgetUse] = []
        for name in sorted(self._state.project.budgets):
            try:
                usage = self._store.budgets.usage(name)
            except KeyError:  # defined in the project but not in the store yet
                continue
            found.append(
                BudgetUse(name, tuple(WindowUse(u.window.units, u.window.seconds, u.used) for u in usage))
            )
        return found

    def overview(self) -> Overview:
        counts = self._store.reads.delivery_counts()
        runs = self._store.runs
        return Overview(
            counts,
            counts.get("failed", 0),
            len(self._store.quarantine.list_open()),
            len(runs.list_runs(status="failed", limit=_FAILED_RUNS_SHOWN)),
            len(runs.list_runs(status="running", limit=_FAILED_RUNS_SHOWN)),
            tuple(f"{o.target}" for o in self._store.overrides.list_all() if o.key == "paused"),
            tuple(self._stale()),
            tuple(self._budgets()),
            tuple(self._store.workers.list_workers()),
        )

    # The partition grid, runs, failures.

    def partition_grid(
        self, dataset: str, *, since: datetime | None = None, cursor: str | None = None, limit: int = 100
    ) -> Grid:
        """The dataset's partitions, last first, each with the status of its latest version on every route of the flow.

        Raises `KeyError` for an unknown dataset.
        """
        found = self._rt.dataset(dataset)
        routes = next(tuple(r.name for r in f.routes) for f in self._rt.flows() if found in f.datasets)
        rows: list[PartitionRow] = self._store.reads.partition_grid(
            dataset, since=since, cursor=cursor, limit=limit
        )
        shown: list[GridRow] = []
        for row in rows:
            try:
                label = found.partition.label_of(row.partition)
            except (ValueError, KeyError):
                label = row.partition
            cells = tuple(GridCell(route, row.cells.get(route)) for route in routes)
            shown.append(
                GridRow(row.partition, label, row.version_id, row.upserts + row.deletes, row.sealed_at, cells)
            )
        return Grid(dataset, routes, tuple(shown), rows[-1].partition if len(rows) == limit else None)

    def runs(
        self, flow: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> list[Run]:
        return self._store.runs.list_runs(flow=flow, status=status, limit=limit, cursor=cursor)

    def retried_tasks(self, run_ids: list[str]) -> dict[str, int]:
        """For each run, how many of its tasks took more than one attempt. Runs with none are left out."""
        return self._store.reads.retried_tasks(run_ids)

    def run_detail(self, run_id: str) -> RunDetail:
        """Raises `KeyError` for an unknown run."""
        run = self._store.runs.get_run(run_id)
        versions: list[Version] = []
        deliveries: list[Delivery] = []
        if run.flow in {f.name for f in self._rt.flows()}:
            flow = self._rt.flow(run.flow)
            for dataset in flow.datasets:
                versions.extend(self._store.versions.sealed_for_run(run_id, dataset.name))
            for version in versions:
                for route in flow.routes:
                    try:
                        deliveries.append(self._store.ledger.get(route.name, version.version_id))
                    except KeyError:  # not delivered to this route (yet)
                        pass
        tasks = sorted(self._store.runs.list_tasks(run_id), key=lambda t: (t.started_at, t.task_id))
        return RunDetail(run, tuple(tasks), tuple(versions), tuple(deliveries))

    def task_logs(self, task_id: str) -> list[str]:
        return self._store.runs.task_logs(task_id)

    def failures_grouped(
        self, route: str | None = None, cursor: str | None = None, limit: int = 100
    ) -> list[FailureGroup]:
        return self._store.reads.failure_groups(route, limit=limit, cursor=cursor)

    def failure_views(
        self, route: str | None = None, cursor: str | None = None, limit: int = 100
    ) -> list[FailureView]:
        """The failure groups, each with the partitions its failed deliveries block."""
        groups = self.failures_grouped(route, cursor, limit)
        shown = {v for g in groups for v in g.versions[:_BLOCKED_SHOWN]}
        found = {v.version_id: v for v in self._store.versions.get_many(shown)}
        waiting: dict[str, dict[tuple[str, str], int]] = {}
        for name in {g.route for g in groups}:  # one query a route, not one a group
            partitions = [
                found[v].partition
                for g in groups
                if g.route == name
                for v in g.versions[:_BLOCKED_SHOWN]
                if v in found
            ]
            waiting[name] = self._store.reads.waiting(name, partitions)
        views: list[FailureView] = []
        for group in groups:
            blocked: list[Blocked] = []
            for version_id in group.versions[:_BLOCKED_SHOWN]:
                failed = found.get(version_id)
                if failed is not None:
                    count = waiting[group.route].get((failed.dataset, failed.partition), 0)
                    blocked.append(Blocked(failed.dataset, failed.partition, count))
            views.append(FailureView(group, tuple(blocked)))
        return views

    def deliveries(
        self,
        route: str | None = None,
        status: DeliveryStatus | None = None,
        partition: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> list[Delivery]:
        return self._store.ledger.list_deliveries(
            route=route, status=status, partition=partition, limit=limit, cursor=cursor
        )

    def quarantine(self, route: str | None = None) -> list[QuarantineRow]:
        return self._store.quarantine.list_open(route)

    def quarantine_entries(self, route: str | None = None) -> list[QuarantineEntry]:
        """The open quarantine rows with their dataset and partition."""
        found: list[QuarantineEntry] = []
        for row in self.quarantine(route):
            dataset, partition = row.dataset, row.partition
            if dataset is None and row.version_id is not None:
                try:
                    version = self._store.versions.get(row.version_id)
                    dataset, partition = version.dataset, version.partition
                except KeyError:
                    pass
            found.append(QuarantineEntry(row, dataset, partition))
        return found

    def connections(self) -> list[ConnectionStatus]:
        flows = self._rt.flows()
        found: list[ConnectionStatus] = []
        for name, connection in sorted(self._state.project.connections.items()):
            using = tuple(
                f.name
                for f in flows
                if name in {d.source.connection for d in f.datasets} | {r.sink.connection for r in f.routes}
            )
            last = self._store.overrides.get(f"connection:{name}", "last_check") or {}
            found.append(
                ConnectionStatus(
                    name,
                    connection.connector,
                    connection.site,
                    connection.budget,
                    tuple(sorted(connection.secret_refs)),
                    using,
                    datetime.fromisoformat(last["at"]) if last else None,
                    last.get("ok"),
                    last.get("message"),
                )
            )
        return found

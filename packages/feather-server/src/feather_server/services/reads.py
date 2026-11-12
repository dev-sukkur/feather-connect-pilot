"""What the dashboard and the API show. Nothing here changes anything, and nothing here holds a secret: a credential
appears as `***` or not at all.
"""

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from cronsim import CronSim

from feather_connect.core.dataset import Dataset
from feather_connect.core.flow import Flow
from feather_connect.core.readiness import OnReady
from feather_connect.core.triggers import Cron, Every, Manual, OnEvent, Trigger
from feather_connect.project import ConfigValue, effective_config
from feather_connect.runtime.retention import kept_days, kept_inbox_days
from feather_connect.storage.port import (
    STREAM_TRIGGER,
    Attempt,
    Delivery,
    DeliveryStatus,
    FailureGroup,
    PartitionRow,
    QuarantineRow,
    ResponseRow,
    Run,
    TaskRow,
    Version,
    WorkerRow,
)
from feather_server.catalog_view import dataset_of
from feather_server.services.text import span

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

STALE_AFTER_RUNS = (
    2  # a dataset is stale when its newest version is older than this many intervals of its flow
)
_RECENT_RUNS = 10
_TASKS_A_PAGE = 100
_RESPONSES_SHOWN = 500
_FAILED_SHOWN = 500  # failed deliveries of a route looked at for a run's older versions
_VERSIONS_SHOWN = 50  # of each dataset, on a run's page: a stream run holds a day of them
_RECENT_VERSIONS = 15
_FAILED_RUNS_SHOWN = 100
_BLOCKED_SHOWN = 2
_KEPT_DAYS_FOR = timedelta(
    seconds=60
)  # the page refreshes every 10 s, and working this out reads every old day


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
class DatasetRow:
    """A dataset of a flow, with when it last got a version."""

    name: str
    flow: str
    connection: str
    strategy: str
    priority: str
    last_sealed_at: datetime | None


@dataclass(frozen=True)
class RouteRow:
    """A route of a flow, with how many of its deliveries are failed and held, each counted up to a cap."""

    name: str
    flow: str
    connection: str
    paused: bool
    failed: int
    held: int


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
class KeptDay:
    day: date
    why: str


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
    kept_days: tuple[KeptDay, ...] = ()


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
class TickCounts:
    """How the ticks of a daily stream run went. A tick's own reading and delivering are tasks of their own."""

    total: int
    skipped: int
    failed: int
    day: str
    day_open: bool  # it is still that day, so more ticks will come


@dataclass(frozen=True)
class RunDetail:
    """A run and one page of what it holds. A stream run can gather thousands of tasks a day, so `tasks` is a page,
    newest first on a stream run and oldest first otherwise, and `versions` are only its newest."""

    run: Run
    tasks: tuple[TaskRow, ...]
    versions: tuple[Version, ...]
    deliveries: tuple[
        Delivery, ...
    ] = ()  # of the run's versions on each route of its flow, with their attempts
    next_task: str | None = None  # the cursor of the next page of tasks
    task_total: int = 0
    tasks_failed: int = 0
    ticks: TickCounts | None = None  # of a stream run
    versions_total: int = 0  # sealed versions of the run, of which `versions` are the newest


@dataclass(frozen=True)
class DeliveryDetail:
    """One delivery with its attempts and what the destination answered for each record (ruling 5), at most
    `_RESPONSES_SHOWN` of them."""

    delivery: Delivery
    attempts: tuple[Attempt, ...]
    responses: tuple[ResponseRow, ...]


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
            return f"every {span(interval)}"  # 15 min, not 0:15:00 (Stage 2 M13)
        case Manual():
            return "manual"
        case OnEvent(dataset):
            return f"on events for {dataset.name}"
        case OnReady(ready=sets, each=each):
            return f"when {', '.join(sets.datasets)} are ready, each {each}"


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
        self._kept_days: tuple[datetime, tuple[KeptDay, ...]] | None = None

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
        # The newest sealed, not the newest staged: a staged version must not hide them (Stage 2 M8).
        sealed = [self._store.versions.last_sealed_at(d.name) for d in flow.datasets]
        return max((at for at in sealed if at is not None), default=None)

    def flows(self) -> list[FlowSummary]:
        """The flows here and the active flows of installed packages (§13: every flow, the project mode retired)."""
        here = [self._summary(flow) for flow in self._rt.flows()]
        names = {s.name for s in here}
        return here + [s for s in self._packaged_summaries() if s.name not in names]

    def _packaged_summaries(self) -> list[FlowSummary]:
        catalogue = self._state.catalogue
        if catalogue is None:
            return []
        found: list[FlowSummary] = []
        for active in catalogue.flows():
            summary = self._packaged_summary(active.name, tuple(_describe(t) for t in active.triggers))
            if summary is not None:
                found.append(summary)
        return found

    def _packaged_summary(self, name: str, triggers: tuple[str, ...] | None = None) -> FlowSummary | None:
        """A packaged flow's summary, from its catalogue spec (`catalog_view`); None for a flow it doesn't have."""
        try:
            spec = self._store.catalog.flow(name).spec
        except KeyError:
            return None
        datasets = tuple(
            DatasetSummary(
                str(d["name"]),
                str(d.get("connection", "")),
                str(d.get("strategy", "")),
                str(d.get("priority", "normal")),
            )
            for d in spec.get("datasets") or []
        )
        runs = self._store.runs.list_runs(flow=name, limit=1)
        sealed = [self._store.versions.last_sealed_at(d.name) for d in datasets]
        return FlowSummary(
            name,
            spec.get("owner"),
            tuple(spec.get("tags") or ()),
            triggers
            if triggers is not None
            else tuple(str(t.get("kind", t)) for t in spec.get("triggers") or []),
            bool(spec.get("enabled", True)),
            self._rt.is_flow_paused(name),
            datasets,
            tuple(
                RouteSummary(
                    str(r["name"]), str(r.get("connection", "")), self._rt.is_route_paused(str(r["name"]))
                )
                for r in spec.get("routes") or []
            ),
            runs[0] if runs else None,
            max((at for at in sealed if at is not None), default=None),
        )

    def _parts(self, flow: str) -> tuple[tuple[str, ...], tuple[str, ...]] | None:
        """The names of the flow's datasets and routes, from the runtime here or, for a packaged flow, from the
        catalogue (`catalog_view`); None for a flow neither knows."""
        if flow in {f.name for f in self._rt.flows()}:
            found = self._rt.flow(flow)
            return tuple(d.name for d in found.datasets), tuple(r.name for r in found.routes)
        try:
            spec = self._store.catalog.flow(flow).spec
        except KeyError:
            return None
        return (
            tuple(str(d["name"]) for d in spec.get("datasets") or []),
            tuple(str(r["name"]) for r in spec.get("routes") or []),
        )

    def flow_detail(self, name: str) -> FlowDetail:
        """Raises `KeyError` for an unknown flow."""
        if name not in {f.name for f in self._rt.flows()}:
            return self._packaged_detail(name)
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

    def _packaged_detail(self, name: str) -> FlowDetail:
        """A packaged flow's detail, from the catalogue: its configuration is its package's, shown on its page."""
        catalogue = self._state.catalogue
        active = (
            next((f for f in catalogue.flows() if f.name == name), None) if catalogue is not None else None
        )
        summary = self._packaged_summary(
            name, None if active is None else tuple(_describe(x) for x in active.triggers)
        )
        if summary is None:
            raise KeyError(f"no flow is named {name!r}")
        recent = self._store.runs.list_runs(flow=name, limit=_RECENT_RUNS)
        versions = sorted(
            (
                v
                for d in summary.datasets
                for v in self._store.versions.list_versions(d.name, limit=_RECENT_VERSIONS)
            ),
            key=lambda v: v.seal_seq or 0,
            reverse=True,
        )[:_RECENT_VERSIONS]
        return FlowDetail(summary, (), tuple(recent), tuple(versions), ())

    # Overview.

    def _stale(self) -> list[StaleDataset]:
        now = self._state.clock.now()
        stale: list[StaleDataset] = []
        for flow in self._rt.flows():
            every = _interval(flow, now)
            if every is None or not flow.enabled or self._rt.is_flow_paused(flow.name):
                continue
            for dataset in flow.datasets:
                last = self._store.versions.last_sealed_at(dataset.name)
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

    def budgets(self) -> list[BudgetUse]:
        """Every budget of the project with its use now, by window."""
        return self._budgets()

    def budget(self, name: str) -> BudgetUse:
        """One budget's use now. Raises `KeyError` for a budget the project doesn't define."""
        if name not in self._state.project.budgets:
            raise KeyError(f"no budget named {name!r}")
        found = next((b for b in self._budgets() if b.name == name), None)
        return found or BudgetUse(name, ())

    def _kept(self) -> tuple[KeptDay, ...]:
        """Why old days are being kept, worked out here because retention's own report is only on the replica that
        leads it. Two requests at once may both work it out, which does no harm."""
        now = self._state.clock.now()
        if self._kept_days is None or now - self._kept_days[0] >= _KEPT_DAYS_FOR:
            days = kept_days(self._store, self._state.spool, now=now)
            inbox = kept_inbox_days(self._store, now=now)
            found = [*days.items(), *((day, f"inbox: {why}") for day, why in inbox.items())]
            self._kept_days = (now, tuple(KeptDay(day, why) for day, why in sorted(found)))
        return self._kept_days[1]

    def overview(self) -> Overview:
        counts = self._store.reads.delivery_counts()
        runs = self._store.runs
        return Overview(
            counts,
            counts.get("failed", 0),
            self._store.quarantine.count_open(),  # a count, not every open row each poll (Stage 2 M7)
            len(runs.list_runs(status="failed", limit=_FAILED_RUNS_SHOWN)),
            len(runs.list_runs(status="running", limit=_FAILED_RUNS_SHOWN)),
            tuple(f"{o.target}" for o in self._store.overrides.list_all() if o.key == "paused"),
            tuple(self._stale()),
            tuple(self._budgets()),
            tuple(self._store.workers.list_workers()),
            self._kept(),
        )

    # The partition grid, runs, failures.

    def partition_grid(
        self, dataset: str, *, since: datetime | None = None, cursor: str | None = None, limit: int = 100
    ) -> Grid:
        """The dataset's partitions, last first, each with the status of its latest version on every route of the flow.

        Raises `KeyError` for an unknown dataset.
        """
        try:
            found: Dataset | None = self._rt.dataset(dataset)
            routes = next(tuple(r.name for r in f.routes) for f in self._rt.flows() if found in f.datasets)
        except KeyError:  # a packaged dataset: its routes from the catalogue, its partitions shown by key
            found, routes = None, dataset_of(self._state, dataset).routes
        rows: list[PartitionRow] = self._store.reads.partition_grid(
            dataset, since=since, cursor=cursor, limit=limit
        )
        shown: list[GridRow] = []
        for row in rows:
            try:
                label = row.partition if found is None else found.partition.label_of(row.partition)
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

    def has_active_run(self, flow: str) -> bool:
        """Whether the flow has a run queued or running, which a second start would only repeat."""
        return any(self._store.runs.list_runs(flow=flow, status=s, limit=1) for s in ("queued", "running"))

    def retried_tasks(self, run_ids: list[str]) -> dict[str, int]:
        """For each run, how many of its tasks took more than one attempt. Runs with none are left out."""
        return self._store.reads.retried_tasks(run_ids)

    def _ticks(self, run: Run, counts: Mapping[tuple[str, str], int]) -> TickCounts | None:
        if run.trigger != STREAM_TRIGGER:
            return None
        day = str(run.params.get("day", ""))
        today = self._state.clock.now().astimezone(ZoneInfo(self._rt.timezone)).date().isoformat()
        total = sum(n for (kind, _), n in counts.items() if kind in ("plan", "skip"))
        return TickCounts(
            total, counts.get(("skip", "skipped"), 0), counts.get(("plan", "failed"), 0), day, day == today
        )

    def run_detail(self, run_id: str, cursor: str | None = None, limit: int = _TASKS_A_PAGE) -> RunDetail:
        """Return the run with a page of its tasks, `cursor` being the last task of the page before. Raises `KeyError`
        for an unknown run."""
        run = self._store.runs.get_run(run_id)
        versions: list[Version] = []
        deliveries: list[Delivery] = []
        total = 0
        parts = self._parts(run.flow)
        if parts is not None:
            dataset_names, route_names = parts
            for dataset in dataset_names:
                newest = self._store.versions.newest_sealed_for_run(run_id, dataset, _VERSIONS_SHOWN)
                versions.extend(reversed(newest))
                total += (
                    len(newest)
                    if len(newest) < _VERSIONS_SHOWN
                    else len(self._store.versions.sealed_for_run(run_id, dataset))
                )
            shown = {v.version_id for v in versions}
            for version in versions:
                for route in route_names:
                    try:
                        deliveries.append(self._store.ledger.get(route, version.version_id))
                    except KeyError:  # not delivered to this route (yet)
                        pass
            if total > len(versions):  # S15: a failed delivery of an older version of the run is shown too
                for route in route_names:
                    failed = [
                        d
                        for d in self._store.ledger.list_deliveries(
                            route=route, status="failed", limit=_FAILED_SHOWN
                        )
                        if d.version_id not in shown
                    ]
                    ours = {
                        v.version_id
                        for v in self._store.versions.get_many({d.version_id for d in failed})
                        if v.run_id == run_id
                    }
                    deliveries.extend(d for d in failed if d.version_id in ours)
        deliveries.sort(key=lambda d: d.status != "failed")  # failed first, each group in its own order
        tasks = self._store.runs.list_tasks(
            run_id, limit=limit, after_task=cursor, newest_first=run.trigger == STREAM_TRIGGER
        )
        counts = self._store.runs.count_tasks(run_id)
        return RunDetail(
            run,
            tuple(tasks),
            tuple(versions),
            tuple(deliveries),
            tasks[-1].task_id if len(tasks) == limit else None,
            sum(counts.values()),
            sum(n for (_, status), n in counts.items() if status == "failed"),
            self._ticks(run, counts),
            total,
        )

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

    # Versions.

    def version_records(
        self, version_id: str, limit: int, cursor: str | None
    ) -> tuple[Version, list[dict[str, Any]], str | None]:
        """The version, a page of its records from the spool, and the cursor of the next page (`chunk:offset`), or
        None on the last. Raises `KeyError` for an unknown version and `ValueError` for a cursor not of this form."""
        version = self._store.versions.get(version_id)
        chunk_no, offset = 0, 0
        if cursor is not None:
            first, _, second = cursor.partition(":")
            chunk_no, offset = int(first), int(second)
            if chunk_no < 0 or offset < 0:
                raise ValueError("a cursor counts from zero")
        spool = self._state.spool
        chunks = [c.chunk_no for c in spool.list_chunks(version_id)]
        records: list[dict[str, Any]] = []
        next_raw: str | None = None
        for number in (n for n in chunks if n >= chunk_no):
            table = spool.get_chunk(version_id, number)
            start = offset if number == chunk_no else 0
            rows = table.slice(start, limit - len(records)).to_pylist()
            records.extend(rows)
            if len(records) >= limit:
                end = start + len(rows)
                if end < table.num_rows:
                    next_raw = f"{number}:{end}"
                elif number != chunks[-1]:
                    next_raw = f"{chunks[chunks.index(number) + 1]}:0"
                break
        return version, records, next_raw

    def delivery_detail(self, route: str, version_id: str) -> DeliveryDetail:
        """Return a delivery with its attempts and the destination's answers. Raises `KeyError` for an unknown one."""
        delivery = self._store.ledger.get(route, version_id)
        return DeliveryDetail(
            delivery,
            tuple(self._store.ledger.attempts(route, version_id)),
            tuple(self._store.responses.of_delivery(route, version_id, _RESPONSES_SHOWN)),
        )

    def quarantine(
        self, route: str | None = None, *, limit: int | None = None, offset: int = 0
    ) -> list[QuarantineRow]:
        return self._store.quarantine.list_open(route, limit=limit, offset=offset)

    def quarantine_count(self, route: str | None = None) -> int:
        return self._store.quarantine.count_open(route)

    def quarantine_entries(
        self, route: str | None = None, *, limit: int | None = None, offset: int = 0
    ) -> list[QuarantineEntry]:
        """The open quarantine rows with their dataset and partition: `limit` of them from `offset` on, or all."""
        found: list[QuarantineEntry] = []
        for row in self.quarantine(route, limit=limit, offset=offset):
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

    # The dashboard's tiles, sparklines and search: each one query, counted or grouped in the database.

    def count_runs(self, status: str | None = None, since: datetime | None = None) -> int:
        """How many runs have `status` (any, when None) and were created at `since` or later."""
        return self._store.runs.count_runs(status, since)

    def run_activity(self, days: int = 7) -> tuple[list[int], list[int]]:
        """Runs created and runs failed on each of the last `days` UTC days, oldest first."""
        today = self._state.clock.now().astimezone(UTC).date()
        first = today - timedelta(days=days - 1)
        found = self._store.runs.runs_by_day(datetime.combine(first, time.min, UTC))
        span_ = [first + timedelta(days=n) for n in range(days)]
        return [found.get(d, (0, 0))[0] for d in span_], [found.get(d, (0, 0))[1] for d in span_]

    def find_runs(self, prefix: str, limit: int = 5) -> list[Run]:
        """The newest runs whose id starts with `prefix`."""
        return self._store.runs.find_runs(prefix, limit)

    def workers(self) -> list[WorkerRow]:
        return self._store.workers.list_workers()

    def run(self, run_id: str) -> Run:
        """One run. Raises `KeyError` for an unknown one."""
        return self._store.runs.get_run(run_id)

    def datasets(self) -> list[DatasetRow]:
        flows = self._rt.flows()
        sealed = self._store.versions.last_sealed_many([d.name for f in flows for d in f.datasets])
        return [
            DatasetRow(
                d.name, flow.name, d.source.connection, d.strategy.name, d.priority, sealed.get(d.name)
            )
            for flow in flows
            for d in flow.datasets
        ]

    def stale_datasets(self) -> list[StaleDataset]:
        return self._stale()

    def routes(self) -> list[RouteRow]:
        flows = self._rt.flows()
        counts = self._store.ledger.count_by_status(
            [r.name for f in flows for r in f.routes], ["failed", "held"]
        )
        return [
            RouteRow(
                r.name,
                flow.name,
                r.sink.connection,
                self._rt.is_route_paused(r.name),
                counts.get((r.name, "failed"), 0),
                counts.get((r.name, "held"), 0),
            )
            for flow in flows
            for r in flow.routes
        ]

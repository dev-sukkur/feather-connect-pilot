"""The store in memory, for tests and the in-process engine. It passes the same `StoreContract` as any backend."""

import copy
import math
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from fractions import Fraction
from typing import Any

from feather_connect.clock import Clock, SystemClock
from feather_connect.ids import new_id
from feather_connect.storage.port import (
    CLOSED,
    Attempt,
    BudgetSpec,
    BudgetStore,
    CatalogStore,
    Delivery,
    DeliveryStatus,
    FlowRow,
    Grant,
    Ledger,
    Priority,
    QuarantineRow,
    QuarantineStore,
    RecordStateStore,
    RouteRow,
    Run,
    RunJournal,
    ScheduleStore,
    StateRow,
    Store,
    TaskRow,
    Version,
    VersionDraft,
    VersionSealed,
    VersionStore,
    WindowUsage,
    WorkerRegistry,
    WorkerRow,
    WorkerState,
    check_transition,
)


@dataclass
class _Data:
    """Everything the store holds. One lock covers it all, so that sealing and the ledger stay consistent."""

    clock: Clock
    lock: threading.RLock = field(default_factory=threading.RLock)
    runs: dict[str, Run] = field(default_factory=dict[str, Run])
    tasks: dict[str, TaskRow] = field(default_factory=dict[str, TaskRow])
    logs: dict[str, list[tuple[datetime, str]]] = field(default_factory=dict[str, list[tuple[datetime, str]]])
    versions: dict[str, Version] = field(default_factory=dict[str, Version])
    seal_seq: int = 0
    slices: dict[tuple[str, str], dict[str, bool]] = field(
        default_factory=dict[tuple[str, str], dict[str, bool]]
    )
    deliveries: dict[tuple[str, str], Delivery] = field(default_factory=dict[tuple[str, str], Delivery])
    changed: dict[tuple[str, str], datetime] = field(default_factory=dict[tuple[str, str], datetime])
    attempts: dict[tuple[str, str], list[Attempt]] = field(
        default_factory=dict[tuple[str, str], list[Attempt]]
    )
    state: dict[str, dict[str, StateRow]] = field(default_factory=dict[str, dict[str, StateRow]])
    quarantine: dict[tuple[str | None, str | None, str, str | None], QuarantineRow] = field(
        default_factory=dict[tuple[str | None, str | None, str, str | None], QuarantineRow]
    )

    flows: list[FlowRow] = field(default_factory=list[FlowRow])
    routes: list[RouteRow] = field(default_factory=list[RouteRow])
    retired: set[str] = field(default_factory=set[str])  # names of flows, and of routes, that are retired
    workers: dict[str, WorkerRow] = field(default_factory=dict[str, WorkerRow])
    ticks: dict[tuple[str, str], datetime] = field(default_factory=dict[tuple[str, str], datetime])


class _Runs(RunJournal):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def create_run(
        self,
        flow: str,
        trigger: str,
        actor: str,
        params: Mapping[str, Any],
        status: str = "queued",
        reason: str | None = None,
    ) -> str:
        with self._d.lock:
            now = self._d.clock.now()
            run_id = new_id()
            self._d.runs[run_id] = Run(run_id, flow, trigger, actor, dict(params), status, reason, now, now)
            return run_id

    def set_run_status(self, run_id: str, status: str, reason: str | None = None) -> None:
        with self._d.lock:
            run = self.get_run(run_id)
            self._d.runs[run_id] = replace(
                run,
                status=status,
                reason=run.reason if reason is None else reason,
                updated_at=self._d.clock.now(),
            )

    def get_run(self, run_id: str) -> Run:
        with self._d.lock:
            return self._d.runs[run_id]

    def list_runs(
        self, flow: str | None = None, status: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> list[Run]:
        with self._d.lock:
            runs = sorted(self._d.runs.values(), key=lambda r: r.run_id, reverse=True)
        runs = [
            r
            for r in runs
            if (flow is None or r.flow == flow)
            and (status is None or r.status == status)
            and (cursor is None or r.run_id < cursor)
        ]
        return runs[:limit]

    def start_task(
        self,
        run_id: str,
        kind: str,
        ref: str,
        dataset: str | None = None,
        partition: str | None = None,
        route: str | None = None,
        worker: str | None = None,
        code_sha: str | None = None,
    ) -> str:
        with self._d.lock:
            for existing in self._d.tasks.values():
                if existing.run_id == run_id and existing.ref == ref:
                    return existing.task_id
            task_id = new_id()
            self._d.tasks[task_id] = TaskRow(
                task_id,
                run_id,
                kind,
                ref,
                dataset,
                partition,
                route,
                worker,
                code_sha,
                "running",
                None,
                self._d.clock.now(),
                None,
            )
            self._d.logs[task_id] = []
            return task_id

    def finish_task(self, task_id: str, status: str, error: str | None = None) -> None:
        with self._d.lock:
            self._d.tasks[task_id] = replace(
                self._d.tasks[task_id], status=status, error=error, finished_at=self._d.clock.now()
            )

    def list_tasks(self, run_id: str) -> list[TaskRow]:
        with self._d.lock:
            return [t for t in self._d.tasks.values() if t.run_id == run_id]

    def log(self, task_id: str, level: str, message: str) -> None:
        with self._d.lock:
            self._d.logs[task_id].append((self._d.clock.now(), f"{level}: {message}"))

    def task_logs(self, task_id: str) -> list[str]:
        with self._d.lock:
            return [line for _, line in self._d.logs[task_id]]

    def delete_runs_before(self, cutoff: datetime) -> int:
        with self._d.lock:
            open_runs = {
                self._d.versions[v].run_id
                for (_, v), d in self._d.deliveries.items()
                if d.status not in CLOSED and v in self._d.versions
            }
            gone = {
                r.run_id
                for r in self._d.runs.values()
                if r.updated_at < cutoff
                and r.status not in ("queued", "running")
                and r.run_id not in open_runs
            }
            for task_id in [t.task_id for t in self._d.tasks.values() if t.run_id in gone]:
                del self._d.tasks[task_id]
                del self._d.logs[task_id]
            for key in [k for k in self._d.slices if k[0] in gone]:
                del self._d.slices[key]
            for run_id in gone:
                del self._d.runs[run_id]
            return len(gone)

    def delete_logs_before(self, cutoff: datetime) -> int:
        with self._d.lock:
            removed = 0
            for task_id, lines in self._d.logs.items():
                kept = [(at, line) for at, line in lines if at >= cutoff]
                removed += len(lines) - len(kept)
                self._d.logs[task_id] = kept
            return removed


class _Versions(VersionStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def stage(self, draft: VersionDraft) -> Version:
        with self._d.lock:
            existing = self._d.versions.get(draft.version_id)
            if existing is not None and existing.status == "sealed":
                raise VersionSealed(f"version {draft.version_id} is sealed")
            version = Version(**vars(draft))
            self._d.versions[draft.version_id] = version
            return version

    def seal(self, version_ids: Sequence[str], routes: Sequence[str]) -> list[Version]:
        with self._d.lock:
            missing = [v for v in version_ids if v not in self._d.versions]
            if missing:
                raise KeyError(
                    f"no such version: {missing[0]}"
                )  # before sealing any, so it is all or nothing
            sealed: list[Version] = []
            for version_id in version_ids:
                version = self._d.versions[version_id]
                if version.status != "sealed":
                    self._d.seal_seq += 1
                    version = replace(
                        version, status="sealed", seal_seq=self._d.seal_seq, sealed_at=self._d.clock.now()
                    )
                    self._d.versions[version_id] = version
                    for route in routes:
                        self._d.deliveries[(route, version_id)] = Delivery(
                            route, version_id, version.dataset, version.partition, self._d.seal_seq, "pending"
                        )
                        self._d.attempts[(route, version_id)] = []
                        self._d.changed[(route, version_id)] = self._d.clock.now()
                sealed.append(version)
            return sealed

    def discard_staged(self, version_id: str) -> None:
        with self._d.lock:
            existing = self._d.versions.get(version_id)
            if existing is not None and existing.status == "sealed":
                raise VersionSealed(f"version {version_id} is sealed")
            self._d.versions.pop(version_id, None)

    def get(self, version_id: str) -> Version:
        with self._d.lock:
            return self._d.versions[version_id]

    def _sealed(self, dataset: str, partition: str | None) -> list[Version]:
        """The dataset's sealed versions, newest first."""
        found = [
            v
            for v in self._d.versions.values()
            if v.status == "sealed"
            and v.dataset == dataset
            and (partition is None or v.partition == partition)
        ]
        return sorted(found, key=lambda v: v.seal_seq or 0, reverse=True)

    def latest_sealed(
        self, dataset: str, partition: str, before_seal_seq: int | None = None
    ) -> Version | None:
        with self._d.lock:
            found = self._sealed(dataset, partition)
        if before_seal_seq is not None:
            found = [v for v in found if (v.seal_seq or 0) < before_seal_seq]
        return next(iter(found), None)

    def since_watermark(self, dataset: str, partition: str, since: Any) -> list[Version]:
        with self._d.lock:
            return [
                v
                for v in self._sealed(dataset, partition)
                if v.watermark is not None and v.watermark >= since
            ]

    def latest_watermark(self, dataset: str) -> Any | None:
        with self._d.lock:
            marks = [v.watermark for v in self._sealed(dataset, None) if v.watermark is not None]
        return max(marks, default=None)

    def register_slices(self, run_id: str, dataset: str, partitions: Iterable[str]) -> None:
        with self._d.lock:
            slices = self._d.slices.setdefault((run_id, dataset), {})
            for partition in partitions:
                slices.setdefault(partition, False)  # a replay must not undo finished slices

    def finish_slice(self, run_id: str, dataset: str, partition: str) -> bool:
        with self._d.lock:
            slices = self._d.slices[(run_id, dataset)]
            if partition not in slices:
                raise KeyError(f"slice {partition} is not registered for run {run_id}, dataset {dataset}")
            slices[partition] = True
            return all(slices.values())

    def slices_for_run(self, run_id: str, dataset: str) -> list[str]:
        with self._d.lock:
            return list(self._d.slices.get((run_id, dataset), {}))

    def staged_for_run(self, run_id: str, dataset: str) -> list[Version]:
        with self._d.lock:
            return [
                v
                for v in self._d.versions.values()
                if v.status == "staged" and v.run_id == run_id and v.dataset == dataset
            ]

    def sealed_for_run(self, run_id: str, dataset: str) -> list[Version]:
        with self._d.lock:
            found = [v for v in self._sealed(dataset, None) if v.run_id == run_id]
        return found[::-1]  # `_sealed` is newest first

    def list_versions(self, dataset: str, partition: str | None = None, limit: int = 100) -> list[Version]:
        with self._d.lock:
            found = [
                v
                for v in self._d.versions.values()
                if v.dataset == dataset and (partition is None or v.partition == partition)
            ]
        return found[::-1][:limit]  # newest staged first

    def get_many(self, version_ids: Iterable[str]) -> list[Version]:
        with self._d.lock:
            return [self._d.versions[v] for v in dict.fromkeys(version_ids) if v in self._d.versions]

    def runs_ready_to_seal(self) -> list[tuple[str, str]]:
        with self._d.lock:
            staged = {(v.run_id, v.dataset) for v in self._d.versions.values() if v.status == "staged"}
            return sorted(
                key for key, slices in self._d.slices.items() if key in staged and all(slices.values())
            )

    def sealed_before(self, cutoff: datetime, limit: int = 1000) -> list[Version]:
        with self._d.lock:
            newest = {(v.dataset, v.partition): v.version_id for v in self._sealed_all()}
            delivered = {version_id for _, version_id in self._d.deliveries}
            old = [
                v
                for v in self._sealed_all()
                if v.sealed_at is not None
                and v.sealed_at < cutoff
                and newest[(v.dataset, v.partition)] != v.version_id
                and v.version_id not in delivered
            ]
        return old[:limit]

    def delete_many(self, version_ids: Iterable[str]) -> int:
        with self._d.lock:
            found = [v for v in dict.fromkeys(version_ids) if v in self._d.versions]
            for version_id in found:
                del self._d.versions[version_id]
            return len(found)

    def _sealed_all(self) -> list[Version]:
        """Every sealed version, oldest first."""
        return sorted(
            (v for v in self._d.versions.values() if v.status == "sealed"), key=lambda v: v.seal_seq or 0
        )


class _Ledger(Ledger):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def oldest_open(self, route: str, partition: str) -> Delivery | None:
        with self._d.lock:
            open_ = [
                d
                for d in self._d.deliveries.values()
                if d.route == route and d.partition == partition and d.status not in CLOSED
            ]
        return min(open_, key=lambda d: d.seal_seq, default=None)

    def get(self, route: str, version_id: str) -> Delivery:
        with self._d.lock:
            return self._d.deliveries[(route, version_id)]

    def transition(
        self,
        route: str,
        version_id: str,
        to: DeliveryStatus,
        *,
        actor: str,
        reason: str | None = None,
        error: str | None = None,
        rows_applied: int = 0,
        rows_rejected: int = 0,
        code_sha: str | None = None,
    ) -> Delivery:
        with self._d.lock:
            current = self.get(route, version_id)
            check_transition(current.status, to)
            applied = to == "applied"
            updated = replace(
                current,
                status=to,
                attempts=current.attempts + (to == "running"),
                last_error=None if applied else (current.last_error if error is None else error),
                rows_applied=rows_applied if applied or rows_applied else current.rows_applied,
                rows_rejected=rows_rejected if applied or rows_rejected else current.rows_rejected,
                replays=current.replays + (current.status == "applied" and to == "pending"),
                retries=current.retries + (current.status == "failed" and to == "pending"),
            )
            self._d.deliveries[(route, version_id)] = updated
            self._d.changed[(route, version_id)] = self._d.clock.now()
            self._d.attempts[(route, version_id)].append(
                Attempt(
                    route,
                    version_id,
                    current.status,
                    to,
                    actor,
                    reason,
                    error,
                    rows_applied,
                    rows_rejected,
                    code_sha,
                    self._d.clock.now(),
                )
            )
            return updated

    def attempts(self, route: str, version_id: str) -> list[Attempt]:
        with self._d.lock:
            return list(self._d.attempts[(route, version_id)])

    def list_deliveries(
        self,
        route: str | None = None,
        status: DeliveryStatus | None = None,
        partition: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> list[Delivery]:
        after = None
        if cursor is not None:
            seq, _, after_route = cursor.partition(":")
            after = (int(seq), after_route)
        with self._d.lock:
            found = sorted(self._d.deliveries.values(), key=lambda d: (d.seal_seq, d.route))
        found = [
            d
            for d in found
            if (route is None or d.route == route)
            and (status is None or d.status == status)
            and (partition is None or d.partition == partition)
            and (after is None or (d.seal_seq, d.route) > after)
        ]
        return found[:limit]

    def all_applied(self) -> bool:
        with self._d.lock:
            return all(d.status == "applied" for d in self._d.deliveries.values())

    def pending_older_than(self, age: timedelta, *, now: datetime | None = None) -> list[Delivery]:
        cutoff = (self._d.clock.now() if now is None else now) - age
        with self._d.lock:
            found = [
                d
                for key, d in self._d.deliveries.items()
                if d.status == "pending" and self._d.changed[key] < cutoff
            ]
        return sorted(found, key=lambda d: (d.seal_seq, d.route))

    def open_versions(self, version_ids: Iterable[str]) -> set[str]:
        wanted = set(version_ids)
        with self._d.lock:
            return {v for (_, v), d in self._d.deliveries.items() if v in wanted and d.status not in CLOSED}

    def delete_closed_before(self, cutoff: datetime) -> int:
        with self._d.lock:
            gone = [
                k for k, d in self._d.deliveries.items() if d.status in CLOSED and self._d.changed[k] < cutoff
            ]
            for key in gone:
                del self._d.deliveries[key]
                del self._d.attempts[key]
                del self._d.changed[key]
            return len(gone)


class _State(RecordStateStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def get_many(self, dataset: str, keys: Iterable[str]) -> dict[str, StateRow]:
        with self._d.lock:
            rows = self._d.state.get(dataset, {})
            return {key: rows[key] for key in keys if key in rows}

    def put_many(self, dataset: str, rows: Iterable[StateRow]) -> None:
        with self._d.lock:
            table = self._d.state.setdefault(dataset, {})
            for row in rows:
                table[row.key] = replace(row, payload=dict(row.payload))

    def delete_many(self, dataset: str, keys: Iterable[str]) -> None:
        with self._d.lock:
            table = self._d.state.get(dataset, {})
            for key in keys:
                table.pop(key, None)


class _Quarantine(QuarantineStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def add(
        self,
        route: str | None,
        version_id: str | None,
        key: str,
        error: str,
        dataset: str | None = None,
        partition: str | None = None,
    ) -> None:
        with self._d.lock:
            self._d.quarantine[(route, version_id, key, dataset)] = QuarantineRow(
                route, version_id, key, error, "open", dataset, partition
            )

    def open_extraction_keys(self, dataset: str, keys: Iterable[str], outside: str | None = None) -> set[str]:
        wanted = set(keys)
        with self._d.lock:
            return {
                r.key
                for r in self._d.quarantine.values()
                if r.status == "open"
                and r.route is None
                and r.dataset == dataset
                and r.key in wanted
                and (outside is None or r.partition != outside)
            }

    def close_extraction(self, dataset: str, keys: Iterable[str]) -> int:
        wanted = set(keys)
        with self._d.lock:
            closed = 0
            for entry_key, r in self._d.quarantine.items():
                if r.status == "open" and r.route is None and r.dataset == dataset and r.key in wanted:
                    self._d.quarantine[entry_key] = replace(r, status="superseded")
                    closed += 1
            return closed

    def supersede(self, route: str, key: str, before_seal_seq: int) -> int:
        with self._d.lock:
            closed = 0
            for entry_key, row in self._d.quarantine.items():
                if row.route != route or row.key != key or row.status != "open" or row.version_id is None:
                    continue
                seal_seq = self._d.versions[row.version_id].seal_seq
                if seal_seq is not None and seal_seq < before_seal_seq:
                    self._d.quarantine[entry_key] = replace(row, status="superseded")
                    closed += 1
            return closed

    def list_open(self, route: str | None = None) -> list[QuarantineRow]:
        with self._d.lock:
            return [
                r
                for r in self._d.quarantine.values()
                if r.status == "open" and (route is None or r.route == route)
            ]


@dataclass
class _Bucket:
    """The tokens left in one window, as of `at`."""

    tokens: Fraction
    at: datetime


class _Budgets(BudgetStore):
    def __init__(self, data: _Data) -> None:
        self._d = data
        self._specs: dict[str, BudgetSpec] = {}
        self._buckets: dict[str, list[_Bucket]] = {}

    def define(self, spec: BudgetSpec) -> None:
        with self._d.lock:
            if self._specs.get(spec.name) == spec:
                return
            now = self._d.clock.now()
            self._specs[spec.name] = spec
            self._buckets[spec.name] = [_Bucket(Fraction(w.units), now) for w in spec.windows]

    def _refilled(self, name: str) -> tuple[BudgetSpec, list[_Bucket]]:
        """Bring the buckets of the budget up to now, and return them."""
        spec, buckets = self._specs[name], self._buckets[name]
        now = self._d.clock.now()
        for window, bucket in zip(spec.windows, buckets, strict=True):
            earned = (
                Fraction(window.units)
                * Fraction((now - bucket.at) // timedelta(microseconds=1), 1_000_000)
                / window.seconds
            )
            bucket.tokens = min(Fraction(window.units), bucket.tokens + earned)
            bucket.at = now
        return spec, buckets

    def acquire(self, name: str, units: int, priority: Priority = "normal") -> Grant:
        with self._d.lock:
            spec, buckets = self._refilled(name)
            kept = (
                Fraction(spec.reserve.get("high", 0.0)).limit_denominator()
                if priority == "low"
                else Fraction(0)
            )
            wait = Fraction(0)
            for window, bucket in zip(spec.windows, buckets, strict=True):
                held_back = window.units * kept
                if units > window.units - held_back:
                    raise ValueError(
                        f"{units} units are more than {priority} requests can ever take from {name!r}"
                    )
                short = units + held_back - bucket.tokens
                if short > 0:
                    wait = max(wait, short * window.seconds / window.units)
            if wait > 0:
                return Grant(False, timedelta(microseconds=math.ceil(wait * 1_000_000)))
            for bucket in buckets:
                bucket.tokens -= units
            return Grant(True)

    def usage(self, name: str) -> list[WindowUsage]:
        with self._d.lock:
            spec, buckets = self._refilled(name)
            return [
                WindowUsage(window, float(window.units - bucket.tokens))
                for window, bucket in zip(spec.windows, buckets, strict=True)
            ]


def _latest[R: (FlowRow, RouteRow)](rows: Sequence[R], retired: set[str]) -> list[R]:
    """Return the latest row of each name that isn't retired, by name. A later row wins a tie."""
    latest: dict[str, R] = {}
    for row in rows:
        if row.name in retired:
            continue
        if row.name not in latest or row.registered_at >= latest[row.name].registered_at:
            latest[row.name] = row
    return sorted(latest.values(), key=lambda r: r.name)


class _Catalog(CatalogStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def sync(self, project: str, code_sha: str, specs: Mapping[str, Any]) -> None:
        d = self._d
        with d.lock:
            now = d.clock.now()
            gone = {f.name for f in d.flows if f.project == project and f.name not in specs}
            d.retired |= gone | {r.name for r in d.routes if r.flow in gone}
            d.flows = [f for f in d.flows if not (f.name in specs and f.code_sha == code_sha)]
            d.routes = [r for r in d.routes if not (r.flow in specs and r.code_sha == code_sha)]
            for name, spec in specs.items():
                kept = {route["name"] for route in spec["routes"]}
                d.retired |= {r.name for r in d.routes if r.flow == name and r.name not in kept}
                d.retired.discard(name)
                d.flows.append(FlowRow(name, project, code_sha, copy.deepcopy(spec), now))
                for route in spec["routes"]:
                    d.retired.discard(route["name"])
                    d.routes.append(RouteRow(route["name"], name, code_sha, copy.deepcopy(route), now))

    def flows(self) -> list[FlowRow]:
        with self._d.lock:
            return _latest(self._d.flows, self._d.retired)

    def flow(self, name: str, code_sha: str | None = None) -> FlowRow:
        with self._d.lock:
            rows = [f for f in self._d.flows if f.name == name and code_sha in (None, f.code_sha)]
            if not rows:
                raise KeyError(f"no flow {name!r}" + ("" if code_sha is None else f" at {code_sha!r}"))
            return max(rows, key=lambda f: f.registered_at)

    def routes(self) -> list[RouteRow]:
        with self._d.lock:
            return _latest(self._d.routes, self._d.retired)


class _Workers(WorkerRegistry):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def heartbeat(self, worker_id: str, labels: Mapping[str, str], code_sha: str, slots: int) -> None:
        with self._d.lock:
            before = self._d.workers.get(worker_id)
            state: WorkerState = "draining" if before is not None and before.state == "draining" else "alive"
            self._d.workers[worker_id] = WorkerRow(
                worker_id, dict(labels), code_sha, slots, self._d.clock.now(), state
            )

    def list_workers(self) -> list[WorkerRow]:
        with self._d.lock:
            return [self._d.workers[w] for w in sorted(self._d.workers)]

    def stale(self, older_than: timedelta) -> list[WorkerRow]:
        cutoff = self._d.clock.now() - older_than
        return [w for w in self.list_workers() if w.state != "dead" and w.last_seen < cutoff]

    def mark(self, worker_id: str, state: WorkerState) -> None:
        with self._d.lock:
            if worker_id not in self._d.workers:
                raise KeyError(f"no worker {worker_id!r}")
            self._d.workers[worker_id] = replace(self._d.workers[worker_id], state=state)


class _Schedules(ScheduleStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def last_tick(self, flow: str, trigger_id: str) -> datetime | None:
        with self._d.lock:
            return self._d.ticks.get((flow, trigger_id))

    def set_last_tick(self, flow: str, trigger_id: str, at: datetime) -> None:
        with self._d.lock:
            self._d.ticks[(flow, trigger_id)] = at


class MemoryStore(Store):
    """A `Store` that keeps everything in memory. `clock` defaults to the system clock."""

    def __init__(self, clock: Clock | None = None) -> None:
        data = _Data(clock or SystemClock())
        super().__init__(
            runs=_Runs(data),
            versions=_Versions(data),
            ledger=_Ledger(data),
            state=_State(data),
            quarantine=_Quarantine(data),
            budgets=_Budgets(data),
            catalog=_Catalog(data),
            workers=_Workers(data),
            schedules=_Schedules(data),
        )

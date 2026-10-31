"""The store in memory, for tests and the in-process engine. It passes the same `StoreContract` as any backend."""

import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from feather_connect.clock import Clock, SystemClock
from feather_connect.ids import new_id
from feather_connect.storage.port import (
    CLOSED,
    Attempt,
    Delivery,
    DeliveryStatus,
    Ledger,
    QuarantineRow,
    QuarantineStore,
    RecordStateStore,
    Run,
    RunJournal,
    StateRow,
    Store,
    TaskRow,
    Version,
    VersionDraft,
    VersionSealed,
    VersionStore,
    check_transition,
)


@dataclass
class _Data:
    """Everything the store holds. One lock covers it all, so that sealing and the ledger stay consistent."""

    clock: Clock
    lock: threading.RLock = field(default_factory=threading.RLock)
    runs: dict[str, Run] = field(default_factory=dict[str, Run])
    tasks: dict[str, TaskRow] = field(default_factory=dict[str, TaskRow])
    logs: dict[str, list[str]] = field(default_factory=dict[str, list[str]])
    versions: dict[str, Version] = field(default_factory=dict[str, Version])
    seal_seq: int = 0
    slices: dict[tuple[str, str], dict[str, bool]] = field(
        default_factory=dict[tuple[str, str], dict[str, bool]]
    )
    deliveries: dict[tuple[str, str], Delivery] = field(default_factory=dict[tuple[str, str], Delivery])
    attempts: dict[tuple[str, str], list[Attempt]] = field(
        default_factory=dict[tuple[str, str], list[Attempt]]
    )
    state: dict[str, dict[str, StateRow]] = field(default_factory=dict[str, dict[str, StateRow]])
    quarantine: dict[tuple[str | None, str | None, str, str | None], QuarantineRow] = field(
        default_factory=dict[tuple[str | None, str | None, str, str | None], QuarantineRow]
    )


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
            self._d.logs[task_id].append(f"{level}: {message}")

    def task_logs(self, task_id: str) -> list[str]:
        with self._d.lock:
            return list(self._d.logs[task_id])


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
        )

"""The store in memory, for tests and the in-process engine. It passes the same `StoreContract` as any backend."""

import copy
import json
import math
import threading
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from fractions import Fraction
from typing import Any

from feather_connect.clock import Clock, SystemClock
from feather_connect.core.connector import InboundRequest
from feather_connect.ids import new_id, stable_id
from feather_connect.storage.port import (
    CLOSED,
    ENV_KINDS,
    MAX_ENV_ROWS,
    MAX_PACKAGE_ROWS,
    STREAM_TRIGGER,
    WAITS_KEPT,
    ActivationState,
    Attempt,
    AuditRow,
    AuditStore,
    BudgetSpec,
    BudgetStore,
    BudgetWait,
    CatalogStore,
    Delivery,
    DeliveryStatus,
    DuplicateFile,
    DuplicateRegistry,
    DuplicateUser,
    DuplicateVersion,
    EnvConfigStore,
    EnvEntry,
    EnvKind,
    EnvState,
    FailureGroup,
    Firing,
    FlowActivation,
    FlowRow,
    Grant,
    HostState,
    InboxDay,
    InboxEvent,
    InboxStore,
    InUse,
    KeyMismatch,
    KeySighting,
    Ledger,
    Override,
    OverrideStore,
    PackageEnv,
    PackageHost,
    PackageState,
    PackageStore,
    PackageVersion,
    PartitionClose,
    PartitionRow,
    Priority,
    QuarantineRow,
    QuarantineStore,
    ReadinessStore,
    ReadStore,
    RecordStateStore,
    RegistryFile,
    RegistryRow,
    RegistryStore,
    ResponseRow,
    ResponseStore,
    Role,
    RouteRow,
    Run,
    RunJournal,
    ScheduleStore,
    SealedPartition,
    SealedSecret,
    SecretStore,
    SliceFinished,
    StaleWriter,
    StateRow,
    Store,
    TaskRow,
    TokenRow,
    TokenScope,
    UserRow,
    UserStore,
    Version,
    VersionDraft,
    VersionNotReady,
    VersionSealed,
    VersionStore,
    WebhookEvent,
    WebhookEventStore,
    WindowUsage,
    WorkerRegistry,
    WorkerRow,
    WorkerState,
    account_mismatch,
    check_transition,
    check_wheel,
    split_error,
    version_key,
)


@dataclass
class _Data:
    """Everything the store holds. One lock covers it all, so that sealing and the ledger stay consistent."""

    clock: Clock
    lock: threading.RLock = field(default_factory=threading.RLock)
    runs: dict[str, Run] = field(default_factory=dict[str, Run])
    tasks: dict[str, TaskRow] = field(default_factory=dict[str, TaskRow])
    stream_runs: dict[tuple[str, str], str] = field(default_factory=dict[tuple[str, str], str])
    logs: dict[str, list[tuple[datetime, str]]] = field(default_factory=dict[str, list[tuple[datetime, str]]])
    versions: dict[str, Version] = field(default_factory=dict[str, Version])
    seal_seq: int = 0
    writers: dict[str, str] = field(default_factory=dict[str, str])  # who holds the claim of a version
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
    # By delivery (route, version), then by (chunk, key).
    responses: dict[tuple[str, str], dict[tuple[int, str], ResponseRow]] = field(
        default_factory=dict[tuple[str, str], dict[tuple[int, str], ResponseRow]]
    )

    flows: list[FlowRow] = field(default_factory=list[FlowRow])
    routes: list[RouteRow] = field(default_factory=list[RouteRow])
    retired: set[str] = field(default_factory=set[str])  # names of flows, and of routes, that are retired
    workers: dict[str, WorkerRow] = field(default_factory=dict[str, WorkerRow])
    overrides: dict[tuple[str, str], Override] = field(default_factory=dict[tuple[str, str], Override])
    ticks: dict[tuple[str, str], datetime] = field(default_factory=dict[tuple[str, str], datetime])
    users: dict[str, UserRow] = field(default_factory=dict[str, UserRow])  # by username
    tokens: dict[str, TokenRow] = field(default_factory=dict[str, TokenRow])  # by token id
    registry_files: dict[str, tuple[RegistryFile, bytes]] = field(
        default_factory=dict[str, tuple[RegistryFile, bytes]]
    )  # by filename, in upload order
    registries: dict[str, RegistryRow] = field(default_factory=dict[str, RegistryRow])
    packages: dict[str, datetime] = field(default_factory=dict[str, datetime])  # name: created at
    package_versions: dict[tuple[str, str], PackageVersion] = field(
        default_factory=dict[tuple[str, str], PackageVersion]
    )  # by (name, version_key), in install order
    wheels: dict[str, bytes] = field(default_factory=dict[str, bytes])  # by sha256
    renames: dict[tuple[str, str], str] = field(
        default_factory=dict[tuple[str, str], str]
    )  # (kind, old): new
    activations: dict[tuple[str, str, str], FlowActivation] = field(
        default_factory=dict[tuple[str, str, str], FlowActivation]
    )  # by (flow, package, version_key)
    catalog_listeners: list[Callable[[], None]] = field(default_factory=list[Callable[[], None]])
    package_envs: dict[tuple[str, str], PackageEnv] = field(default_factory=dict[tuple[str, str], PackageEnv])
    package_hosts: dict[tuple[str, str, str], PackageHost] = field(
        default_factory=dict[tuple[str, str, str], PackageHost]
    )
    env_config: dict[tuple[str, str, str], EnvEntry] = field(
        default_factory=dict[tuple[str, str, str], EnvEntry]
    )
    secrets: dict[tuple[str, str], SealedSecret] = field(default_factory=dict[tuple[str, str], SealedSecret])
    secret_keys: dict[str, str] = field(default_factory=dict[str, str])
    webhook_events: dict[tuple[str, str, str], WebhookEvent] = field(
        default_factory=dict[tuple[str, str, str], WebhookEvent]
    )  # by (package, webhook, key), in arrival order
    audit: list[AuditRow] = field(default_factory=list[AuditRow])
    # The key index, by (dataset, key): (version, partition, op, row hash, chunk), in seal order.
    keys: dict[tuple[str, str], list[tuple[str, str, str, int, int]]] = field(
        default_factory=dict[tuple[str, str], list[tuple[str, str, str, int, int]]]
    )
    spool: Any = None  # the spool bound to this store, which sealing reads the keys from


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

    def set_run_result(self, run_id: str, result: Mapping[str, Any]) -> None:
        with self._d.lock:
            run = self.get_run(run_id)
            self._d.runs[run_id] = replace(run, result=dict(result), updated_at=self._d.clock.now())

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

    def tagged_versions(self, statuses: Sequence[str] = ("queued", "running")) -> set[tuple[str, str]]:
        with self._d.lock:
            runs = list(self._d.runs.values())
        found: set[tuple[str, str]] = set()
        for r in runs:
            package, version = r.params.get("package"), r.params.get("version")
            if r.status in statuses and isinstance(package, str) and isinstance(version, str):
                found.add((package, version))
        return found

    def queued_of(self, package: str, version: str, limit: int = 50) -> list[Run]:
        with self._d.lock:
            runs = sorted(self._d.runs.values(), key=lambda r: (r.created_at, r.run_id))
        return [
            r
            for r in runs
            if r.status == "queued"
            and r.params.get("package") == package
            and r.params.get("version") == version
        ][:limit]

    def count_runs(self, status: str | None = None, since: datetime | None = None) -> int:
        with self._d.lock:
            return sum(
                1
                for r in self._d.runs.values()
                if (status is None or r.status == status) and (since is None or r.created_at >= since)
            )

    def runs_by_day(self, since: datetime) -> dict[date, tuple[int, int]]:
        found: dict[date, tuple[int, int]] = {}
        with self._d.lock:
            for r in self._d.runs.values():
                if r.created_at >= since:
                    day = r.created_at.astimezone(UTC).date()
                    made, failed = found.get(day, (0, 0))
                    found[day] = (made + 1, failed + (r.status == "failed"))
        return found

    def find_runs(self, prefix: str, limit: int = 5) -> list[Run]:
        wanted = prefix.upper()
        with self._d.lock:
            runs = sorted(
                (r for r in self._d.runs.values() if r.run_id.startswith(wanted)),
                key=lambda r: r.run_id,
                reverse=True,
            )
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

    def record_attempt(self, task_id: str, attempt: int, error: str) -> None:
        with self._d.lock:
            self._d.tasks[task_id] = replace(self._d.tasks[task_id], attempt=attempt, error=error)

    def finish_task(self, task_id: str, status: str, error: str | None = None) -> None:
        with self._d.lock:
            task = self._d.tasks[task_id]
            self._d.tasks[task_id] = replace(
                task,
                status=status,
                error=task.error if error is None else error,
                finished_at=self._d.clock.now(),
            )

    def stream_run(self, flow: str, day: str) -> str:
        with self._d.lock:
            found = self._d.stream_runs.get((flow, day))
            if found is None:
                found = self.create_run(flow, STREAM_TRIGGER, "system", {"day": day}, status="succeeded")
                self._d.stream_runs[(flow, day)] = found
            return found

    def list_tasks(
        self,
        run_id: str,
        *,
        status: str | None = None,
        kinds: Sequence[str] | None = None,
        limit: int | None = None,
        after_task: str | None = None,
        newest_first: bool = False,
    ) -> list[TaskRow]:
        with self._d.lock:
            rows = [t for t in self._d.tasks.values() if t.run_id == run_id]
        if newest_first:
            rows.reverse()
        if after_task is not None:
            at = next((i for i, row in enumerate(rows) if row.task_id == after_task), None)
            rows = [] if at is None else rows[at + 1 :]
        rows = [
            t for t in rows if (status is None or t.status == status) and (kinds is None or t.kind in kinds)
        ]
        return rows if limit is None else rows[:limit]

    def get_task(self, task_id: str) -> TaskRow:
        with self._d.lock:
            return self._d.tasks[task_id]

    def task_by_ref(self, run_id: str, ref: str) -> TaskRow | None:
        return next((t for t in self.list_tasks(run_id) if t.ref == ref), None)

    def tasks_by_ref(self, run_id: str, prefix: str) -> list[TaskRow]:
        return [t for t in self.list_tasks(run_id) if t.ref.startswith(prefix)]

    def count_tasks(self, run_id: str) -> Mapping[tuple[str, str], int]:
        return Counter((t.kind, t.status) for t in self.list_tasks(run_id))

    def log(self, task_id: str, level: str, message: str) -> None:
        with self._d.lock:
            self._d.logs.setdefault(task_id, []).append((self._d.clock.now(), f"{level}: {message}"))

    def task_logs(self, task_id: str) -> list[str]:
        with self._d.lock:
            return [line for _, line in self._d.logs.get(task_id, [])]

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

    @property
    def lock(self) -> threading.RLock:
        return self._d.lock

    def attach_spool(self, spool: Any) -> None:
        """Be told the spool that holds this store's versions, whose keys sealing indexes."""
        self._d.spool = spool

    def _index(self, version: Version) -> None:
        spool = self._d.spool
        if spool is None:
            return
        for ref in spool.list_chunks(version.version_id):
            table = spool.get_chunk(version.version_id, ref.chunk_no)
            for key, op, row_hash in zip(
                table.column("_fc_key").to_pylist(),
                table.column("_fc_op").to_pylist(),
                table.column("_fc_row_hash").to_pylist(),
                strict=True,
            ):
                self._d.keys.setdefault((version.dataset, key), []).append(
                    (version.version_id, version.partition, op, row_hash, ref.chunk_no)
                )

    def _sightings_of(self, dataset: str, key: str) -> list[KeySighting]:
        found: list[KeySighting] = []
        for version_id, partition, op, row_hash, chunk_no in self._d.keys.get((dataset, key), []):
            version = self._d.versions[version_id]
            assert version.seal_seq is not None and version.sealed_at is not None
            found.append(
                KeySighting(
                    version_id,
                    dataset,
                    partition,
                    key,
                    op,
                    row_hash,
                    chunk_no,
                    version.run_id,
                    version.seal_seq,
                    version.sealed_at,
                )
            )
        return found

    def keys_page(
        self,
        dataset: str,
        version_ids: Sequence[str],
        *,
        after: tuple[str, str] | None = None,
        prefix: str | None = None,
        limit: int = 50,
    ) -> list[KeySighting]:
        wanted = set(version_ids)
        with self._d.lock:
            found = [
                s
                for (ds, key) in self._d.keys
                if ds == dataset and (not prefix or key.startswith(prefix))
                for s in self._sightings_of(ds, key)
                if s.version_id in wanted and (after is None or (s.key, s.version_id) > after)
            ]
        return sorted(found, key=lambda s: (s.key, s.version_id))[:limit]

    def earlier_sightings(
        self, dataset: str, keys: Sequence[str], before_seal_seq: int
    ) -> dict[str, KeySighting]:
        with self._d.lock:
            found: dict[str, KeySighting] = {}
            for key in keys:
                earlier = [s for s in self._sightings_of(dataset, key) if s.seal_seq < before_seal_seq]
                if earlier:
                    found[key] = max(earlier, key=lambda s: s.seal_seq)
            return found

    def key_history(self, dataset: str, key: str, limit: int = 100) -> list[KeySighting]:
        with self._d.lock:
            found: list[KeySighting] = []
            for version_id, partition, op, row_hash, chunk_no in self._d.keys.get((dataset, key), []):
                version = self._d.versions[version_id]
                assert version.seal_seq is not None and version.sealed_at is not None
                found.append(
                    KeySighting(
                        version_id,
                        dataset,
                        partition,
                        key,
                        op,
                        row_hash,
                        chunk_no,
                        version.run_id,
                        version.seal_seq,
                        version.sealed_at,
                    )
                )
        return found[-limit:]

    def check_writable(self, version_id: str, writer: str | None) -> None:
        with self._d.lock:
            existing = self._d.versions.get(version_id)
            if existing is not None and existing.status == "sealed":
                raise VersionSealed(f"version {version_id} is sealed")
            if writer is not None and self._d.writers.get(version_id) != writer:
                raise StaleWriter(f"version {version_id} was claimed by a later execution")

    def claim(self, version_id: str, writer: str, run_id: str, dataset: str, partition: str) -> None:
        with self._d.lock:
            if self._d.slices.get((run_id, dataset), {}).get(partition):
                raise SliceFinished(f"slice {partition} of run {run_id} is done")
            self.check_writable(version_id, None)
            self._d.writers[version_id] = writer
            self._d.versions.pop(version_id, None)

    def take_over(self, version_ids: Sequence[str], writer: str) -> None:
        with self._d.lock:
            for version_id in version_ids:
                self.check_writable(version_id, None)
            self._d.writers.update(dict.fromkeys(version_ids, writer))

    def stage(self, draft: VersionDraft, writer: str | None = None) -> Version:
        with self._d.lock:
            self.check_writable(draft.version_id, writer)
            version = Version(**vars(draft))
            self._d.versions[draft.version_id] = version
            return version

    def seal(
        self, version_ids: Sequence[str], routes: Sequence[str], writer: str | None = None
    ) -> list[Version]:
        with self._d.lock:
            if writer is not None:  # before the rest: a version that a later claim took away is not "missing"
                for version_id in version_ids:
                    found = self._d.versions.get(version_id)
                    if (found is None or found.status != "sealed") and self._d.writers.get(
                        version_id
                    ) != writer:
                        raise StaleWriter(f"version {version_id} was claimed by a later execution")
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
                    self._index(version)
                    for route in routes:
                        self._d.deliveries[(route, version_id)] = Delivery(
                            route, version_id, version.dataset, version.partition, self._d.seal_seq, "pending"
                        )
                        self._d.attempts[(route, version_id)] = []
                        self._d.changed[(route, version_id)] = self._d.clock.now()
                sealed.append(version)
            return sealed

    def discard_staged(self, version_id: str, writer: str | None = None) -> None:
        with self._d.lock:
            self.check_writable(version_id, writer)
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
        found: list[Version] = []
        with self._d.lock:
            for version in self._sealed(dataset, partition):
                if version.watermark is None:
                    continue
                if version.watermark < since:
                    break  # see the port
                found.append(version)
        return found

    def last_sealed_at(self, dataset: str) -> datetime | None:
        with self._d.lock:
            sealed = [v.sealed_at for v in self._d.versions.values() if v.dataset == dataset and v.sealed_at]
            return sealed[-1] if sealed else None  # by staging, as the versions are kept in that order

    def last_sealed_many(self, datasets: Sequence[str]) -> dict[str, datetime]:
        found = {d: self.last_sealed_at(d) for d in datasets}
        return {d: at for d, at in found.items() if at is not None}

    def latest_watermark(self, dataset: str) -> Any | None:
        with self._d.lock:
            marks = [v.watermark for v in self._sealed(dataset, None) if v.watermark is not None]
        return max(marks, default=None)

    def register_slices(self, run_id: str, dataset: str, partitions: Iterable[str]) -> None:
        with self._d.lock:
            slices = self._d.slices.setdefault((run_id, dataset), {})
            for partition in partitions:
                slices.setdefault(partition, False)  # a replay must not undo finished slices

    def finish_slice(self, run_id: str, dataset: str, partition: str, writer: str | None = None) -> bool:
        with self._d.lock:
            slices = self._d.slices[(run_id, dataset)]
            if partition not in slices:
                raise KeyError(f"slice {partition} is not registered for run {run_id}, dataset {dataset}")
            version_id = stable_id(run_id, dataset, partition)
            if writer is not None and not slices[partition] and self._d.writers.get(version_id) != writer:
                raise StaleWriter(f"slice {partition} of run {run_id} was claimed by a later execution")
            slices[partition] = True
            return all(slices.values())

    def slices_for_run(self, run_id: str, dataset: str) -> list[str]:
        with self._d.lock:
            return list(self._d.slices.get((run_id, dataset), {}))

    def staged_for_run(self, run_id: str, dataset: str, tick: str | None = None) -> list[Version]:
        with self._d.lock:
            return [
                v
                for v in self._d.versions.values()
                if v.status == "staged"
                and v.run_id == run_id
                and v.dataset == dataset
                and (tick is None or v.tick == tick)
            ]

    def newest_sealed_for_run(self, run_id: str, dataset: str, limit: int) -> list[Version]:
        with self._d.lock:
            return [v for v in self._sealed(dataset, None) if v.run_id == run_id][:limit]

    def sealed_for_run(self, run_id: str, dataset: str, tick: str | None = None) -> list[Version]:
        with self._d.lock:
            found = [
                v
                for v in self._sealed(dataset, None)
                if v.run_id == run_id and (tick is None or v.tick == tick)
            ]
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
            staged = {
                (v.run_id, v.dataset)
                for v in self._d.versions.values()
                if v.status == "staged" and not self._ended_badly(v.run_id)
            }
            return sorted(
                key for key, slices in self._d.slices.items() if key in staged and all(slices.values())
            )

    def _ended_badly(self, run_id: str) -> bool:
        run = self._d.runs.get(run_id)
        return run is not None and run.status in ("failed", "cancelled")

    def abandoned(self, limit: int = 1000) -> list[Version]:
        with self._d.lock:
            return [
                v
                for v in self._d.versions.values()
                if v.status == "staged"
                and self._ended_badly(v.run_id)
                and self._d.runs[v.run_id].trigger != STREAM_TRIGGER
            ][:limit]

    def stream_leftovers(self, limit: int = 1000) -> list[Version]:
        with self._d.lock:
            return [
                v
                for v in self._d.versions.values()
                if v.status == "staged"
                and v.tick is not None
                and self._d.runs[v.run_id].trigger == STREAM_TRIGGER
            ][:limit]

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
                self._d.writers.pop(version_id, None)
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

    def latest_before(
        self,
        route: str,
        partition: str,
        dataset: str,
        before_seq: int,
        status: DeliveryStatus | None = "applied",
    ) -> Delivery | None:
        with self._d.lock:
            found = [
                d
                for d in self._d.deliveries.values()
                if d.route == route
                and d.partition == partition
                and d.dataset == dataset
                and (status is None or d.status == status)
                and d.seal_seq < before_seq
            ]
        return max(found, key=lambda d: d.seal_seq, default=None)

    def delivered_routes(self, dataset: str, routes: Sequence[str]) -> set[str]:
        wanted = set(routes)
        with self._d.lock:
            return {
                d.route for d in self._d.deliveries.values() if d.dataset == dataset and d.route in wanted
            }

    def has_deliveries(self, dataset: str) -> bool:
        with self._d.lock:
            return any(d.dataset == dataset for d in self._d.deliveries.values())

    def count_by_status(
        self, routes: Sequence[str], statuses: Sequence[DeliveryStatus]
    ) -> dict[tuple[str, DeliveryStatus], int]:
        found: dict[tuple[str, DeliveryStatus], int] = {}
        with self._d.lock:
            for d in self._d.deliveries.values():
                if d.route in routes and d.status in statuses:
                    found[(d.route, d.status)] = found.get((d.route, d.status), 0) + 1
        return found

    def all_applied(self) -> bool:
        with self._d.lock:
            return all(d.status == "applied" for d in self._d.deliveries.values())

    def pending_older_than(
        self, age: timedelta, *, now: datetime | None = None, limit: int = 1000
    ) -> list[Delivery]:
        cutoff = (self._d.clock.now() if now is None else now) - age
        with self._d.lock:
            next_up: dict[tuple[str, str, str], Delivery] = {}
            for delivery in sorted(self._d.deliveries.values(), key=lambda d: d.seal_seq):
                if delivery.status not in CLOSED:
                    stream = (delivery.route, delivery.dataset, delivery.partition)
                    next_up.setdefault(stream, delivery)
            found = [
                d
                for d in next_up.values()
                if d.status == "pending" and self._d.changed[(d.route, d.version_id)] < cutoff
            ]
        return sorted(found, key=lambda d: (d.seal_seq, d.route))[:limit]

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
                self._d.responses.pop(key, None)
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
        *,
        payload: Mapping[str, Any] | None = None,
        raw: str | None = None,
    ) -> None:
        with self._d.lock:
            self._d.quarantine[(route, version_id, key, dataset)] = QuarantineRow(
                route,
                version_id,
                key,
                error,
                "open",
                dataset,
                partition,
                None if payload is None else copy.deepcopy(dict(payload)),
                raw,
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

    def list_open(
        self, route: str | None = None, *, limit: int | None = None, offset: int = 0
    ) -> list[QuarantineRow]:
        with self._d.lock:
            found = [
                r
                for r in self._d.quarantine.values()
                if r.status == "open" and (route is None or r.route == route)
            ]
        return found[offset:] if limit is None else found[offset : offset + limit]

    def count_open(self, route: str | None = None) -> int:
        return len(self.list_open(route))

    def of_versions(
        self,
        version_ids: Sequence[str],
        route: str | None,
        keys: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[QuarantineRow]:
        wanted, named = set(version_ids), None if keys is None else set(keys)
        with self._d.lock:
            found = [
                r
                for r in self._d.quarantine.values()
                if r.version_id in wanted and r.route == route and (named is None or r.key in named)
            ]
        return found[:limit]

    def of_key(self, dataset: str, key: str, version_ids: Sequence[str]) -> list[QuarantineRow]:
        wanted = set(version_ids)
        with self._d.lock:
            return [
                r
                for r in self._d.quarantine.values()
                if r.key == key
                and (
                    (r.version_id is not None and r.version_id in wanted)
                    or (r.route is None and r.dataset == dataset)
                )
            ]


class _Responses(ResponseStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def record(self, rows: Sequence[ResponseRow]) -> None:
        with self._d.lock:
            now = self._d.clock.now()
            for row in rows:
                found = self._d.responses.setdefault((row.route, row.version_id), {})
                before = found.get((row.chunk_no, row.key))
                fields = row.fields or (before.fields if before is not None else {})
                kept = replace(row, fields=copy.deepcopy(dict(fields)), sent=copy.deepcopy(row.sent), at=now)
                found[(row.chunk_no, row.key)] = kept

    def replace_chunk(self, route: str, version_id: str, chunk_no: int, rows: Sequence[ResponseRow]) -> None:
        with self._d.lock:
            keys = {row.key for row in rows}
            found = self._d.responses.get((route, version_id), {})
            for chunk, key in [k for k in found if k[0] == chunk_no and k[1] not in keys]:
                del found[(chunk, key)]
            self.record(rows)

    def of_delivery(self, route: str, version_id: str, limit: int = 1000) -> list[ResponseRow]:
        with self._d.lock:
            found = self._d.responses.get((route, version_id), {})
            return [copy.deepcopy(found[k]) for k in sorted(found)[:limit]]

    def for_keys(self, route: str, version_id: str, keys: Sequence[str]) -> dict[str, ResponseRow]:
        found = {key: self.find(route, version_id, key) for key in keys}
        return {key: row for key, row in found.items() if row is not None}

    def find(self, route: str, version_id: str, key: str) -> ResponseRow | None:
        with self._d.lock:
            found = self._d.responses.get((route, version_id), {})
            chunks = sorted(chunk for chunk, k in found if k == key)
            return copy.deepcopy(found[(chunks[-1], key)]) if chunks else None


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
        self._waits: dict[tuple[str, str], BudgetWait] = {}

    def define(self, spec: BudgetSpec) -> None:
        with self._d.lock:
            if self._specs.get(spec.name) == spec:
                return
            now = self._d.clock.now()
            shared = [s for s in self._specs.values() if s.key == spec.key and s.name != spec.name]
            if shared and shared[0].windows != spec.windows:
                raise ValueError(account_mismatch(spec, repr(shared[0].name), shared[0].windows))
            self._specs[spec.name] = spec
            if not shared or spec.key not in self._buckets:
                self._buckets[spec.key] = [_Bucket(Fraction(w.units), now) for w in spec.windows]

    def _refilled(self, name: str) -> tuple[BudgetSpec, list[_Bucket]]:
        """Bring the buckets of the budget up to now, and return them."""
        spec = self._specs[name]
        buckets = self._buckets[spec.key]
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

    def note_wait(self, name: str, task_ref: str, run_id: str | None, until: datetime) -> None:
        with self._d.lock:
            now = self._d.clock.now()
            for key in [k for k, w in self._waits.items() if w.until < now - WAITS_KEPT]:
                del self._waits[key]
            found = self._waits.get((name, task_ref))
            self._waits[(name, task_ref)] = (
                BudgetWait(name, task_ref, run_id, now, until, 1)
                if found is None
                else replace(found, until=until, waits=found.waits + 1, run_id=run_id or found.run_id)
            )

    def waits(self, name: str) -> list[BudgetWait]:
        with self._d.lock:
            now = self._d.clock.now()
            return sorted(
                (w for (budget, _), w in self._waits.items() if budget == name and w.until > now),
                key=lambda w: (w.until, w.task_ref),
            )


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

    def forget_stopped(self, before: datetime) -> int:
        with self._d.lock:
            gone = [
                w for w, row in self._d.workers.items() if row.state != "alive" and row.last_seen < before
            ]
            for worker_id in gone:
                del self._d.workers[worker_id]
            return len(gone)


class _Schedules(ScheduleStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def last_tick(self, flow: str, trigger_id: str) -> datetime | None:
        with self._d.lock:
            return self._d.ticks.get((flow, trigger_id))

    def set_last_tick(self, flow: str, trigger_id: str, at: datetime) -> None:
        with self._d.lock:
            self._d.ticks[(flow, trigger_id)] = at

    def advance_tick(self, flow: str, trigger_id: str, seen: datetime | None, at: datetime) -> bool:
        with self._d.lock:
            if self._d.ticks.get((flow, trigger_id)) != seen:
                return False
            self._d.ticks[(flow, trigger_id)] = at
            return True


class _Overrides(OverrideStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def set(
        self,
        target: str,
        key: str,
        value: Any,
        *,
        actor: str,
        reason: str | None = None,
        expires_at: datetime | None = None,
    ) -> None:
        with self._d.lock:
            self._d.overrides[(target, key)] = Override(
                target, key, copy.deepcopy(value), reason, actor, self._d.clock.now(), expires_at
            )

    def get(self, target: str, key: str) -> Any | None:
        with self._d.lock:
            found = self._d.overrides.get((target, key))
            return None if found is None or self._expired(found) else copy.deepcopy(found.value)

    def delete_expired(self) -> int:
        with self._d.lock:
            gone = [k for k, found in self._d.overrides.items() if self._expired(found)]
            for k in gone:
                del self._d.overrides[k]
            return len(gone)

    def claim(
        self,
        target: str,
        key: str,
        value: Any,
        *,
        actor: str,
        reason: str | None = None,
        expires_at: datetime | None = None,
    ) -> bool:
        with self._d.lock:
            held = self._d.overrides.get((target, key))
            if held is not None and not self._expired(held):
                return False
            self._d.overrides[(target, key)] = Override(
                target, key, copy.deepcopy(value), reason, actor, self._d.clock.now(), expires_at
            )
            return True

    def list_all(self) -> list[Override]:
        with self._d.lock:
            return [o for _, o in sorted(self._d.overrides.items()) if not self._expired(o)]

    def clear(self, target: str, key: str, *, actor: str, reason: str | None = None) -> None:
        with self._d.lock:
            self._d.overrides.pop((target, key), None)

    def _expired(self, override: Override) -> bool:
        return override.expires_at is not None and override.expires_at <= self._d.clock.now()


class _Users(UserStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def create_user(self, username: str, password_hash: str, role: Role) -> str:
        with self._d.lock:
            if username in self._d.users:
                raise DuplicateUser(username)
            user_id = new_id()
            self._d.users[username] = UserRow(user_id, username, password_hash, self._d.clock.now(), role)
            return user_id

    def get_user(self, username: str) -> UserRow | None:
        with self._d.lock:
            return self._d.users.get(username)

    def list_users(self) -> list[UserRow]:
        with self._d.lock:
            return sorted(self._d.users.values(), key=lambda user: user.username)

    def set_password_hash(self, username: str, password_hash: str) -> None:
        with self._d.lock:
            self._d.users[username] = replace(self._d.users[username], password_hash=password_hash)

    def set_role(self, username: str, role: Role) -> None:
        with self._d.lock:
            self._d.users[username] = replace(self._d.users[username], role=role)

    def create_token(
        self,
        name: str,
        token_hash: str,
        expires_at: datetime | None,
        role: Role,
        created_by: str | None,
        scope: TokenScope = "all",
    ) -> str:
        with self._d.lock:
            if any(t.token_hash == token_hash for t in self._d.tokens.values()):
                raise ValueError("a token with that hash exists")
            token_id = new_id()
            self._d.tokens[token_id] = TokenRow(
                token_id, name, token_hash, self._d.clock.now(), expires_at, None, role, created_by, scope
            )
            return token_id

    def find_token(self, token_hash: str) -> TokenRow | None:
        with self._d.lock:
            return next((t for t in self._d.tokens.values() if t.token_hash == token_hash), None)

    def revoke_token(self, token_id: str) -> None:
        with self._d.lock:
            found = self._d.tokens.get(token_id)
            if found is not None and found.revoked_at is None:
                self._d.tokens[token_id] = replace(found, revoked_at=self._d.clock.now())


class _Registry(RegistryStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def put_file(self, file: RegistryFile, content: bytes) -> None:
        with self._d.lock:
            taken = any(
                f.filename == file.filename
                or (f.project, version_key(f.version)) == (file.project, version_key(file.version))
                for f, _ in self._d.registry_files.values()
            )
            if taken:
                raise DuplicateFile(f"{file.project} {file.version}")
            self._d.registry_files[file.filename] = (file, bytes(content))

    def projects(self) -> list[str]:
        with self._d.lock:
            return sorted({f.project for f, _ in self._d.registry_files.values()})

    def files(self, project: str) -> list[RegistryFile]:
        with self._d.lock:
            return [f for f, _ in self._d.registry_files.values() if f.project == project]

    def file(self, filename: str) -> RegistryFile | None:
        with self._d.lock:
            found = self._d.registry_files.get(filename)
            return None if found is None else found[0]

    def content(self, filename: str) -> bytes | None:
        with self._d.lock:
            found = self._d.registry_files.get(filename)
            return None if found is None else found[1]

    def yank(self, project: str, version: str, reason: str | None) -> bool:
        with self._d.lock:
            for name, (f, content) in self._d.registry_files.items():
                if (f.project, f.version) == (project, version):
                    if f.yanked_at is None:
                        changed = replace(f, yanked_at=self._d.clock.now(), yanked_reason=reason)
                        self._d.registry_files[name] = (changed, content)
                    return True
            return False

    def add_registry(self, name: str, index_url: str, token_ref: str | None, added_by: str) -> RegistryRow:
        with self._d.lock:
            if name in self._d.registries:
                raise DuplicateRegistry(name)
            row = RegistryRow(name, index_url, token_ref, added_by, self._d.clock.now())
            self._d.registries[name] = row
            return row

    def registries(self) -> list[RegistryRow]:
        with self._d.lock:
            return [self._d.registries[name] for name in sorted(self._d.registries)]

    def get_registry(self, name: str) -> RegistryRow | None:
        with self._d.lock:
            return self._d.registries.get(name)

    def remove_registry(self, name: str) -> bool:
        with self._d.lock:
            return self._d.registries.pop(name, None) is not None


class _Packages(PackageStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def install(
        self, version: PackageVersion, content: bytes, flows: Sequence[str], reason: str | None = None
    ) -> None:
        key = (version.name, version_key(version.version))
        check_wheel(version, content)
        with self._d.lock:
            if key in self._d.package_versions:
                raise DuplicateVersion(f"{version.name} {version.version}")
            self._d.packages.setdefault(version.name, version.installed_at)
            self._d.wheels.setdefault(version.sha256, bytes(content))
            self._d.package_versions[key] = version
            for flow in flows:
                self._d.activations[(flow, version.name, key[1])] = FlowActivation(
                    flow,
                    version.name,
                    version.version,
                    "inactive",
                    version.installed_by,
                    version.installed_at,
                    reason,
                )

    def packages(self) -> list[str]:
        with self._d.lock:
            return sorted(self._d.packages)[:MAX_PACKAGE_ROWS]

    def versions(self, name: str | None = None) -> list[PackageVersion]:
        with self._d.lock:
            found = [v for v in self._d.package_versions.values() if name is None or v.name == name]
        found.sort(key=lambda v: (v.name, v.installed_at))
        return found[:MAX_PACKAGE_ROWS]

    def version(self, name: str, version: str) -> PackageVersion | None:
        with self._d.lock:
            return self._d.package_versions.get((name, version_key(version)))

    def wheel(self, sha256: str) -> bytes | None:
        with self._d.lock:
            return self._d.wheels.get(sha256)

    def uninstall(self, name: str, version: str, by: str, at: datetime, reason: str | None = None) -> bool:
        key = (name, version_key(version))
        with self._d.lock:
            row = self._d.package_versions.get(key)
            if row is None or row.state == "uninstalled":
                raise KeyError(f"{name} {version} is not installed here")
            busy = [
                a
                for (_, package, vkey), a in self._d.activations.items()
                if package == name and vkey == key[1] and a.state in ("active", "activating")
            ]
            if busy:
                raise InUse(f"{name} {row.version}: its flow {busy[0].flow} is {busy[0].state}")
            runs = [
                r
                for r in self._d.runs.values()
                if r.status in ("queued", "running")
                and r.params.get("package") == name
                and r.params.get("version") == row.version
            ]
            if runs:
                raise InUse(
                    f"{name} {row.version} has {runs[0].status} runs: let them finish, or cancel them"
                )
            self._d.package_versions[key] = replace(row, state="uninstalled")
            shared = any(
                v.sha256 == row.sha256 and v.state != "uninstalled" for v in self._d.package_versions.values()
            )
            if not shared:
                self._d.wheels.pop(row.sha256, None)
            listeners = list(self._d.catalog_listeners)
        for listener in listeners:
            listener()
        return not shared

    def needed_lock_hashes(self) -> set[str]:
        with self._d.lock:
            return {v.lock_hash for v in self._d.package_versions.values() if v.state != "uninstalled"}

    def renames(self) -> dict[tuple[str, str], str]:
        with self._d.lock:
            return dict(self._d.renames)

    def record_renames(self, renames: Mapping[tuple[str, str], str]) -> None:
        """As the upgrade from the project mode records them, for tests: an old name that has one keeps it."""
        with self._d.lock:
            for key, new in renames.items():
                self._d.renames.setdefault(key, new)

    def activations(self, name: str, version: str | None = None) -> list[FlowActivation]:
        wanted = None if version is None else version_key(version)
        with self._d.lock:
            found = [
                a
                for (_, package, key), a in self._d.activations.items()
                if package == name and (wanted is None or key == wanted)
            ]
        found.sort(key=lambda a: (a.flow, a.changed_at, a.version))
        return found[:MAX_PACKAGE_ROWS]

    def activate(
        self, flow: str, name: str, version: str, by: str, at: datetime, reason: str | None = None
    ) -> FlowActivation:
        wanted = (flow, name, version_key(version))
        with self._d.lock:
            row = self._d.activations.get(wanted)
            if row is None:
                raise KeyError(f"{name} {version} has no flow {flow}")
            installed = self._d.package_versions.get((name, wanted[2]))
            if installed is None or installed.state != "ready":
                state = "uninstalled" if installed is None else installed.state
                raise VersionNotReady(
                    f"{name} {version} is {state}: only a ready version can be activated", state
                )
            for key, other in self._d.activations.items():
                if key[0] == flow and key != wanted and other.state == "activating":
                    self._d.activations[key] = replace(
                        other, state="inactive", changed_by=by, changed_at=at, reason=reason
                    )
            state = "active" if row.state == "active" else "activating"
            done = replace(row, state=state, changed_by=by, changed_at=at, reason=reason, error=None)
            self._d.activations[wanted] = done
            listeners = list(self._d.catalog_listeners)
        for listener in listeners:
            listener()
        return done

    def _settle(
        self, name: str, version: str, to: ActivationState, at: datetime, error: str | None
    ) -> list[FlowActivation]:
        key = version_key(version)
        done: list[FlowActivation] = []
        with self._d.lock:
            for (flow, package, vkey), row in list(self._d.activations.items()):
                if package != name or vkey != key or row.state != "activating":
                    continue
                if to == "active":
                    for other_key, other in list(self._d.activations.items()):
                        if (
                            other_key[0] == flow
                            and other_key != (flow, package, vkey)
                            and other.state == "active"
                        ):
                            self._d.activations[other_key] = replace(
                                other, state="inactive", changed_at=at, last_active_at=at
                            )
                served = at if to == "active" else row.last_active_at
                settled = replace(row, state=to, changed_at=at, error=error, last_active_at=served)
                self._d.activations[(flow, package, vkey)] = settled
                done.append(settled)
            listeners = list(self._d.catalog_listeners) if done else []
        for listener in listeners:
            listener()
        return sorted(done, key=lambda a: a.flow)

    def promote(self, name: str, version: str, at: datetime) -> list[FlowActivation]:
        return self._settle(name, version, "active", at, None)

    def activation_failed(self, name: str, version: str, error: str, at: datetime) -> list[FlowActivation]:
        return self._settle(name, version, "failed", at, error)

    def activating(self) -> list[FlowActivation]:
        with self._d.lock:
            found = [a for a in self._d.activations.values() if a.state == "activating"]
        return sorted(found, key=lambda a: a.flow)[:MAX_PACKAGE_ROWS]

    def deactivate(self, flow: str, by: str, at: datetime, reason: str | None = None) -> FlowActivation:
        with self._d.lock:
            found = [
                (k, a)
                for k, a in self._d.activations.items()
                if k[0] == flow and a.state in ("active", "activating")
            ]
            if not found:
                raise KeyError(f"no version of {flow} is active")
            found.sort(key=lambda item: item[1].state != "active")  # the active one first
            done: FlowActivation | None = None
            for key, row in found:
                served = at if row.state == "active" else row.last_active_at
                settled = replace(
                    row, state="inactive", changed_by=by, changed_at=at, reason=reason, last_active_at=served
                )
                self._d.activations[key] = settled
                done = done or settled
            listeners = list(self._d.catalog_listeners)
        for listener in listeners:
            listener()
        assert done is not None
        return done

    def active(self) -> list[FlowActivation]:
        with self._d.lock:
            found = [a for a in self._d.activations.values() if a.state == "active"]
        return sorted(found, key=lambda a: a.flow)[:MAX_PACKAGE_ROWS]

    def listen(self, callback: Callable[[], None]) -> Callable[[], None]:
        with self._d.lock:
            self._d.catalog_listeners.append(callback)

        def stop() -> None:
            with self._d.lock:
                if callback in self._d.catalog_listeners:
                    self._d.catalog_listeners.remove(callback)

        return stop

    def prepared(
        self,
        name: str,
        version: str,
        state: PackageState,
        load_check: Mapping[str, Any],
        expect: Sequence[PackageState] = ("installed", "preparing"),
    ) -> bool:
        key = (name, version_key(version))
        with self._d.lock:
            row = self._d.package_versions.get(key)
            if row is None or row.state not in expect:
                return False
            self._d.package_versions[key] = replace(row, state=state, load_check=dict(load_check))
            listeners = list(self._d.catalog_listeners)
        for listener in listeners:
            listener()
        return True

    def record_env(
        self, worker_id: str, lock_hash: str, state: EnvState, at: datetime, error: str | None = None
    ) -> None:
        with self._d.lock:
            self._d.package_envs[(worker_id, lock_hash)] = PackageEnv(worker_id, lock_hash, state, error, at)

    def envs(self, worker_id: str | None = None) -> list[PackageEnv]:
        with self._d.lock:
            found = [e for (w, _), e in self._d.package_envs.items() if worker_id in (None, w)]
        return sorted(found, key=lambda e: (e.worker_id, e.lock_hash))[:MAX_PACKAGE_ROWS]

    def record_host(
        self,
        worker_id: str,
        name: str,
        version: str,
        state: HostState,
        restarts: int,
        at: datetime,
        error: str | None = None,
    ) -> None:
        with self._d.lock:
            self._d.package_hosts[(worker_id, name, version)] = PackageHost(
                worker_id, name, version, state, restarts, error, at
            )

    def hosts(self, worker_id: str | None = None) -> list[PackageHost]:
        with self._d.lock:
            found = [h for (w, _, _), h in self._d.package_hosts.items() if worker_id in (None, w)]
        return sorted(found, key=lambda h: (h.worker_id, h.package, h.version))[:MAX_PACKAGE_ROWS]


class _EnvConfig(EnvConfigStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def entries(self, env: str, kind: EnvKind | None = None) -> list[EnvEntry]:
        with self._d.lock:
            found = [e for (en, k, _), e in self._d.env_config.items() if en == env and kind in (None, k)]
        found.sort(key=lambda e: (ENV_KINDS.index(e.kind), e.name))
        return found[:MAX_ENV_ROWS]

    def entry(self, env: str, kind: EnvKind, name: str) -> EnvEntry | None:
        with self._d.lock:
            return self._d.env_config.get((env, kind, name))

    def put(self, entry: EnvEntry) -> EnvEntry | None:
        with self._d.lock:
            old = self._d.env_config.get((entry.env, entry.kind, entry.name))
            self._d.env_config[(entry.env, entry.kind, entry.name)] = replace(
                entry, value=copy.deepcopy(entry.value)
            )
            return old

    def remove(self, env: str, kind: EnvKind, name: str) -> EnvEntry | None:
        with self._d.lock:
            return self._d.env_config.pop((env, kind, name), None)


class _Secrets(SecretStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    @property
    def tenant(self) -> str:
        return "default"

    def put(self, secret: SealedSecret) -> bool:
        with self._d.lock:
            active = self._d.secret_keys.setdefault(secret.env, secret.key_id)
            if active != secret.key_id:
                raise KeyMismatch(active, secret.key_id)
            replaced = (secret.env, secret.name) in self._d.secrets
            self._d.secrets[(secret.env, secret.name)] = secret
            return replaced

    def get(self, env: str, name: str) -> SealedSecret | None:
        with self._d.lock:
            return self._d.secrets.get((env, name))

    def names(self, env: str) -> list[SealedSecret]:
        with self._d.lock:
            found = [s for (e, _), s in self._d.secrets.items() if e == env]
        return sorted(found, key=lambda s: s.name)[:MAX_ENV_ROWS]

    def remove(self, env: str, name: str) -> bool:
        with self._d.lock:
            return self._d.secrets.pop((env, name), None) is not None

    def active_key(self, env: str) -> str | None:
        with self._d.lock:
            return self._d.secret_keys.get(env)

    def rotate(
        self, env: str, expected: str | None, new_key: str, reseal: Callable[[SealedSecret], SealedSecret]
    ) -> int:
        with self._d.lock:
            active = self._d.secret_keys.get(env)
            if active != expected:
                raise KeyMismatch(active, expected)
            sealed = {k: reseal(s) for k, s in self._d.secrets.items() if k[0] == env}
            self._d.secrets.update(sealed)
            self._d.secret_keys[env] = new_key
            return len(sealed)


class _Webhooks(WebhookEventStore):
    def __init__(self, data: _Data, inbox: "_Inbox") -> None:
        self._d = data
        self._inbox = inbox

    def keep_many(self, events: Sequence[tuple[WebhookEvent, str, InboundRequest]]) -> list[bool]:
        kept: list[bool] = []
        with self._d.lock:
            for event, dataset, request in events:
                new = self.add(replace(event, processed_at=event.received_at))
                if new:
                    self._inbox.put(dataset, request, event.key)
                kept.append(new)
        return kept

    def add(self, event: WebhookEvent) -> bool:
        key = (event.package, event.webhook, event.key)
        with self._d.lock:
            if key in self._d.webhook_events:
                return False
            self._d.webhook_events[key] = event
            return True

    def events(self, package: str, webhook: str, limit: int = 100) -> list[WebhookEvent]:
        with self._d.lock:
            found = [e for (p, w, _), e in self._d.webhook_events.items() if (p, w) == (package, webhook)]
        return found[: max(0, min(limit, MAX_ENV_ROWS))]

    def processed(self, package: str, webhook: str, keys: Sequence[str], at: datetime) -> int:
        marked = 0
        with self._d.lock:
            for key in keys:
                event = self._d.webhook_events.get((package, webhook, key))
                if event is not None:
                    self._d.webhook_events[(package, webhook, key)] = replace(event, processed_at=at)
                    marked += 1
        return marked

    def purge(self, before: datetime) -> int:
        with self._d.lock:
            old = [
                k
                for k, e in self._d.webhook_events.items()
                if e.processed_at is not None and e.received_at < before
            ]
            for key in old:
                del self._d.webhook_events[key]
        return len(old)


class _Audit(AuditStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def record(
        self,
        actor_type: str,
        actor: str,
        action: str,
        target: str | None,
        request: Mapping[str, Any],
        result: str,
    ) -> str:
        with self._d.lock:
            audit_id = new_id()
            self._d.audit.append(
                AuditRow(
                    audit_id,
                    self._d.clock.now(),
                    actor_type,
                    actor,
                    action,
                    target,
                    copy.deepcopy(dict(request)),
                    result,
                )
            )
            return audit_id

    def list_audit(self, limit: int = 100, cursor: str | None = None) -> list[AuditRow]:
        with self._d.lock:
            rows = sorted(self._d.audit, key=lambda r: r.audit_id, reverse=True)
        if cursor is not None:
            rows = [r for r in rows if r.audit_id < cursor]
        return rows[:limit]

    def for_target(self, target: str, since: datetime | None = None, limit: int = 500) -> list[AuditRow]:
        with self._d.lock:
            rows = [r for r in self._d.audit if r.target == target and (since is None or r.at >= since)]
        return sorted(rows, key=lambda r: (r.at, r.audit_id))[:limit]


class _Reads(ReadStore):
    def __init__(self, data: _Data) -> None:
        self._d = data

    def partition_grid(
        self, dataset: str, *, since: datetime | None = None, cursor: str | None = None, limit: int = 100
    ) -> list[PartitionRow]:
        with self._d.lock:
            latest: dict[str, Version] = {}
            for version in self._d.versions.values():
                if version.dataset != dataset or version.status != "sealed" or version.seal_seq is None:
                    continue
                held = latest.get(version.partition)
                if held is None or (held.seal_seq or 0) < version.seal_seq:
                    latest[version.partition] = version
            rows: list[PartitionRow] = []
            for partition in sorted(latest, reverse=True):
                if len(rows) >= limit:  # the cells are the costly part: build them for the page only
                    break
                version = latest[partition]
                assert version.seal_seq is not None and version.sealed_at is not None
                if (cursor is not None and partition >= cursor) or (since and version.sealed_at < since):
                    continue
                cells = {
                    route: d.status
                    for (route, v), d in sorted(self._d.deliveries.items())
                    if v == version.version_id
                }
                rows.append(
                    PartitionRow(
                        dataset,
                        partition,
                        version.version_id,
                        version.seal_seq,
                        version.sealed_at,
                        version.upserts,
                        version.deletes,
                        cells,
                    )
                )
            return rows

    def failure_groups(
        self, route: str | None = None, *, limit: int = 100, cursor: str | None = None
    ) -> list[FailureGroup]:
        with self._d.lock:
            found: dict[tuple[str, str, str], list[Delivery]] = {}
            for delivery in self._d.deliveries.values():
                if delivery.status != "failed" or (route is not None and delivery.route != route):
                    continue
                error_class, message = split_error(delivery.last_error)
                found.setdefault((delivery.route, error_class, message), []).append(delivery)
        groups = [
            FailureGroup(
                r, c, m, len(ds), tuple(d.version_id for d in sorted(ds, key=lambda d: d.seal_seq)[:5])
            )
            for (r, c, m), ds in sorted(found.items())
        ]
        if cursor is not None:
            after = tuple(json.loads(cursor))
            groups = [g for g in groups if (g.route, g.error_class, g.message) > after]
        return groups[:limit]

    def delivery_counts(self) -> dict[str, int]:
        with self._d.lock:
            counts: dict[str, int] = {}
            for delivery in self._d.deliveries.values():
                counts[delivery.status] = counts.get(delivery.status, 0) + 1
            return counts

    def retried_tasks(self, run_ids: Sequence[str]) -> dict[str, int]:
        with self._d.lock:
            counts: dict[str, int] = {}
            for task in self._d.tasks.values():
                if task.run_id in run_ids and task.attempt > 1:
                    counts[task.run_id] = counts.get(task.run_id, 0) + 1
            return counts

    def waiting(self, route: str, partitions: Sequence[str]) -> dict[tuple[str, str], int]:
        with self._d.lock:
            found: dict[tuple[str, str], int] = {}
            for (name, _), d in self._d.deliveries.items():
                if name == route and d.partition in partitions and d.status in ("pending", "held"):
                    found[(d.dataset, d.partition)] = found.get((d.dataset, d.partition), 0) + 1
            return found


class _Readiness(ReadinessStore):
    def __init__(self, data: _Data, runs: RunJournal, inbox: "_Inbox") -> None:
        self._d = data
        self._runs = runs
        self._inbox = inbox
        self._closes: dict[tuple[str, str], PartitionClose] = {}
        self._firings: dict[tuple[str, str, str, str, int], Firing] = {}

    def listen(self, callback: Callable[[], None]) -> Callable[[], None]:
        return lambda: None  # nothing to wake: in one process the evaluator's poll is as quick

    def close(self, dataset: str, partition: str, actor: str, reason: str, at: datetime) -> PartitionClose:
        with self._d.lock:
            return self._closes.setdefault(
                (dataset, partition), PartitionClose(dataset, partition, at, actor, reason)
            )

    def closes(self, dataset: str, partitions: Sequence[str]) -> dict[str, PartitionClose]:
        with self._d.lock:
            return {p: c for p in partitions if (c := self._closes.get((dataset, p))) is not None}

    def fire(self, firing: Firing, params: Mapping[str, Any]) -> str | None:
        key = (firing.flow, firing.ready_set, firing.period, firing.scope, firing.seq)
        with self._d.lock:
            if key in self._firings:
                return None
            run_id = self._runs.create_run(firing.flow, "ready", firing.actor, params)
            self._firings[key] = replace(firing, run_id=run_id)
            return run_id

    def firings(self, flow: str, ready_set: str | None = None, limit: int = 200) -> list[Firing]:
        with self._d.lock:
            found = [
                f
                for f in self._firings.values()
                if f.flow == flow and (ready_set is None or f.ready_set == ready_set)
            ]
        found.sort(key=lambda f: (f.fired_at, f.seq), reverse=True)
        return found[: max(0, min(limit, 1000))]

    def _sealed(self, v: Version) -> SealedPartition:
        run = self._d.runs[v.run_id]
        assert v.seal_seq is not None and v.sealed_at is not None
        return SealedPartition(
            v.partition, v.version_id, v.seal_seq, v.run_id, run.status, run.created_at, v.sealed_at
        )

    def _sealed_of(self, dataset: str) -> list[Version]:
        found = [v for v in self._d.versions.values() if v.dataset == dataset and v.status == "sealed"]
        return sorted(found, key=lambda v: v.seal_seq or 0)

    def latest_sealed(self, dataset: str, since: datetime) -> dict[str, SealedPartition]:
        with self._d.lock:
            versions = self._sealed_of(dataset)
            recent = {v.partition for v in versions if v.sealed_at is not None and v.sealed_at >= since}
            return {v.partition: self._sealed(v) for v in versions if v.partition in recent}  # the last wins

    def sealed_since(self, dataset: str, since: datetime) -> list[SealedPartition]:
        with self._d.lock:
            return [
                self._sealed(v)
                for v in self._sealed_of(dataset)
                if v.sealed_at is not None and v.sealed_at >= since
            ]

    def open_deliveries(self, route: str, dataset: str) -> dict[str, int]:
        found: dict[str, int] = {}
        with self._d.lock:
            for d in self._d.deliveries.values():
                if d.route == route and d.dataset == dataset and d.status not in CLOSED:
                    found[d.partition] = min(found.get(d.partition, d.seal_seq), d.seal_seq)
        return found

    def oldest_unsealed(self, dataset: str) -> datetime | None:
        return self._inbox.oldest_unsealed(dataset)


@dataclass
class _InboxRow:
    event: InboxEvent
    day: date
    sealed_at: datetime | None = None
    held: bool = False


class _Inbox(InboxStore):
    def __init__(self, data: _Data, versions: _Versions) -> None:
        self._d = data
        self._versions = versions
        self._rows: dict[int, _InboxRow] = {}
        self._seen: dict[tuple[str, str], date] = {}
        self._seq = 0

    def oldest_unsealed(self, dataset: str) -> datetime | None:
        with self._d.lock:
            found = [
                r.event.received_at
                for r in self._rows.values()
                if r.event.dataset == dataset and r.sealed_at is None and not r.held
            ]
        return min(found, default=None)

    def ensure_partitions(self, days_ahead: int = 7) -> None:
        """Memory needs no partitions."""

    def put(self, dataset: str, request: InboundRequest, event_id: str | None = None) -> bool:
        with self._d.lock:
            day = self._d.clock.now().date()
            if event_id is not None:
                if (dataset, event_id) in self._seen:
                    return False
                self._seen[(dataset, event_id)] = day
            self._seq += 1
            event = InboxEvent(self._seq, dataset, event_id, request, request.received_at)
            self._rows[self._seq] = _InboxRow(event, day)
            return True

    def put_many(self, events: Sequence[tuple[str, InboundRequest, str | None]]) -> list[bool]:
        return [self.put(dataset, request, event_id) for dataset, request, event_id in events]

    def _open(self, dataset: str) -> list[InboxEvent]:
        rows = (
            r
            for r in self._rows.values()
            if r.sealed_at is None and not r.held and r.event.dataset == dataset
        )
        return sorted((r.event for r in rows), key=lambda event: event.event_seq)

    def defer(self, event_seq: int, until: datetime) -> None:
        with self._d.lock:
            row = self._rows.get(event_seq)
            if row is not None and row.sealed_at is None and not row.held:
                row.event = replace(row.event, tries=row.event.tries + 1, retry_at=until)

    def hold(self, event_seq: int) -> None:
        with self._d.lock:
            row = self._rows.get(event_seq)
            if row is None or row.sealed_at is not None or row.held:
                raise StaleWriter("another sealer has dealt with this event")
            row.held = True

    def _held(self, dataset: str, event_seq: int) -> _InboxRow | None:
        row = self._rows.get(event_seq)
        return row if row is not None and row.held and row.event.dataset == dataset else None

    def reprocess(self, dataset: str, event_seq: int) -> bool:
        with self._d.lock:
            row = self._held(dataset, event_seq)
            if row is None:
                return False
            row.held = False
            row.event = replace(row.event, tries=0, retry_at=None)
            return True

    def release(self, dataset: str, event_seq: int) -> bool:
        with self._d.lock:
            row = self._held(dataset, event_seq)
            if row is None:
                return False
            row.held = False
            row.sealed_at = self._d.clock.now()
            return True

    def take(self, dataset: str, limit: int, max_bytes: int | None = None) -> list[InboxEvent]:
        with self._d.lock:
            found = self._open(dataset)[:limit]
        if max_bytes is None:
            return found
        kept: list[InboxEvent] = []
        size = 0
        for event in found:
            if size >= max_bytes:
                break
            kept.append(event)
            size += len(event.request.body)
        return kept

    def oldest_age(self, dataset: str, now: datetime) -> timedelta | None:
        with self._d.lock:
            found = self._open(dataset)
        return now - found[0].received_at if found else None

    def seal_batch(
        self,
        event_seqs: Sequence[int],
        drafts: Sequence[VersionDraft],
        routes: Sequence[str],
        writer: str | None = None,
    ) -> list[Version]:
        with (
            self._d.lock
        ):  # every check comes before the first change, which is what one transaction amounts to here
            for seq in event_seqs:
                row = self._rows.get(seq)
                if row is None or row.sealed_at is not None or row.held:
                    raise StaleWriter("another sealer has dealt with an event of this batch")
            for draft in drafts:
                self._versions.check_writable(draft.version_id, writer)
            for seq in event_seqs:
                self._rows[seq].sealed_at = self._d.clock.now()
            for draft in drafts:
                self._versions.stage(draft, writer)
            return self._versions.seal([draft.version_id for draft in drafts], routes, writer)

    def days(self) -> list[date]:
        with self._d.lock:
            return sorted({row.day for row in self._rows.values()})

    def day_status(self, day: date) -> InboxDay:
        with self._d.lock:
            rows = [row for row in self._rows.values() if row.day == day]
        waiting = [row.event.event_seq for row in rows if row.sealed_at is None]
        sealed = [row.sealed_at for row in rows if row.sealed_at is not None]
        return InboxDay(len(waiting), min(waiting, default=None), max(sealed, default=None))

    def drop_day(self, day: date) -> bool:
        with self._d.lock:
            rows = {seq: row for seq, row in self._rows.items() if row.day == day}
            if any(row.sealed_at is None for row in rows.values()):
                return False
            for seq in rows:
                del self._rows[seq]
            self._seen = {key: seen for key, seen in self._seen.items() if seen != day}
            return True


class MemoryStore(Store):
    """A `Store` that keeps everything in memory. `clock` defaults to the system clock."""

    def __init__(self, clock: Clock | None = None) -> None:
        data = _Data(clock or SystemClock())
        versions = _Versions(data)
        inbox = _Inbox(data, versions)
        runs = _Runs(data)
        super().__init__(
            runs=runs,
            versions=versions,
            ledger=_Ledger(data),
            state=_State(data),
            quarantine=_Quarantine(data),
            budgets=_Budgets(data),
            catalog=_Catalog(data),
            workers=_Workers(data),
            schedules=_Schedules(data),
            overrides=_Overrides(data),
            users=_Users(data),
            audit=_Audit(data),
            reads=_Reads(data),
            inbox=inbox,
            responses=_Responses(data),
            registry=_Registry(data),
            packages=_Packages(data),
            env_config=_EnvConfig(data),
            secrets=_Secrets(data),
            webhooks=_Webhooks(data, inbox),
            readiness=_Readiness(data, runs, inbox),
        )

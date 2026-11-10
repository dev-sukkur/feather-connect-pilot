"""The trace of one record: what happened to it, by its dataset key, as one timeline, oldest first.

Each sealed version that held the key comes from the key index that sealing writes (ruling 11), so finding them reads
no spool chunk; the record as each version read it is then read from the one chunk the index names. For every route,
each delivery of those versions follows with its attempts, what the destination answered for the record, its quarantine
entries, and the retries, skips and replays the operators made, matched to their audit rows.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc

from feather_connect.storage.port import (
    Attempt,
    AuditRow,
    Delivery,
    KeySighting,
    QuarantineRow,
    ResponseRow,
)
from feather_connect.storage.spool import table_to_records
from feather_server.catalog_view import dataset_of

if TYPE_CHECKING:
    from feather_server.app import AppState

VERSIONS_TRACED = 100  # the newest of a key's versions that a trace shows
_ACTIONS = {("failed", "pending"): "retry", ("applied", "pending"): "replay"}


@dataclass(frozen=True)
class Change:
    field: str
    before: Any
    after: Any


@dataclass(frozen=True)
class Action:
    """A retry, skip or replay of a delivery, or a dismissal of its quarantine entry, with its audit row if found."""

    action: str
    actor: str
    reason: str | None
    at: datetime
    audit_id: str | None
    result: str | None


@dataclass(frozen=True)
class TracedDelivery:
    delivery: Delivery
    attempts: tuple[Attempt, ...]
    response: ResponseRow | None
    quarantine: tuple[QuarantineRow, ...]
    actions: tuple[Action, ...]
    log_task: str | None = None  # the latest task that applied it, in the run that read the version


@dataclass(frozen=True)
class TracedVersion:
    """One version that held the record. `op` is insert, update, unchanged or delete, said against the version before
    it in the trace; `record` is None when retention has taken its chunk."""

    sighting: KeySighting
    op: str
    record: Mapping[str, Any] | None
    changes: tuple[Change, ...]
    deliveries: tuple[TracedDelivery, ...]
    log_task: str | None  # the task that read it, whose log the page links to


@dataclass(frozen=True)
class Trace:
    dataset: str
    key: str
    versions: tuple[TracedVersion, ...]
    extraction: tuple[QuarantineRow, ...]  # entries extraction made for the key, outside any version
    truncated: bool  # the key has more versions than the trace shows: the oldest are left out


def _op(sighting: KeySighting, before: KeySighting | None) -> str:
    if sighting.op == "delete":
        return "delete"
    if before is None or before.op == "delete":
        return "insert"
    return "unchanged" if before.row_hash == sighting.row_hash else "update"


def _changes(before: Mapping[str, Any] | None, after: Mapping[str, Any] | None) -> tuple[Change, ...]:
    if before is None or after is None:
        return ()
    names = list(dict.fromkeys([*before, *after]))
    return tuple(Change(n, before.get(n), after.get(n)) for n in names if before.get(n) != after.get(n))


def _action_of(attempt: Attempt) -> str | None:
    if attempt.to_status == "skipped":
        return "skip"
    return _ACTIONS.get((attempt.from_status, attempt.to_status))


def _audit_for(action: str, attempt: Attempt, rows: Sequence[AuditRow]) -> AuditRow | None:
    """The audit row an operator's move of a delivery came with: of that action, by that actor with that reason, and
    the latest of them written before the move."""
    found = [
        r
        for r in rows
        if r.action == f"{action}_deliveries"
        and r.actor == attempt.actor
        and r.request.get("reason") == attempt.reason
        and r.at <= attempt.at
    ]
    return found[-1] if found else None


class Tracer:
    def __init__(self, state: "AppState") -> None:
        self._state = state
        self._store = state.store
        self._rt = state.runtime

    def datasets(self) -> list[str]:
        return sorted({d.name for f in self._rt.flows() for d in f.datasets})

    def trace(self, dataset: str, key: str) -> Trace:
        """Raises `KeyError` for a dataset the project doesn't have."""
        try:
            found = self._rt.dataset(dataset)
            routes = [r.name for f in self._rt.flows() if found in f.datasets for r in f.routes]
        except KeyError:  # a packaged dataset: its routes from the catalogue
            routes = list(dataset_of(self._state, dataset).routes)
        sightings = self._store.versions.key_history(dataset, key, VERSIONS_TRACED)
        version_ids = [s.version_id for s in sightings]
        entries = self._store.quarantine.of_key(dataset, key, version_ids)
        since = sightings[0].sealed_at if sightings else None
        audits = {route: self._store.audit.for_target(f"route:{route}", since) for route in routes}
        everywhere = self._store.audit.for_target("deliveries", since)
        tasks: dict[str, dict[tuple[str | None, str | None], str]] = {}
        shown: list[TracedVersion] = []
        before: KeySighting | None = None
        record_before: Mapping[str, Any] | None = None
        for sighting in sightings:
            record = self._record(sighting)
            deliveries = tuple(
                d
                for route in routes
                if (d := self._delivery(route, sighting, key, entries, [*audits[route], *everywhere]))
            )
            shown.append(
                TracedVersion(
                    sighting,
                    _op(sighting, before),
                    record,
                    _changes(record_before, record) if before is not None and sighting.op != "delete" else (),
                    deliveries,
                    self._log_task(tasks, sighting),
                )
            )
            before, record_before = sighting, record
        extraction = tuple(e for e in entries if e.route is None)
        return Trace(dataset, key, tuple(shown), extraction, len(sightings) >= VERSIONS_TRACED)

    def _record(self, sighting: KeySighting) -> Mapping[str, Any] | None:
        try:
            table = self._state.spool.get_chunk(sighting.version_id, sighting.chunk_no)
        except KeyError:  # retention took it
            return None
        wanted = pa.scalar(sighting.key, pa.string())
        mine = table.filter(pc.equal(table.column("_fc_key"), wanted))
        records = table_to_records(mine)
        return dict(records[0].payload) if records else None

    def _delivery(
        self,
        route: str,
        sighting: KeySighting,
        key: str,
        entries: Sequence[QuarantineRow],
        audits: Sequence[AuditRow],
    ) -> TracedDelivery | None:
        try:
            delivery = self._store.ledger.get(route, sighting.version_id)
        except KeyError:
            return None
        attempts = tuple(self._store.ledger.attempts(route, sighting.version_id))
        actions: list[Action] = []
        for attempt in attempts:
            action = _action_of(attempt)
            if action is None:
                continue
            row = _audit_for(action, attempt, audits)
            actions.append(
                Action(
                    action,
                    attempt.actor,
                    attempt.reason,
                    attempt.at,
                    None if row is None else row.audit_id,
                    None if row is None else row.result,
                )
            )
        for row in audits:
            if (
                row.action == "dismiss_quarantine"
                and row.request.get("key") == key
                and row.request.get("version_id") == sighting.version_id
            ):
                actions.append(
                    Action("dismiss", row.actor, row.request.get("reason"), row.at, row.audit_id, row.result)
                )
        return TracedDelivery(
            delivery,
            attempts,
            self._store.responses.find(route, sighting.version_id, key),
            tuple(e for e in entries if e.route == route and e.version_id == sighting.version_id),
            tuple(sorted(actions, key=lambda a: a.at)),
            self._apply_task(route, sighting),
        )

    def _apply_task(self, route: str, sighting: KeySighting) -> str | None:
        found = self._store.runs.tasks_by_ref(sighting.run_id, f"apply:{route}:{sighting.version_id}:")
        return found[-1].task_id if found else None

    def _log_task(
        self, tasks: dict[str, dict[tuple[str | None, str | None], str]], sighting: KeySighting
    ) -> str | None:
        """The extract task of the version's run that read its partition, from one listing per run."""
        if sighting.run_id not in tasks:
            tasks[sighting.run_id] = {
                (t.dataset, t.partition): t.task_id
                for t in self._store.runs.list_tasks(sighting.run_id, kinds=("extract",))
            }
        found = tasks[sighting.run_id]
        # A read of the whole dataset, or one that finds its partitions as it goes, is one task for `*`.
        return found.get((sighting.dataset, sighting.partition)) or found.get((sighting.dataset, "*"))


def _attempt(a: Attempt) -> dict[str, Any]:
    return {
        "from": a.from_status,
        "to": a.to_status,
        "actor": a.actor,
        "reason": a.reason,
        "error": a.error,
        "rows_applied": a.rows_applied,
        "rows_rejected": a.rows_rejected,
        "at": a.at,
    }


def _entry(q: QuarantineRow) -> dict[str, Any]:
    return {
        "route": q.route,
        "version_id": q.version_id,
        "error": q.error,
        "status": q.status,
        "payload": q.payload,
        "raw": q.raw,
    }


def as_json(trace: Trace) -> dict[str, Any]:
    """The trace as the API gives it."""
    return {
        "dataset": trace.dataset,
        "key": trace.key,
        "indexed_only": True,  # versions sealed before the key index came in are not traced
        "truncated": trace.truncated,
        "versions": [
            {
                "version_id": v.sighting.version_id,
                "seal_seq": v.sighting.seal_seq,
                "partition": v.sighting.partition,
                "run_id": v.sighting.run_id,
                "sealed_at": v.sighting.sealed_at,
                "op": v.op,
                "record": v.record,
                "changes": [{"field": c.field, "before": c.before, "after": c.after} for c in v.changes],
                "log_task_id": v.log_task,
                "deliveries": [
                    {
                        "route": d.delivery.route,
                        "status": d.delivery.status,
                        "last_error": d.delivery.last_error,
                        "log_task_id": d.log_task,
                        "attempts": [_attempt(a) for a in d.attempts],
                        "response": None
                        if d.response is None
                        else {
                            "status": d.response.status,
                            "fields": dict(d.response.fields),
                            "sent": d.response.sent,
                        },
                        "quarantine": [_entry(q) for q in d.quarantine],
                        "actions": [
                            {
                                "action": a.action,
                                "actor": a.actor,
                                "reason": a.reason,
                                "at": a.at,
                                "audit_id": a.audit_id,
                                "result": a.result,
                            }
                            for a in d.actions
                        ],
                    }
                    for d in v.deliveries
                ],
            }
            for v in trace.versions
        ],
        "extraction_quarantine": [_entry(q) for q in trace.extraction],
    }

"""The records of one task of a run, for the Records panel of its timeline row.

An extract task shows the records of the versions it sealed, with the entries extraction turned down; an apply task
shows the records of its version, each with what its route made of it. Records come from the key index (ruling 11) a
page at a time, keyset by key, so a version of 100,000 records costs a page; each page's payloads are read from only
the spool chunks that hold them.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc

from feather_connect.runtime.extract import version_id_of
from feather_connect.storage.port import Delivery, KeySighting, QuarantineRow, ResponseRow, TaskRow, Version
from feather_connect.storage.spool import table_to_records

if TYPE_CHECKING:
    from feather_server.app import AppState

PAGE = 50
_QUARANTINE_SHOWN = 100


class NoRecords(LookupError):
    """The task is of a kind that holds no records, or its version can't be told from it."""


@dataclass(frozen=True)
class TaskRecord:
    """One record of the panel. `op` is insert, update, unchanged or delete, against the version that held the key
    before; `payload` is None for a delete or when retention took its chunk (`in_spool` False)."""

    sighting: KeySighting
    op: str
    payload: Mapping[str, Any] | None
    in_spool: bool
    result: str | None = None  # apply: what the route made of it
    response: ResponseRow | None = None
    error: str | None = None  # apply: why the route turned it down


@dataclass(frozen=True)
class TaskRecords:
    task: TaskRow
    dataset: str
    route: str | None
    versions: tuple[Version, ...]
    delivery: Delivery | None
    records: tuple[TaskRecord, ...]
    next_cursor: str | None  # the raw cursor of the next page, or None on the last
    query: str
    not_indexed: bool  # sealed before the key index: there are records, and none are indexed
    quarantine: tuple[QuarantineRow, ...]  # extract: the entries extraction made


def cursor_of(sighting: KeySighting) -> str:
    return json.dumps([sighting.key, sighting.version_id])


def after_of(cursor: str | None) -> tuple[str, str] | None:
    """Raises `ValueError` for a cursor not of this form."""
    if not cursor:
        return None
    found = json.loads(cursor)
    if not (isinstance(found, list) and len(found) == 2 and all(isinstance(part, str) for part in found)):
        raise ValueError("not a records cursor")
    return found[0], found[1]


def _op(sighting: KeySighting, before: KeySighting | None) -> str:
    if sighting.op == "delete":
        return "delete"
    if before is None or before.op == "delete":
        return "insert"
    return "unchanged" if before.row_hash == sighting.row_hash else "update"


def _tick(task: TaskRow, kind: str) -> tuple[str | None, str]:
    """The tick a task's ref names, if any, and the rest of the ref."""
    prefix = f"tick:{task.run_id}:"
    if not task.ref.startswith(prefix):
        return None, task.ref
    tick, _, rest = task.ref.removeprefix(prefix).partition(f":{kind}:")
    return tick, f"{kind}:{rest}"


class TaskRecordReader:
    def __init__(self, state: "AppState") -> None:
        self._state = state
        self._store = state.store

    def task(self, run_id: str, task_id: str) -> TaskRow:
        """Raises `KeyError` for a task that isn't one of the run's."""
        task = self._store.runs.get_task(task_id)
        if task.run_id != run_id:
            raise KeyError(task_id)
        return task

    def read(
        self, run_id: str, task_id: str, cursor: str | None = None, query: str = "", limit: int | None = None
    ) -> TaskRecords:
        """Raises `KeyError` for an unknown task or one of another run, `NoRecords` for a task that holds none, and
        `ValueError` for a cursor not handed out here."""
        task = self.task(run_id, task_id)
        after = after_of(cursor)
        limit = limit or PAGE
        if task.kind == "extract" and task.dataset:
            return self._extract(task, after, query.strip(), limit)
        if task.kind == "apply" and task.dataset and task.route:
            return self._apply(task, after, query.strip(), limit)
        raise NoRecords(f"a {task.kind} task holds no records")

    # The two kinds.

    def _extract(self, task: TaskRow, after: tuple[str, str] | None, query: str, limit: int) -> TaskRecords:
        dataset = task.dataset
        assert dataset is not None
        tick, _ = _tick(task, "extract")
        if task.partition and task.partition != "*":
            wanted = [version_id_of(task.run_id, dataset, task.partition, tick)]
            versions = [v for v in self._known(wanted) if v.status == "sealed"]
        else:  # a read that found its partitions as it went: every version of the run it sealed
            versions = self._store.versions.sealed_for_run(task.run_id, dataset, tick)
            wanted = [v.version_id for v in versions]
        records, next_cursor = self._page(dataset, versions, after, query, limit)
        return TaskRecords(
            task,
            dataset,
            None,
            tuple(versions),
            None,
            tuple(records),
            next_cursor,
            query,
            self._not_indexed(dataset, versions, records),
            tuple(self._store.quarantine.of_versions(wanted, None, limit=_QUARANTINE_SHOWN)),
        )

    def _apply(self, task: TaskRow, after: tuple[str, str] | None, query: str, limit: int) -> TaskRecords:
        dataset, route = task.dataset, task.route
        assert dataset is not None and route is not None
        _, ref = _tick(task, "apply")
        head = f"apply:{route}:"
        if not ref.startswith(head):
            raise NoRecords(f"the version of task {task.task_id} can't be told from its ref")
        version_id = ref.removeprefix(head).split(":", 1)[0]
        versions = self._known([version_id])
        try:
            delivery: Delivery | None = self._store.ledger.get(route, version_id)
        except KeyError:
            delivery = None
        page, next_cursor = self._page(dataset, versions, after, query, limit)
        keys = [r.sighting.key for r in page]
        answers = self._store.responses.for_keys(route, version_id, keys)
        turned_down = {q.key: q for q in self._store.quarantine.of_versions([version_id], route, keys)}
        records: list[TaskRecord] = []
        for record in page:
            key = record.sighting.key
            entry = turned_down.get(key)
            if entry is not None:
                result = "rejected"
            elif delivery is None:
                result = None
            else:
                result = delivery.status
            records.append(
                TaskRecord(
                    record.sighting,
                    record.op,
                    record.payload,
                    record.in_spool,
                    result,
                    answers.get(key),
                    None if entry is None else entry.error,
                )
            )
        return TaskRecords(
            task,
            dataset,
            route,
            tuple(versions),
            delivery,
            tuple(records),
            next_cursor,
            query,
            self._not_indexed(dataset, versions, records),
            (),
        )

    # Shared.

    def _known(self, version_ids: Sequence[str]) -> list[Version]:
        found: list[Version] = []
        for version_id in version_ids:
            try:
                found.append(self._store.versions.get(version_id))
            except KeyError:
                pass
        return found

    def _page(
        self, dataset: str, versions: Sequence[Version], after: tuple[str, str] | None, query: str, limit: int
    ) -> tuple[list[TaskRecord], str | None]:
        ids = [v.version_id for v in versions]
        found = self._store.versions.keys_page(
            dataset, ids, after=after, prefix=query or None, limit=limit + 1
        )
        shown, more = found[:limit], len(found) > limit
        seal_seqs = {v.version_id: v.seal_seq for v in versions}
        before: dict[tuple[str, str], KeySighting] = {}
        for version_id in {s.version_id for s in shown}:
            keys = [s.key for s in shown if s.version_id == version_id]
            seal_seq = seal_seqs.get(version_id)
            if seal_seq is not None:
                for key, sighting in self._store.versions.earlier_sightings(dataset, keys, seal_seq).items():
                    before[(version_id, key)] = sighting
        payloads, kept = self._payloads(shown)
        records = [
            TaskRecord(
                s,
                _op(s, before.get((s.version_id, s.key))),
                payloads.get((s.version_id, s.key)),
                (s.version_id, s.chunk_no) in kept,
            )
            for s in shown
        ]
        return records, cursor_of(shown[-1]) if more and shown else None

    def _payloads(
        self, shown: Sequence[KeySighting]
    ) -> tuple[dict[tuple[str, str], Mapping[str, Any]], set[tuple[str, int]]]:
        """The payloads of a page, reading each chunk that holds one of them once and no other chunk."""
        payloads: dict[tuple[str, str], Mapping[str, Any]] = {}
        kept: set[tuple[str, int]] = set()
        for version_id, chunk_no in sorted({(s.version_id, s.chunk_no) for s in shown}):
            try:
                table = self._state.spool.get_chunk(version_id, chunk_no)
            except KeyError:  # retention took it
                continue
            kept.add((version_id, chunk_no))
            keys = pa.array(
                [s.key for s in shown if (s.version_id, s.chunk_no) == (version_id, chunk_no)], pa.string()
            )
            for record in table_to_records(table.filter(pc.is_in(table.column("_fc_key"), value_set=keys))):
                if record.op != "delete":
                    payloads[(version_id, record.key)] = dict(record.payload)
        return payloads, kept

    def _not_indexed(self, dataset: str, versions: Sequence[Version], shown: Sequence[TaskRecord]) -> bool:
        if shown or not any(v.upserts + v.deletes for v in versions):
            return False
        ids = [v.version_id for v in versions]
        return not self._store.versions.keys_page(dataset, ids, limit=1)


def as_json(found: TaskRecords) -> dict[str, Any]:
    """What the API gives, beside the page's `items` and `next_cursor`."""
    return {
        "task_id": found.task.task_id,
        "kind": found.task.kind,
        "dataset": found.dataset,
        "route": found.route,
        "version_ids": [v.version_id for v in found.versions],
        "delivery_status": None if found.delivery is None else found.delivery.status,
        "not_indexed": found.not_indexed,
        "quarantine": [
            {
                "version_id": q.version_id,
                "key": q.key,
                "error": q.error,
                "status": q.status,
                "partition": q.partition,
            }
            for q in found.quarantine
        ],
    }


def item_json(record: TaskRecord) -> dict[str, Any]:
    s = record.sighting
    return {
        "key": s.key,
        "version_id": s.version_id,
        "partition": s.partition,
        "op": record.op,
        "chunk_no": s.chunk_no,
        "in_spool": record.in_spool,
        "payload": record.payload,
        "result": record.result,
        "error": record.error,
        "response": None
        if record.response is None
        else {"status": record.response.status, "fields": dict(record.response.fields)},
    }

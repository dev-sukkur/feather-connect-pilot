"""Extraction: read a source, work out what changed, write the changes to the spool and produce versions.

Memory stays bounded to about one chunk of rows, plus a key and a hash per record. Every record read first goes
to `full` chunks. Once the read is over, the strategy compares the keys and hashes with what was known before,
and a second pass over the `full` chunks keeps the rows that changed.
"""

import hashlib
import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, cast, overload

import pyarrow as pa
from pydantic import BaseModel

from feather_connect.core.connector import ReadRequest, Source
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import MissingKeyError, PartitionError
from feather_connect.core.flow import RouteSpec
from feather_connect.core.records import Op, Record, canonical_json, make_key, row_hash
from feather_connect.core.strategies import ByStamp, read_stamp
from feather_connect.ids import new_id, stable_id
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.storage import spool as spool_settings
from feather_connect.storage.port import StateRow, Version, VersionDraft
from feather_connect.storage.spool import ChunkKind, records_to_table, snapshot_table, table_to_records

_KEY, _OP, _HASH, _MOVED = "_fc_key", "_fc_op", "_fc_row_hash", "_fc_moved_from"
_MOVES_SCHEMA = pa.schema([pa.field(_KEY, pa.string()), pa.field(_MOVED, pa.string())])
_REJECTED_SCHEMA = pa.schema([pa.field(_KEY, pa.string())])
_VERSION_KINDS: tuple[ChunkKind, ...] = ("data", "snapshot", "full", "candidates", "moves")
_ALL_PARTITIONS = "*"
_MISSING_KEY = "<missing>"  # how the quarantine names a record that has no key
_RESERVED_PREFIX = "_fc_"  # the spool's own columns
# The hash a snapshot holds for a key whose row was turned down before it ever reached the slice.
NOT_DELIVERED = 0

# What a bad record can raise. Validation and Arrow errors are `ValueError`s or `TypeError`s, or Arrow's own.
_RECORD_ERRORS = (MissingKeyError, PartitionError, pa.ArrowException, TypeError, ValueError)


class ExtractFailed(Exception):
    """A version can't be produced, so nothing of it is sealed. The message says why, and what to change."""


@dataclass
class _Slice:
    """The part of a read that goes into one version."""

    partition: str
    version_id: str
    chunks: int = 0  # how many `full` chunks are written
    watermark: Any = None
    buffer: dict[str, Record] = field(default_factory=dict[str, Record])  # last row wins per key


class _Read:
    """Takes the rows of a read and files them under their slices, as `full` chunks.

    `latest` holds the partition and hash of the last row read for each key, wherever it landed. `rejected` holds
    the keys of records that were read but turned down.
    """

    def __init__(
        self,
        dataset: Dataset,
        run_id: str,
        partition: str | None,
        stamp_kind: str | None,
        deps: RuntimeDeps,
        *,
        dry_run: bool = False,
    ) -> None:
        self._dry_run = dry_run  # a dry run turns no record down in the store: the quarantine is left alone
        self._dataset = dataset
        self._run_id = run_id
        self._partition = partition  # None when each row's partition comes from the row
        self._stamp_kind = stamp_kind  # None until the first acceptable stamp of a dataset's first read
        self._deps = deps
        self._buffered = 0
        self.slices: dict[str, _Slice] = {}
        self.latest: dict[str, tuple[str, int]] = {}
        self.rejected: set[str] = set()
        if partition is not None:
            self._slice(partition)

    def _version_id(self, partition: str) -> str:
        return stable_id(self._run_id, self._dataset.name, partition)

    def _slice(self, partition: str) -> _Slice:
        found = self.slices.get(partition)
        if found is None:
            found = _Slice(partition, self._version_id(partition))
            self._deps.spool.delete_version(found.version_id)  # what an interrupted attempt left behind
            self.slices[partition] = found
        return found

    def add(self, row: Mapping[str, Any]) -> None:
        dataset = self._dataset
        key: str | None = None
        partition = self._partition
        try:
            key = make_key(row, dataset.key)
            partition = dataset.partition.key_of(row) if partition is None else partition
            _validate(dataset.schema, row)
            _check_columns(row)
            record = Record(key, _op_of(dataset, row), row, row_hash(row))
            stamp = self._accepted_stamp(
                row
            )  # last, so that only a record that is kept can set the stamp kind
        except _RECORD_ERRORS as e:
            self._reject(key, partition, e)
            return
        target = self._slice(partition)
        self.latest[key] = (partition, record.row_hash)
        if stamp is not None and (target.watermark is None or stamp > target.watermark):
            target.watermark = stamp
        self._buffered += key not in target.buffer
        target.buffer[key] = record
        if self._buffered >= spool_settings.CHUNK_ROWS:
            self.flush()

    def _accepted_stamp(self, row: Mapping[str, Any]) -> Any:
        """Return the row's stamp as it compares, or None if it has none. Raises if it can't count to a watermark.

        Only stamps of the dataset's kind are comparable with its watermark, and a sealed watermark can't be taken
        back. So each stamp is checked, not just compared with the others of its slice.
        """
        strategy = self._dataset.strategy
        stamp = read_stamp(row.get(strategy.field)) if isinstance(strategy, ByStamp) else None
        if stamp is None:
            return None
        kind = _stamp_kind(stamp)
        if self._stamp_kind not in (None, kind):
            raise TypeError(
                f"change stamp {stamp!r} is a {kind}, but this dataset's are of kind {self._stamp_kind}"
            )
        self._stamp_kind = kind
        return stamp

    def _reject(self, key: str | None, partition: str | None, error: Exception) -> None:
        name = _MISSING_KEY if key is None else key
        if self._dataset.on_record_error == "fail":
            raise ExtractFailed(
                f"dataset {self._dataset.name!r}: record {name} is invalid and on_record_error is 'fail': {error}"
            ) from error
        if key is not None:
            self.rejected.add(key)
        if self._dry_run:
            return
        version_id = None if partition is None else self._version_id(partition)
        self._deps.store.quarantine.add(None, version_id, name, str(error), self._dataset.name, partition)

    def flush(self) -> None:
        """Write out what is buffered, so that all the slices together hold less than a chunk in memory."""
        for target in self.slices.values():
            if target.buffer:
                table = records_to_table(list(target.buffer.values()))
                self._deps.spool.put_chunk(target.version_id, target.chunks, "full", table)
                target.chunks += 1
                target.buffer = {}
        self._buffered = 0


def _validate(schema: type[BaseModel] | pa.Schema | None, row: Mapping[str, Any]) -> None:
    """Raise if `row` doesn't fit the dataset's declared schema. The row itself stays as it was read."""
    if isinstance(schema, pa.Schema):
        pa.Table.from_pylist([dict(row)], schema=schema)
        fields: Any = schema  # the stubs leave a schema's fields untyped
        for column in fields:
            if not column.nullable and row.get(column.name) is None:
                raise ValueError(f"column {column.name!r} must have a value")
    elif schema is not None:
        schema.model_validate(row)


def _check_columns(row: Mapping[str, Any]) -> None:
    for name in row:
        if name.startswith(_RESERVED_PREFIX):
            raise ValueError(f"column {name!r} uses the reserved prefix {_RESERVED_PREFIX!r}")


def _op_of(dataset: Dataset, row: Mapping[str, Any]) -> Op:
    strategy = dataset.strategy
    if isinstance(strategy, ByStamp) and strategy.deleted_field and row.get(strategy.deleted_field):
        return "delete"
    return "upsert"


def _stamp_kind(stamp: Any) -> str:
    """Return the kind of a change stamp. Stamps of one kind always compare. Raises `TypeError` for a bad stamp."""
    if isinstance(stamp, datetime):  # before date, because a datetime is also a date
        return "naive datetime" if stamp.utcoffset() is None else "aware datetime"
    if isinstance(stamp, date):
        return "date"
    if isinstance(stamp, int) and not isinstance(stamp, bool):
        return "number"
    if isinstance(stamp, float | Decimal) and math.isfinite(stamp):
        return "number"
    raise TypeError(f"change stamp {stamp!r} is not a date, a datetime, ISO-8601 text or a finite number")


def _rows_of(batch: Any) -> list[dict[str, Any]]:
    return batch.to_pylist() if isinstance(batch, pa.RecordBatch) else batch


def _read_since(dataset: Dataset, strategy: ByStamp, watermark: Any) -> Any:
    """Return where a `by_stamp` read starts: the watermark less the lookback. None means the first read."""
    if watermark is None or not strategy.lookback:
        return watermark
    try:
        return watermark - strategy.lookback
    except TypeError as e:
        raise ExtractFailed(
            f"dataset {dataset.name!r}: a lookback can't be taken from the watermark {watermark!r}, "
            f"so stamp field {strategy.field!r} needs dates, or the lookback must go"
        ) from e


def _read_source(dataset: Dataset, read: _Read, since: Any, deps: RuntimeDeps, partition: str) -> None:
    strategy = dataset.strategy
    stamp_field = strategy.field if isinstance(strategy, ByStamp) else None
    request = ReadRequest(partition, dataset.partition, since=since, stamp_field=stamp_field)
    # A push dataset's events come in through ingestion. Only pull sources are read.
    source = cast(Source, dataset.source)
    for batch in source.read(deps.make_context(source.connection), request):
        for row in _rows_of(batch):
            read.add(row)
    read.flush()


def _pairs(table: pa.Table) -> Iterator[tuple[str, int]]:
    keys = cast("list[str]", table.column(_KEY).to_pylist())
    hashes = cast("list[int]", table.column(_HASH).to_pylist())
    return zip(keys, hashes, strict=True)


def _recent_hashes(dataset: Dataset, partition: str, since: Any, deps: RuntimeDeps) -> pa.Table | None:
    """Return what the versions within the lookback say about each key. A later version wins."""
    if since is None:
        return None
    known: dict[str, int] = {}
    for version in deps.store.versions.since_watermark(dataset.name, partition, since):  # newest first
        for chunk in deps.spool.list_chunks(version.version_id):
            table = deps.spool.get_chunk(version.version_id, chunk.chunk_no)
            for key, hash_ in _pairs(table):
                known.setdefault(key, hash_)
    return snapshot_table(known.items())


def _last_snapshot(dataset: Dataset, partition: str, deps: RuntimeDeps) -> pa.Table | None:
    """Return the newest snapshot of the partition. A version without one must not hide an older one."""
    version = deps.store.versions.latest_sealed(dataset.name, partition)
    while version is not None:
        if deps.spool.list_chunks(version.version_id, "snapshot"):
            return deps.spool.get_chunk(version.version_id, 0, "snapshot")
        version = deps.store.versions.latest_sealed(dataset.name, partition, before_seal_seq=version.seal_seq)
    return None


def _old_snapshot(
    dataset: Dataset, partition: str, new: Mapping[str, int], since: Any, deps: RuntimeDeps
) -> pa.Table | None:
    """Return what was known before this read, in the shape of a snapshot, or None if nothing was."""
    if isinstance(dataset.strategy, ByStamp):
        if dataset.keep_state:
            rows = deps.store.state.get_many(dataset.name, new).values()
            return snapshot_table((row.key, row.row_hash) for row in rows)
        return _recent_hashes(dataset, partition, since, deps)
    return _last_snapshot(dataset, partition, deps) if dataset.strategy.complete else None


def _write_changes(read: _Read, target: _Slice, upsert_keys: set[str], deps: RuntimeDeps) -> list[Record]:
    """Go through the `full` chunks again and write the changed rows as `data` chunks, in the order they were read.

    Returns the records written, without their payloads. An earlier row of a key that was read again is left out.
    """
    written: list[Record] = []
    buffer: list[Record] = []
    chunks = 0

    def write_buffer() -> None:
        nonlocal chunks, buffer
        deps.spool.put_chunk(target.version_id, chunks, "data", records_to_table(buffer))
        written.extend(Record(r.key, r.op, {}, r.row_hash) for r in buffer)
        chunks += 1
        buffer = []

    pending = set(upsert_keys)
    for chunk_no in range(target.chunks):
        if not pending:
            break
        for record in table_to_records(deps.spool.get_chunk(target.version_id, chunk_no, "full")):
            if record.key in pending and read.latest[record.key] == (target.partition, record.row_hash):
                pending.remove(record.key)
                buffer.append(record)
                if len(buffer) >= spool_settings.CHUNK_ROWS:
                    write_buffer()
    if buffer:
        write_buffer()
    return written


def _content_hash(changes: Sequence[Record], deletes: Mapping[str, int]) -> str:
    """Return a hash of what a version says, which doesn't depend on the order it was read in."""
    items = sorted(
        [[r.key, r.op, r.row_hash] for r in changes] + [[k, "delete", h] for k, h in deletes.items()]
    )
    return hashlib.blake2b(canonical_json(items), digest_size=16).hexdigest()


def _schema_hash(version_id: str, deps: RuntimeDeps) -> str | None:
    """Return a hash of the columns of the version's first data chunk, or None if it has no data chunk yet."""
    if not deps.spool.list_chunks(version_id):
        return None
    schema = deps.spool.get_chunk(version_id, 0).schema.remove_metadata()
    return hashlib.blake2b(str(schema).encode("utf-8"), digest_size=16).hexdigest()


def _discard(version_id: str, deps: RuntimeDeps) -> None:
    """Remove what an earlier attempt may have staged, row first: leftover chunks are cleared by the next attempt."""
    deps.store.versions.discard_staged(version_id)
    deps.spool.delete_version(version_id)


def _moved_here(dataset: Dataset, partition: str, keys: Iterable[str], deps: RuntimeDeps) -> set[str]:
    """Return the keys read in `partition` whose record state names another one, which must upsert even if unchanged.

    A row that returns to a partition can match that partition's old snapshot exactly, yet the destination and record
    state hold the row from where it was since. Only `keep_state` can tell.
    """
    if not dataset.keep_state:
        return set()
    rows = deps.store.state.get_many(dataset.name, keys)
    return {key for key, row in rows.items() if row.partition != partition}


@dataclass(frozen=True)
class _Comparison:
    """What a slice says against what was known: the records that changed (as written to its `data` chunks), the keys
    that disappeared with the hash each had, the keys held back because they were turned down, and the old hashes."""

    changes: list[Record]
    deletes: dict[str, int]
    held: set[str]
    old_hashes: dict[str, int]


def _compare(
    dataset: Dataset, read: _Read, target: _Slice, new: dict[str, int], since: Any, deps: RuntimeDeps
) -> _Comparison:
    """Compare the slice with what was known and write its changed rows as `data` chunks."""
    strategy = dataset.strategy
    old = _old_snapshot(dataset, target.partition, new, since, deps)
    old_hashes = dict(_pairs(old)) if old is not None else {}
    returned = _moved_here(dataset, target.partition, new.keys(), deps)
    # A record that was turned down is still in the source: it keeps its last known state, and isn't a delete. One
    # with no known state here is held as `NOT_DELIVERED`, so that a fixed row upserts and a dropped key is deleted.
    held = read.rejected - new.keys() if strategy.complete else set[str]()
    for key in held:
        new[key] = old_hashes.get(key, NOT_DELIVERED)
    diff = strategy.diff(snapshot_table(new.items()), old)
    upsert_keys = set(cast("list[str]", diff.upsert_keys.to_pylist())) - held | returned
    changes = _write_changes(read, target, upsert_keys, deps)
    deletes = {key: old_hashes.get(key, 0) for key in cast("list[str]", diff.delete_keys.to_pylist())}
    return _Comparison(changes, deletes, held, old_hashes)


def _stage_slice(
    dataset: Dataset,
    run_id: str,
    read: _Read,
    target: _Slice,
    new: dict[str, int],
    since: Any,
    keep_full: bool,
    deps: RuntimeDeps,
) -> Version | None:
    """Compare a slice with what was known, write its chunks and stage its version, if there is anything to say."""
    strategy = dataset.strategy
    found = _compare(dataset, read, target, new, since, deps)
    changes, deletes, held, old_hashes = found.changes, found.deletes, found.held, found.old_hashes
    # A newly held key must reach the snapshot even with no records, or nothing would remember it.
    if not changes and not deletes and held <= old_hashes.keys():
        _discard(target.version_id, deps)
        return None
    if strategy.complete:
        deps.spool.put_chunk(target.version_id, 0, "snapshot", snapshot_table(new.items()))
    if deletes:
        deps.spool.put_chunk(target.version_id, 0, "candidates", snapshot_table(deletes.items()))
    if not keep_full:
        deps.spool.delete_chunks(target.version_id, "full")
    return deps.store.versions.stage(
        VersionDraft(
            version_id=target.version_id,
            dataset=dataset.name,
            partition=target.partition,
            run_id=run_id,
            complete=strategy.complete,
            upserts=sum(r.op == "upsert" for r in changes),
            deletes=sum(r.op == "delete" for r in changes) + len(deletes),
            content_hash=_content_hash(changes, deletes),
            schema_hash=_schema_hash(target.version_id, deps),
            watermark=target.watermark,
            code_sha=deps.code_sha,
        )
    )


def _sealed_earlier(dataset: Dataset, run_id: str, partition: str | None, deps: RuntimeDeps) -> list[Version]:
    """Return what an earlier attempt at this extract already sealed. Sealing is all or nothing, so it did it all.

    `partition` is None for a read across partitions, which can have sealed any number of versions.
    """
    if partition is None:
        return deps.store.versions.sealed_for_run(run_id, dataset.name)
    try:
        version = deps.store.versions.get(stable_id(run_id, dataset.name, partition))
    except KeyError:
        return []
    return [version] if version.status == "sealed" else []


@dataclass(frozen=True)
class VerifyResult:
    """What reading a partition again would change: the records that would upsert and the ones that would delete."""

    upserts: int
    deletes: int


def _verify(dataset: Dataset, partition: str, deps: RuntimeDeps) -> VerifyResult:
    """Read the partition as `run_extract` would and count what differs from the last sealed version, writing nothing
    that stays: its chunks go to a scratch id that is cleared at the end, and no version, quarantine entry or state
    is stored."""
    strategy = dataset.strategy
    by_stamp = strategy if isinstance(strategy, ByStamp) else None
    watermark = None if by_stamp is None else deps.store.versions.latest_watermark(dataset.name)
    since = None if by_stamp is None else _read_since(dataset, by_stamp, watermark)
    stamp_kind = None if watermark is None else _stamp_kind(watermark)
    read = _Read(
        dataset, f"verify-{new_id()}", None if by_stamp else partition, stamp_kind, deps, dry_run=True
    )
    upserts = deletes = 0
    try:
        _read_source(dataset, read, since, deps, _ALL_PARTITIONS if by_stamp else partition)
        for name, target in read.slices.items():
            new = {key: hash_ for key, (slice_name, hash_) in read.latest.items() if slice_name == name}
            found = _compare(dataset, read, target, new, since, deps)
            if strategy.complete:
                # A complete strategy delivers its whole slice, so what changed is read off the hashes instead.
                upserts += sum(found.old_hashes.get(key) != hash_ for key, hash_ in new.items())
                deletes += len(found.old_hashes.keys() - new.keys())
            else:
                upserts += sum(r.op == "upsert" for r in found.changes)
                deletes += sum(r.op == "delete" for r in found.changes) + len(found.deletes)
    finally:
        for target in read.slices.values():
            deps.spool.delete_version(target.version_id)
    return VerifyResult(upserts, deletes)


@overload
def run_extract(
    dataset: Dataset,
    run_id: str,
    partition: str,
    *,
    routes: Sequence[RouteSpec],
    stage_only: bool,
    deps: RuntimeDeps,
    dry_run: Literal[False] = False,
) -> list[Version]: ...


@overload
def run_extract(
    dataset: Dataset,
    run_id: str,
    partition: str,
    *,
    routes: Sequence[RouteSpec],
    stage_only: bool,
    deps: RuntimeDeps,
    dry_run: Literal[True],
) -> VerifyResult: ...


def run_extract(
    dataset: Dataset,
    run_id: str,
    partition: str,
    *,
    routes: Sequence[RouteSpec],
    stage_only: bool,
    deps: RuntimeDeps,
    dry_run: bool = False,
) -> list[Version] | VerifyResult:
    """Read one partition of a dataset and return the versions it produced, or an empty list if nothing changed.

    With `dry_run` it only reads: it returns what would change as a `VerifyResult` and writes nothing that stays.

    A `by_stamp` dataset always reads across partitions (`*`), whatever `partition` says, and produces a version
    for each partition it touched. With `stage_only` the versions wait for `finalize_versions`, otherwise they are
    finalized here. Calling this again with the same arguments starts again and replaces what the last attempt left,
    unless that attempt got as far as sealing: sealed versions never change (I1), and are returned as they are.
    """
    if dry_run:
        return _verify(dataset, partition, deps)
    strategy = dataset.strategy
    by_stamp = strategy if isinstance(strategy, ByStamp) else None
    versions = _sealed_earlier(dataset, run_id, None if by_stamp else partition, deps)
    read_partitions: list[str] = []
    if not versions:
        watermark = None if by_stamp is None else deps.store.versions.latest_watermark(dataset.name)
        since = None if by_stamp is None else _read_since(dataset, by_stamp, watermark)
        stamp_kind = None if watermark is None else _stamp_kind(watermark)
        read = _Read(dataset, run_id, None if by_stamp else partition, stamp_kind, deps)
        _read_source(dataset, read, since, deps, _ALL_PARTITIONS if by_stamp else partition)
        if by_stamp:  # an earlier attempt may have staged a partition that this read no longer touches
            for stale in deps.store.versions.staged_for_run(run_id, dataset.name):
                if stale.partition not in read.slices:
                    _discard(stale.version_id, deps)
        read_partitions = list(read.slices)
        new_by_slice: dict[str, dict[str, int]] = {name: {} for name in read.slices}
        for key, (slice_name, hash_) in read.latest.items():
            new_by_slice[slice_name][key] = hash_
        keep_full = any(route.mode == "replace_partition" for route in routes)
        for name, target in read.slices.items():
            version = _stage_slice(dataset, run_id, read, target, new_by_slice[name], since, keep_full, deps)
            if version is not None:
                versions.append(version)
            # After staging, which may discard a no-op slice's chunks. Other slices of the run still need to see these,
            # and the empty-read guard too. A read that seals at once and has no version leaves nothing to clear them.
            rejected = read.rejected - read.latest.keys()
            if strategy.complete and rejected and (stage_only or version is not None):
                rows = [{_KEY: key} for key in sorted(rejected)]
                table = pa.Table.from_pylist(rows, schema=_REJECTED_SCHEMA)
                deps.spool.put_chunk(target.version_id, 0, "rejected", table)
            # Also after staging. A row accepted now settles what was turned down before, once the run has sealed.
            accepted = new_by_slice[name].keys() - read.rejected
            _write_settled(
                target.version_id, deps.store.quarantine.open_extraction_keys(dataset.name, accepted), deps
            )
    if stage_only:
        return versions
    sealed = finalize_versions(dataset, versions, routes, deps)
    settle_quarantine(dataset, run_id, {*read_partitions, *(v.partition for v in sealed)}, deps)
    return sealed


@dataclass(frozen=True)
class _Reconciled:
    """What the run's slices together say about one staged version.

    `candidates` are its delete candidates as staged, `deletes` those that remain real, `changes` its other records,
    without payloads, and `moves` the partition that each key moving into it came from. `kept` are the candidates that
    stay in its snapshot, with their old hashes: their rows are turned down elsewhere, in this run or an earlier one
    whose quarantine entry is open, so the destination still has them here, and a replace of this slice must not lose
    them. `kept_before` says that no slice of this run with a version has just turned one of them down. `left` are the
    keys whose extraction entries it settles: placeholders leaving its snapshot, and its real deletes.
    """

    version: Version
    candidates: Mapping[str, int]
    deletes: Mapping[str, int]
    changes: Sequence[Record]
    moves: Mapping[str, str]
    kept: Mapping[str, int]
    kept_before: bool
    left: AbstractSet[str]


def _candidates_of(version: Version, deps: RuntimeDeps) -> dict[str, int]:
    if not deps.spool.list_chunks(version.version_id, "candidates"):
        return {}
    return dict(_pairs(deps.spool.get_chunk(version.version_id, 0, "candidates")))


def _changes_of(version: Version, candidates: Mapping[str, int], deps: RuntimeDeps) -> list[Record]:
    """Return the records of the version's data chunks, without payloads and without an earlier attempt's deletes."""
    changes: list[Record] = []
    for chunk in deps.spool.list_chunks(version.version_id):
        table = deps.spool.get_chunk(version.version_id, chunk.chunk_no)
        keys = cast("list[str]", table.column(_KEY).to_pylist())
        ops = cast("list[Op]", table.column(_OP).to_pylist())
        hashes = cast("list[int]", table.column(_HASH).to_pylist())
        changes += [
            Record(key, op, {}, hash_)
            for key, op, hash_ in zip(keys, ops, hashes, strict=True)
            if key not in candidates
        ]
    return changes


def _keys_in(version_id: str, kind: ChunkKind, deps: RuntimeDeps) -> set[str]:
    keys: set[str] = set()
    for chunk in deps.spool.list_chunks(version_id, kind):
        table = deps.spool.get_chunk(version_id, chunk.chunk_no, kind)
        keys.update(cast("list[str]", table.column(_KEY).to_pylist()))
    return keys


def _rejected_keys(version_id: str, deps: RuntimeDeps) -> set[str]:
    return _keys_in(version_id, "rejected", deps)


def _write_settled(version_id: str, keys: Iterable[str], deps: RuntimeDeps) -> None:
    """Add `keys` to those whose extraction quarantine entries `settle_quarantine` is to close for the slice."""
    settled = _keys_in(version_id, "settled", deps).union(keys)
    if settled:
        rows = [{_KEY: key} for key in sorted(settled)]
        deps.spool.put_chunk(version_id, 0, "settled", pa.Table.from_pylist(rows, schema=_REJECTED_SCHEMA))


def settle_quarantine(dataset: Dataset, run_id: str, partitions: Iterable[str], deps: RuntimeDeps) -> None:
    """Close the extraction entries of keys that the run, now sealed, settled, and clear what said so.

    A key is settled by an accepted row, or by leaving a slice that held it as turned down, unless a slice of this run
    turned it down. It comes after sealing, and works from chunks that stay until it is done, so it can be repeated.
    """
    version_ids = [stable_id(run_id, dataset.name, partition) for partition in partitions]
    settled: set[str] = set()
    rejected: set[str] = set()
    for version_id in version_ids:
        settled |= _keys_in(version_id, "settled", deps)
        rejected |= _rejected_keys(version_id, deps)
    if settled - rejected:
        deps.store.quarantine.close_extraction(dataset.name, settled - rejected)
    for version_id in version_ids:
        deps.spool.delete_chunks(version_id, "settled")
        deps.spool.delete_chunks(version_id, "rejected")


def _holders(
    versions: Sequence[Version],
    keys: set[str],
    candidates: Mapping[str, Mapping[str, int]],
    deps: RuntimeDeps,
) -> dict[str, str]:
    """Return, for each of `keys` in a new snapshot, the id of the first of `versions` whose snapshot holds it.

    A key that a version only holds because its row was turned down doesn't count: it has no record to take a move.
    Nor does one it has as a candidate: an earlier attempt kept that in its snapshot, for its old slice's sake.
    """
    holders: dict[str, str] = {}
    if not keys:
        return holders
    for version in versions:
        rejected = _rejected_keys(version.version_id, deps) | candidates.get(version.version_id, {}).keys()
        for chunk in deps.spool.list_chunks(version.version_id, "snapshot"):
            for key, _ in _pairs(deps.spool.get_chunk(version.version_id, chunk.chunk_no, "snapshot")):
                if key in keys and key not in rejected:
                    holders.setdefault(key, version.version_id)
    return holders


def _held_without_a_version(
    dataset: Dataset, versions: Sequence[Version], keys: set[str], deps: RuntimeDeps
) -> tuple[set[str], set[str], set[str]]:
    """Return those of `keys` that the source still has in a slice of the run, with no version to take a move.

    The first set is the keys a slice turned down. The second is the keys of a registered slice with no version, which
    read as unchanged and so still holds what its last snapshot does: a key it holds only as a placeholder isn't one.
    Neither is a delete. The third is those of the first turned down by a slice with no version: old news.
    A key with no entry in any of these can still be protected, by an open quarantine entry from an earlier run.
    """
    if not keys:
        return set(), set(), set()
    run_id = versions[0].run_id
    registered = set(deps.store.versions.slices_for_run(run_id, dataset.name))
    with_version = {version.partition for version in versions}
    turned_down: set[str] = set()
    unchanged: set[str] = set()
    for partition in registered | with_version:
        turned_down |= keys & _rejected_keys(stable_id(run_id, dataset.name, partition), deps)
    repeated: set[str] = set()
    for partition in registered - with_version:
        repeated |= keys & _rejected_keys(stable_id(run_id, dataset.name, partition), deps)
        old = _last_snapshot(dataset, partition, deps)
        if old is not None:
            unchanged.update(key for key, hash_ in _pairs(old) if key in keys and hash_ != NOT_DELIVERED)
    return turned_down, unchanged, repeated


def _by_partition(versions: Iterable[Version]) -> list[Version]:
    return sorted(versions, key=lambda version: version.partition)


def _reconcile(
    dataset: Dataset, versions: Sequence[Version], staged: Sequence[Version], deps: RuntimeDeps
) -> list[_Reconciled]:
    """Tell the real deletes from the moves, across all of `versions`, without changing anything (I10).

    It works only from what stays as it is until sealing, so a repeat after a crash decides the same. Going through
    the versions by partition settles a key that several of them claim the same way each time.
    """
    ordered = _by_partition(staged)
    candidates = {v.version_id: _candidates_of(v, deps) for v in ordered}
    changes: dict[str, list[Record]] = {}
    for version in ordered:
        needed = dataset.keep_state or candidates[version.version_id]
        changes[version.version_id] = (
            _changes_of(version, candidates[version.version_id], deps) if needed else []
        )
    wanted = {key for found in candidates.values() for key in found}
    holders = _holders(_by_partition(versions), wanted, candidates, deps)
    turned_down, unchanged, repeated = _held_without_a_version(
        dataset, versions, wanted - holders.keys(), deps
    )
    upserted = {r.key for found in changes.values() for r in found if r.op == "upsert"}
    state = deps.store.state.get_many(dataset.name, wanted | upserted) if dataset.keep_state else {}
    # A key leaving its placeholder has its entry closed by this run, so unless it was turned down again, no entry
    # protects it here. A slice read in the same run can delete it.
    leaving = {key for found in candidates.values() for key, hash_ in found.items() if hash_ == NOT_DELIVERED}

    moves: dict[str, dict[str, str]] = {v.version_id: {} for v in ordered}
    deletes: dict[str, dict[str, int]] = {v.version_id: {} for v in ordered}
    kept: dict[str, dict[str, int]] = {v.version_id: {} for v in ordered}
    left: dict[str, set[str]] = {v.version_id: set() for v in ordered}
    for version in ordered:
        # An entry from this very slice protects nothing: the slice has just been read without the key, so its row is
        # gone. An entry from elsewhere says that the source may still have the key there.
        protected = turned_down | (
            deps.store.quarantine.open_extraction_keys(
                dataset.name,
                candidates[version.version_id].keys() - holders.keys(),
                outside=version.partition,
            )
            - leaving
        )
        for key, hash_ in candidates[version.version_id].items():
            row = state.get(key)
            if hash_ == NOT_DELIVERED:
                # Never delivered from this slice, so there is nothing to delete or move from it.
                left[version.version_id].add(key)
            elif key in holders:
                # A holder that is sealed already can't take a move, so it may have no entry yet.
                moves.setdefault(holders[key], {}).setdefault(key, version.partition)
            elif key in unchanged:
                continue
            elif key in protected:
                kept[version.version_id][key] = hash_
            elif row is None or row.partition == version.partition:
                deletes[version.version_id][key] = hash_
                left[version.version_id].add(key)
    # Record state knows where a key last was, so it goes over what an old snapshot says.
    for version in ordered:
        for record in changes[version.version_id]:
            row = state.get(record.key)
            if record.op == "upsert" and row is not None and row.partition != version.partition:
                moves[version.version_id][record.key] = row.partition
    return [
        _Reconciled(
            v,
            candidates[v.version_id],
            deletes[v.version_id],
            changes[v.version_id],
            moves[v.version_id],
            kept[v.version_id],
            not kept[v.version_id].keys() & (turned_down - repeated),
            left[v.version_id],
        )
        for v in ordered
    ]


def _check_empty_read(dataset: Dataset, found: _Reconciled, deps: RuntimeDeps) -> None:
    """Refuse to let a read with no accepted rows wipe a slice. A bad source response must not empty a destination.

    A slice that is empty because its records moved to other slices wipes nothing, so only real deletes count. Keys
    held for rejected rows are in the snapshot, but the read didn't accept them, so they don't count as rows.
    """
    version = found.version
    if dataset.allow_empty or not version.complete or not found.deletes:
        return
    rows = sum(chunk.rows for chunk in deps.spool.list_chunks(version.version_id, "snapshot"))
    if rows == len(_rejected_keys(version.version_id, deps)):
        raise ExtractFailed(
            f"dataset {dataset.name!r}, partition {version.partition!r}: the read gave no usable rows, which would "
            "delete the whole slice; if the source can really be empty, set allow_empty=True on the dataset"
        )


def _write_implied_deletes(found: _Reconciled, deps: RuntimeDeps) -> None:
    """Write a version's real deletes at the end of its last data chunk, in place of what an earlier attempt wrote.

    The last chunk is written whole, in one step, so that this can be repeated. A chunk left with nothing is removed:
    it held only the deletes of an earlier attempt.
    """
    version_id = found.version.version_id
    chunks = deps.spool.list_chunks(version_id)
    last = chunks[-1].chunk_no if chunks else 0
    existing = table_to_records(deps.spool.get_chunk(version_id, last)) if chunks else []
    records = [r for r in existing if r.key not in found.candidates]
    if not found.deletes and len(records) == len(existing):
        return
    records += [Record(key, "delete", {}, hash_) for key, hash_ in found.deletes.items()]
    if records:
        deps.spool.put_chunk(version_id, last, "data", records_to_table(records))
    else:
        deps.spool.delete_chunks(version_id, "data", last)


def _new_snapshot(found: _Reconciled, deps: RuntimeDeps) -> dict[str, int]:
    return {**dict(_pairs(deps.spool.get_chunk(found.version.version_id, 0, "snapshot"))), **found.kept}


def _says_nothing(dataset: Dataset, found: _Reconciled, deps: RuntimeDeps) -> bool:
    """Return whether a version turns out to repeat what its partition's last snapshot says.

    Its only candidates are keys it keeps again, because their rows are still turned down elsewhere.
    """
    if found.changes or found.deletes or found.moves or not found.kept or not found.kept_before:
        return False
    old = _last_snapshot(dataset, found.version.partition, deps)
    return old is not None and dict(_pairs(old)) == _new_snapshot(found, deps)


def _write_decision(found: _Reconciled, deps: RuntimeDeps) -> None:
    """Write what `_reconcile` decided over what an earlier attempt wrote, and correct the staged version to match."""
    version = found.version
    if found.moves:
        rows = [{_KEY: key, _MOVED: partition} for key, partition in found.moves.items()]
        deps.spool.put_chunk(version.version_id, 0, "moves", pa.Table.from_pylist(rows, schema=_MOVES_SCHEMA))
    else:
        deps.spool.delete_chunks(version.version_id, "moves")
    if found.kept:
        deps.spool.put_chunk(
            version.version_id, 0, "snapshot", snapshot_table(_new_snapshot(found, deps).items())
        )
    _write_settled(version.version_id, found.left, deps)
    if found.candidates:
        _write_implied_deletes(found, deps)
        deps.store.versions.stage(
            replace(
                version,
                upserts=sum(r.op == "upsert" for r in found.changes),
                deletes=sum(r.op == "delete" for r in found.changes) + len(found.deletes),
                content_hash=_content_hash(found.changes, found.deletes),
            )
        )


def _keys_with_newer_state(
    dataset: Dataset, version: Version, keys: Iterable[str], deps: RuntimeDeps
) -> set[str]:
    """Return the keys whose record state comes from a version sealed after `version`.

    A repeat of a finalize, however late, must not take these back.
    """
    rows = deps.store.state.get_many(dataset.name, keys)
    later: set[str] = set()
    for version_id in {row.last_version for row in rows.values()} - {version.version_id}:
        try:
            seal_seq = deps.store.versions.get(version_id).seal_seq
        except KeyError:
            continue
        if seal_seq is not None and version.seal_seq is not None and seal_seq > version.seal_seq:
            later.add(version_id)
    return {key for key, row in rows.items() if row.last_version in later}


def _update_state(dataset: Dataset, version: Version, deps: RuntimeDeps) -> None:
    for chunk in deps.spool.list_chunks(version.version_id):
        records = table_to_records(deps.spool.get_chunk(version.version_id, chunk.chunk_no))
        newer = _keys_with_newer_state(dataset, version, [r.key for r in records], deps)
        current = [r for r in records if r.key not in newer]
        deps.store.state.put_many(
            dataset.name,
            (
                StateRow(r.key, version.partition, r.row_hash, r.payload, version.version_id)
                for r in current
                if r.op == "upsert"
            ),
        )
        deps.store.state.delete_many(dataset.name, (r.key for r in current if r.op == "delete"))


def finalize_versions(
    dataset: Dataset, versions: Sequence[Version], routes: Sequence[RouteSpec], deps: RuntimeDeps
) -> list[Version]:
    """Decide the implied deletes and moves of staged versions and seal them all together, with a delivery per route.

    A delete candidate that another of `versions` holds in its new snapshot is a move, and with `keep_state` so is one
    whose record state is in another partition. A move is written to the `moves` chunk of the version that received it.

    A placeholder candidate, for a key that was never delivered from its slice, just leaves the snapshot. A candidate
    whose row is turned down elsewhere, in this run or by an open quarantine entry, stays in the snapshot. Calling
    `settle_quarantine` for the run is what comes after this.

    Raises `ExtractFailed`, with nothing sealed, when a complete read came back empty and would really delete records,
    unless the dataset allows it. Versions that are already sealed stay as they are, so this can be repeated: all that
    comes before sealing is worked out from the candidates, which go only after it. Record state is updated after
    sealing, so that a crash in between can't hide a change from the next read of an unsealed version.
    """
    current = [deps.store.versions.get(version.version_id) for version in versions]
    decisions = _reconcile(dataset, current, [v for v in current if v.status == "staged"], deps)
    for found in decisions:
        _check_empty_read(dataset, found, deps)
    # What says nothing goes only after sealing, so that a repeat decides the same from the same chunks.
    silent = {f.version.version_id for f in decisions if _says_nothing(dataset, f, deps)}
    for found in decisions:
        if found.version.version_id not in silent:
            _write_decision(found, deps)
    sealed = deps.store.versions.seal(
        [v.version_id for v in current if v.version_id not in silent], [route.name for route in routes]
    )
    if dataset.keep_state:
        for version in sealed:
            _update_state(dataset, version, deps)
    for version in sealed:
        deps.spool.delete_chunks(version.version_id, "candidates")
    for version_id in silent:
        deps.store.versions.discard_staged(version_id)
        for kind in _VERSION_KINDS:  # what `settle_quarantine` reads stays for it
            deps.spool.delete_chunks(version_id, kind)
    return sealed

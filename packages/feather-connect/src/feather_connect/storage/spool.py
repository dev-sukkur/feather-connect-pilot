"""The spool: where the data of a version lives between extraction and delivery (I8).

Data is held in chunks, each an Arrow table. A data chunk has the payload columns plus `_fc_key`, `_fc_op`,
`_fc_row_hash` (uint64) and `_fc_moved_from`; a snapshot chunk has `_fc_key` and `_fc_row_hash`.
"""

import base64
import json
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol, cast

import pyarrow as pa

from feather_connect.clock import Clock, SystemClock
from feather_connect.core.records import Op, Record

ChunkKind = Literal["data", "snapshot", "full", "candidates", "moves", "rejected", "settled"]

# How many records go into one chunk.
CHUNK_ROWS = 50_000

# Schema metadata key naming the columns that are stored as JSON text, as a JSON list.
JSON_COLUMNS_KEY = "fc.json_columns"

_KEY, _OP, _HASH, _MOVED = "_fc_key", "_fc_op", "_fc_row_hash", "_fc_moved_from"
_RESERVED_PREFIX = "_fc_"
_TAG = "__fc__"  # marks a value that JSON can't hold, so that the column decodes to the same Python types


@dataclass(frozen=True)
class ChunkRef:
    """Where a chunk is and how big it is."""

    version_id: str
    chunk_no: int
    kind: ChunkKind
    rows: int
    nbytes: int


class Spool(Protocol):
    """Chunks by (version, kind, chunk number). Putting a chunk that exists replaces it."""

    def put_chunk(self, version_id: str, chunk_no: int, kind: ChunkKind, table: pa.Table) -> ChunkRef: ...
    def get_chunk(self, version_id: str, chunk_no: int, kind: ChunkKind = "data") -> pa.Table:
        """Raises `KeyError` when there is no such chunk."""
        ...

    def list_chunks(self, version_id: str, kind: ChunkKind = "data") -> list[ChunkRef]:
        """In chunk order."""
        ...

    def delete_chunks(self, version_id: str, kind: ChunkKind, from_chunk: int = 0) -> None:
        """Drop the chunks of `kind` numbered `from_chunk` and up."""
        ...

    def delete_version(self, version_id: str) -> None:
        """Drop every chunk of the version, of every kind."""
        ...

    # Retention (design 7.3). The chunks of a version live on the day of its first write, whatever kind they are
    # except a snapshot, which outlives the day.

    def days(self) -> list[date]:
        """The days that hold chunks, oldest first."""
        ...

    def versions_on(self, day: date) -> list[str]:
        """The versions whose chunks live on `day`."""
        ...

    def snapshot_versions(self) -> list[str]:
        """The versions that have a snapshot chunk."""
        ...

    def drop_day(self, day: date) -> None:
        """Drop the day's chunks, but not the snapshots."""
        ...

    def ensure_partitions(self, days_ahead: int = 7) -> None:
        """Make room for writes from today to `days_ahead` days on, for a backend that needs it."""
        ...


class MemorySpool:
    """A spool in memory, for tests and the in-process engine."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._lock = threading.Lock()
        self._clock = clock or SystemClock()
        self._chunks: dict[tuple[str, ChunkKind, int], pa.Table] = {}
        self._day_of: dict[
            str, date
        ] = {}  # the day of a version's first write of a chunk that is not a snapshot

    def put_chunk(self, version_id: str, chunk_no: int, kind: ChunkKind, table: pa.Table) -> ChunkRef:
        with self._lock:
            self._chunks[(version_id, kind, chunk_no)] = table
            if kind != "snapshot":
                self._day_of.setdefault(version_id, self._clock.now().date())
        return ChunkRef(version_id, chunk_no, kind, table.num_rows, table.nbytes)

    def get_chunk(self, version_id: str, chunk_no: int, kind: ChunkKind = "data") -> pa.Table:
        with self._lock:
            try:
                return self._chunks[(version_id, kind, chunk_no)]
            except KeyError:
                raise KeyError(f"no {kind} chunk {chunk_no} for version {version_id}") from None

    def list_chunks(self, version_id: str, kind: ChunkKind = "data") -> list[ChunkRef]:
        with self._lock:
            found = sorted((n, t) for (v, k, n), t in self._chunks.items() if v == version_id and k == kind)
        return [ChunkRef(version_id, n, kind, t.num_rows, t.nbytes) for n, t in found]

    def delete_chunks(self, version_id: str, kind: ChunkKind, from_chunk: int = 0) -> None:
        with self._lock:
            for key in [
                k for k in self._chunks if k[0] == version_id and k[1] == kind and k[2] >= from_chunk
            ]:
                del self._chunks[key]

    def delete_version(self, version_id: str) -> None:
        with self._lock:
            for key in [k for k in self._chunks if k[0] == version_id]:
                del self._chunks[key]
            self._day_of.pop(version_id, None)

    def days(self) -> list[date]:
        with self._lock:
            return sorted(set(self._day_of.values()))

    def versions_on(self, day: date) -> list[str]:
        with self._lock:
            return sorted(v for v, d in self._day_of.items() if d == day)

    def snapshot_versions(self) -> list[str]:
        with self._lock:
            return sorted({v for v, kind, _ in self._chunks if kind == "snapshot"})

    def drop_day(self, day: date) -> None:
        with self._lock:
            gone = {v for v, d in self._day_of.items() if d == day}
            for key in [k for k in self._chunks if k[0] in gone and k[1] != "snapshot"]:
                del self._chunks[key]
            for version_id in gone:
                del self._day_of[version_id]

    def ensure_partitions(self, days_ahead: int = 7) -> None:
        """Memory needs no partitions."""


def encode_json_value(value: Any) -> Any:
    """The `default` for `json.dumps`: tag the types that JSON can't hold."""
    if isinstance(value, datetime):  # before date, because a datetime is also a date
        return {_TAG: "datetime", "v": value.isoformat()}
    if isinstance(value, date):
        return {_TAG: "date", "v": value.isoformat()}
    if isinstance(value, Decimal):
        return {_TAG: "decimal", "v": str(value)}
    if isinstance(value, bytes):
        return {_TAG: "bytes", "v": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def decode_json_value(obj: dict[str, Any]) -> Any:
    """The `object_hook` for `json.loads`: undo `encode_json_value`."""
    match obj.get(_TAG):
        case "datetime":
            return datetime.fromisoformat(obj["v"])
        case "date":
            return date.fromisoformat(obj["v"])
        case "decimal":
            return Decimal(obj["v"])
        case "bytes":
            return base64.b64decode(obj["v"])
        case _:
            return obj


def _array(values: Sequence[Any], type_: pa.DataType | None = None) -> "pa.Array[Any]":
    """`pa.array`, typed: the stubs leave the element type of the result unknown."""
    array: Any = pa.array(values, type=type_)  # pyright: ignore[reportUnknownMemberType]
    return array


def _has_empty_struct(arrow_type: Any) -> bool:
    """Whether the type holds a struct with no fields, which Parquet can't write."""
    if pa.types.is_struct(arrow_type):
        struct: Any = arrow_type
        count: int = struct.num_fields
        return count == 0 or any(_has_empty_struct(struct.field(i).type) for i in range(count))
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return _has_empty_struct(arrow_type.value_type)
    return False


def _arrow_column(values: list[Any]) -> "pa.Array[Any] | None":
    """Return `values` as an Arrow array that reads back to exactly the same values, or None if none does."""
    present = [v for v in values if v is not None]
    kinds: set[type] = {type(v) for v in present}
    if len(kinds) > 1:  # Arrow would coerce, or refuse, mixed types
        return None
    if kinds == {Decimal} and len({v.as_tuple().exponent for v in present}) > 1:
        return None  # one Arrow decimal type has one scale
    if kinds == {datetime} and len({v.tzinfo is None for v in present}) > 1:
        return None
    try:
        array = _array(values)
    except (pa.ArrowException, OverflowError, ValueError, TypeError):
        return None
    nested = kinds & {dict, list, tuple}
    if (kinds == {Decimal} or nested) and array.to_pylist() != values:
        return None  # for instance structs of different shapes, whose missing fields come back as None
    if _has_empty_struct(array.type):
        return None  # an empty object, such as {}, would make the chunk unwritable as Parquet
    return array


def records_to_table(records: Sequence[Record]) -> pa.Table:
    """Return the records as one chunk.

    A payload column whose values Arrow can't hold as one type, such as numbers mixed with text, is stored as
    JSON text and named in the schema metadata `fc.json_columns`; `table_to_records` decodes it.
    """
    names = list(dict.fromkeys(name for record in records for name in record.payload))
    if clash := next((n for n in names if n.startswith(_RESERVED_PREFIX)), None):
        raise ValueError(f"payload column {clash!r} uses the reserved prefix {_RESERVED_PREFIX!r}")
    arrays: list[pa.Array[Any]] = []
    json_columns: list[str] = []
    for name in names:
        values = [record.payload.get(name) for record in records]
        array = _arrow_column(values)
        if array is None:
            json_columns.append(name)
            array = _array(
                [None if v is None else json.dumps(v, default=encode_json_value) for v in values], pa.string()
            )
        arrays.append(array)
    arrays += [
        _array([r.key for r in records], pa.string()),
        _array([r.op for r in records], pa.string()),
        _array([r.row_hash for r in records], pa.uint64()),
        _array([r.moved_from for r in records], pa.string()),
    ]
    metadata = {JSON_COLUMNS_KEY: json.dumps(json_columns)} if json_columns else None
    return pa.Table.from_arrays(arrays, names=[*names, _KEY, _OP, _HASH, _MOVED], metadata=metadata)


def table_to_records(table: pa.Table) -> list[Record]:
    """Return the records of a chunk made by `records_to_table`.

    Chunks are tabular, so a key that one record lacks and another in the chunk has reads back as None. For a
    delete record the Nones are dropped, so a delete with no payload reads back with none.
    """
    metadata = table.schema.metadata or {}
    json_columns = set(json.loads(metadata.get(JSON_COLUMNS_KEY.encode(), b"[]")))
    columns: dict[str, list[Any]] = {}
    for name in table.column_names:
        if name.startswith(_RESERVED_PREFIX):
            continue
        values: list[Any] = table.column(name).to_pylist()
        if name in json_columns:
            values = [None if v is None else json.loads(v, object_hook=decode_json_value) for v in values]
        columns[name] = values
    keys = cast("list[str]", table.column(_KEY).to_pylist())
    ops = cast("list[Op]", table.column(_OP).to_pylist())
    hashes = cast("list[int]", table.column(_HASH).to_pylist())
    moved = cast("list[str | None]", table.column(_MOVED).to_pylist())
    records: list[Record] = []
    for i, key in enumerate(keys):
        payload = {name: values[i] for name, values in columns.items()}
        if ops[i] == "delete":
            payload = {name: value for name, value in payload.items() if value is not None}
        records.append(Record(key, ops[i], payload, hashes[i], moved[i]))
    return records


def snapshot_table(pairs: Iterable[tuple[str, int]]) -> pa.Table:
    """Return (key, row hash) pairs as a snapshot chunk."""
    keys: list[str] = []
    hashes: list[int] = []
    for key, hash_ in pairs:
        keys.append(key)
        hashes.append(hash_)
    arrays = [_array(keys, pa.string()), _array(hashes, pa.uint64())]
    return pa.Table.from_arrays(arrays, names=[_KEY, _HASH])

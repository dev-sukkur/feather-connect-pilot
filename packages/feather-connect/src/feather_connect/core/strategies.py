"""Change strategies: how a read of a dataset becomes the upserts and deletes of a version."""

# pyarrow's classes can't be subscripted while the program runs, so annotations that do so, such as
# `pa.Array[pa.StringScalar]`, are kept as text.
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

import pyarrow as pa

_KEY = "_fc_key"
_HASH = "_fc_row_hash"
_DURATION = re.compile(r"(?:[0-9]+[smhd])+")
_DURATION_PART = re.compile(r"([0-9]+)([smhd])")
_SECONDS = {"s": 1, "m": 60, "h": 60 * 60, "d": 24 * 60 * 60}


@dataclass(frozen=True)
class Diff:
    """What changed from one snapshot to the next: the keys to upsert and the keys to delete.

    The keys are in no particular order.
    """

    upsert_keys: pa.Array[pa.StringScalar]
    delete_keys: pa.Array[pa.StringScalar]


class Strategy(Protocol):
    """How a dataset's reads become versions.

    `name` is how a connector's spec lists the strategy. `complete` says whether a version describes the
    whole slice, so that it may imply deletes (I4).
    """

    name: str
    complete: bool

    def diff(self, new: pa.Table, old: pa.Table | None) -> Diff:
        """Return the keys to upsert and to delete to get from the snapshot `old` to the snapshot `new`.

        Both tables have a string column `_fc_key` and a uint64 column `_fc_row_hash`, with a row for each
        record. `old` is None when nothing was known before.
        """
        ...


def _no_keys() -> pa.Array[pa.StringScalar]:
    return pa.nulls(0, pa.string())


def _keys(table: pa.Table) -> pa.Array[pa.StringScalar]:
    return table[_KEY].combine_chunks()


def _changed(new: pa.Table, old: pa.Table | None) -> pa.Array[pa.StringScalar]:
    """Return the keys in `new` that `old` doesn't hold with the same hash: the added and the changed."""
    if old is None:
        return _keys(new)
    return _keys(new.join(old, keys=[_KEY, _HASH], join_type="left anti"))


def _vanished(new: pa.Table, old: pa.Table | None) -> pa.Array[pa.StringScalar]:
    """Return the keys in `old` that `new` no longer has."""
    if old is None:
        return _no_keys()
    return _keys(old.join(new, keys=_KEY, join_type="left anti"))


@dataclass(frozen=True)
class ByStamp(Strategy):
    """Reads the rows whose change stamp `field` is later than the last watermark, less `lookback`.

    A row that is read again and hasn't changed is dropped, and a version never implies deletes: a row is
    deleted only when its `deleted_field`, a flag that a soft-deleting source sets, says so.
    """

    field: str
    lookback: timedelta = timedelta(0)
    deleted_field: str | None = None
    name = "by_stamp"
    complete = False

    def diff(self, new: pa.Table, old: pa.Table | None) -> Diff:
        return Diff(_changed(new, old), _no_keys())


@dataclass(frozen=True)
class _SnapshotDiff(Strategy):
    """Reads the whole partition and compares it with the previous snapshot. Keys that went are deletes."""

    name = "snapshot_diff"
    complete = True

    def diff(self, new: pa.Table, old: pa.Table | None) -> Diff:
        return Diff(_changed(new, old), _vanished(new, old))


@dataclass(frozen=True)
class _Replace(Strategy):
    """Reads the whole partition and delivers all of it. Keys that went are deletes."""

    name = "replace"
    complete = True

    def diff(self, new: pa.Table, old: pa.Table | None) -> Diff:
        return Diff(_keys(new), _vanished(new, old))


@dataclass(frozen=True)
class _Push(Strategy):
    """Takes the events that arrive. A version carries only the deletes that the events state."""

    name = "push"
    complete = False

    def diff(self, new: pa.Table, old: pa.Table | None) -> Diff:
        return Diff(_changed(new, old), _no_keys())


def parse_duration(text: str) -> timedelta:
    """Read a duration such as `90s`, `15m`, `2h`, `7d` or `1h30m`.

    It is whole numbers, each followed by a unit: s, m, h or d. Raises `ValueError` for anything else.
    """
    if not _DURATION.fullmatch(text):
        raise ValueError(
            f"{text!r} is not a duration; write whole numbers with s, m, h or d, such as '90s' or '1h30m'"
        )
    seconds = sum(int(number) * _SECONDS[unit] for number, unit in _DURATION_PART.findall(text))
    try:
        return timedelta(seconds=seconds)
    except OverflowError as e:
        raise ValueError(f"{text!r} is too long to be a duration") from e


def by_stamp(field: str, lookback: str = "0s", deleted_field: str | None = None) -> ByStamp:
    """Return the strategy for a source with a reliable change stamp in `field`.

    Each read starts `lookback` before the last watermark, such as `"2h"`, to catch rows that were stamped
    late. See `ByStamp` for `deleted_field`.
    """
    return ByStamp(field, parse_duration(lookback), deleted_field)


def snapshot_diff() -> Strategy:
    """Return the strategy for a source with no change stamp: read everything and diff the row hashes."""
    return _SnapshotDiff()


def replace() -> Strategy:
    """Return the strategy for a small source that is cheaper to reload than to diff."""
    return _Replace()


def push() -> Strategy:
    """Return the strategy for a source that sends events, such as webhooks or change data capture."""
    return _Push()

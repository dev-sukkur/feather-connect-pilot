"""Reading other datasets from a transform or a derived dataset (M3.1 plan 1 F1, F2; discovery FR-C5).

A reference is a dataset's latest sealed state: the record state the sealer keeps for a dataset with `keep_state=True`.
Its stamp is the `seal_seq` up to which that state is written (`extract.mark_state`), so two reads with
the same stamp read the same rows. `TaskReferences` is given to one task's contexts: it reads each reference once, the
first time it is asked for, and keeps that for the task, so every chunk of an apply sees the same rows (H16). It
remembers the stamps it used, for the delivery attempt. The rows themselves are cached for the process by
(tenant, dataset, stamp), so the next task reads them again only after a new version is sealed.
"""

import os
import threading
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.records import Record, canonical_json
from feather_connect.core.sql import References
from feather_connect.runtime.extract import STATE_MARK_KEY, state_marker
from feather_connect.storage.port import Store
from feather_connect.storage.spool import records_to_table

_PAGE = 10_000
_CACHED = 16  # references kept per process, least recently used out first


def _max_rows() -> int:
    text = os.environ.get("FEATHER_SQL_REFERENCE_MAX_ROWS", "5000000")
    try:
        return int(text)
    except ValueError:
        raise PermanentError(f"FEATHER_SQL_REFERENCE_MAX_ROWS is {text!r}: give a number of rows") from None


def key_text(key: Any) -> str:
    """A business key as the store names it: the value as text, or a JSON list for a compound key (`make_key`)."""
    if isinstance(key, tuple | list):
        return canonical_json(list(key)).decode("utf-8")  # pyright: ignore[reportUnknownArgumentType]
    return str(key)


@dataclass(frozen=True)
class _Loaded:
    stamp: int
    table: pa.Table
    rows: Mapping[str, Mapping[str, Any]]


class _ProcessCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[str, str], _Loaded] = OrderedDict()

    def get(self, tenant: str, dataset: str, stamp: int) -> _Loaded | None:
        with self._lock:
            found = self._entries.get((tenant, dataset))
            if found is None or found.stamp != stamp:
                return None
            self._entries.move_to_end((tenant, dataset))
            return found

    def put(self, tenant: str, dataset: str, loaded: _Loaded) -> None:
        with self._lock:
            self._entries[(tenant, dataset)] = loaded
            self._entries.move_to_end((tenant, dataset))
            while len(self._entries) > _CACHED:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


CACHE = _ProcessCache()


def full_name(dataset: str, package: str | None) -> str:
    """The stored name of a referenced dataset: a bare name is the package's own, `<package>/<name>` another's, as in
    `fc.ready`."""
    return dataset if package is None or "/" in dataset else f"{package}/{dataset}"


def stamp_of(store: Store, dataset: str) -> int | None:
    """The stamp of the dataset's state, or None if no state has been written: it doesn't keep state, or nothing of it
    is sealed yet."""
    done = store.overrides.get(state_marker(dataset), STATE_MARK_KEY)
    return None if done is None else int(done)


def _load(store: Store, dataset: str, stamp: int) -> _Loaded:
    limit = _max_rows()
    rows: dict[str, Mapping[str, Any]] = {}
    after: str | None = None
    while True:
        page = store.state.scan(dataset, after=after, limit=_PAGE)
        for row in page:
            rows[row.key] = row.payload
        if len(rows) > limit:
            raise PermanentError(
                f"reference {dataset!r} has more than {limit:,} records (FEATHER_SQL_REFERENCE_MAX_ROWS): "
                "read one that big from the warehouse, through the DuckDB connector"
            )
        if len(page) < _PAGE:
            break
        after = page[-1].key
    records = [Record(key, "upsert", payload, 0) for key, payload in rows.items()]
    table: pa.Table = records_to_table(records) if records else pa.Table.from_pylist([])
    table = table.drop_columns([c for c in table.column_names if c.startswith("_fc_")])
    return _Loaded(stamp, table, rows)


class TaskReferences(References):
    """The references of one task. See the module's docstring."""

    def __init__(self, store: Store, tenant: str, package: str | None = None) -> None:
        self._store = store
        self._tenant = tenant
        self._package = package
        self._pinned: dict[str, _Loaded] = {}
        self._lock = threading.Lock()

    @property
    def used(self) -> dict[str, int]:
        """Each reference read so far, with the stamp of the state read."""
        return {name: loaded.stamp for name, loaded in self._pinned.items()}

    def _get(self, dataset: str) -> _Loaded:
        with self._lock:
            if dataset in self._pinned:
                return self._pinned[dataset]
            stored = full_name(dataset, self._package)
            stamp = stamp_of(self._store, stored)
            if stamp is None:
                raise TransientError(
                    f"reference {dataset!r} has no sealed state yet: it is read once its first version is sealed "
                    "(and it must keep its state, keep_state=True)"
                )
            loaded = CACHE.get(self._tenant, stored, stamp)
            if loaded is None:
                loaded = _load(self._store, stored, stamp)
                CACHE.put(self._tenant, stored, loaded)
            self._pinned[dataset] = loaded
            return loaded

    def table(self, dataset: str) -> pa.Table:
        return self._get(dataset).table

    def lookup(self, dataset: str, key: Any) -> dict[str, Any] | None:
        found = self._get(dataset).rows.get(key_text(key))
        return None if found is None else dict(found)

"""SQL over records, on embedded DuckDB (M3.1 plan 1 F1, F2; discovery FR-C2, FR-C4, FR-C5).

`fc.sql(query)` is a batch map: the batch is the table `batch`, and the query's result is what the sink gets. In the
query, `{{ dataset('name') }}` is the latest sealed state of another dataset, one that keeps its state
(`keep_state=True`): a reference. `fc.derived(query, inputs=[...])` is a source whose rows are such a query over other
datasets.

DuckDB runs in the worker's process, shut in: it can't read files or URLs (`enable_external_access = false`), it sees
only `batch` and the references the query names, and its memory is capped by `FEATHER_SQL_MEMORY` (default `1GB`). It
comes with the `sql` extra: `pip install "feather-connect[sql]"`.
"""

import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import pyarrow as pa

from feather_connect.core.errors import PermanentError

if TYPE_CHECKING:
    from feather_connect.core.context import Context

_REFERENCE = re.compile(r"\{\{\s*dataset\(\s*(['\"])([^'\"]+)\1\s*\)\s*\}\}")
_ANY_TEMPLATE = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_BATCH_MAP = "_fc_batch_map"  # the mark `fc.batch_map` sets
_JSON_COLUMNS = b"fc.json_columns"
INSTALL = 'pip install "feather-connect[sql]"'


class References(Protocol):
    """What a SQL transform, a lookup or a derived dataset reads other datasets through. The runtime gives one to each
    context; it reads each dataset's state once per task and keeps it, by its stamp, for the process."""

    def table(self, dataset: str) -> pa.Table:
        """The dataset's latest sealed state, one row per record, its payload's fields as columns."""
        ...

    def lookup(self, dataset: str, key: Any) -> dict[str, Any] | None:
        """The payload of the record with business key `key`, or None."""
        ...


def references_in(query: str) -> tuple[str, ...]:
    """The datasets a query names with `{{ dataset('name') }}`, in order of first use. Raises `ValueError` for any
    other `{{ … }}`, so that a typo doesn't reach DuckDB as text."""
    names = list(dict.fromkeys(m.group(2) for m in _REFERENCE.finditer(query)))
    left = _ANY_TEMPLATE.findall(_REFERENCE.sub("", query))
    if left:
        raise ValueError(f"{left[0]} is not a reference: write {{{{ dataset('name') }}}}")
    return tuple(names)


def _duckdb() -> Any:
    try:
        import duckdb
    except ImportError:
        raise PermanentError(f"SQL needs DuckDB, which isn't installed: {INSTALL}") from None
    return duckdb


def _memory_limit() -> str:
    limit = os.environ.get("FEATHER_SQL_MEMORY", "1GB").strip()
    if not re.fullmatch(r"\d+(\.\d+)?\s*(KB|MB|GB|TB|KiB|MiB|GiB|TiB)", limit, re.IGNORECASE):
        raise PermanentError(f"FEATHER_SQL_MEMORY is {limit!r}: give a size such as 1GB or 512MB")
    return limit


def run_query(query: str, tables: Mapping[str, pa.Table], references: References | None) -> pa.Table:
    """Run `query` on a shut-in DuckDB with `tables` registered by name and each reference it names as a view. Returns
    the result as an Arrow table. A failing query is a `PermanentError`: running it again gives the same answer."""
    names = references_in(query)
    if names and references is None:
        raise PermanentError(f"the query reads {names[0]!r}, and nothing here can read other datasets")
    duckdb = _duckdb()
    connection = duckdb.connect(
        ":memory:",
        config={"enable_external_access": False, "memory_limit": _memory_limit(), "threads": 2},
    )
    try:
        for name, table in tables.items():
            connection.register(name, table)
        aliases: dict[str, str] = {}
        for i, dataset in enumerate(names):
            assert references is not None
            alias = f"__fc_reference_{i}"
            connection.register(alias, references.table(dataset))
            aliases[dataset] = alias
        text = _REFERENCE.sub(lambda m: aliases[m.group(2)], query)
        try:
            result = connection.execute(text).arrow()
        except duckdb.Error as error:
            raise PermanentError(f"the SQL failed: {error}") from error
        return result.read_all() if isinstance(result, pa.RecordBatchReader) else result
    finally:
        connection.close()


@dataclass(frozen=True)
class SqlMap:
    """A batch map written in SQL: see `sql`."""

    query: str
    references: tuple[str, ...]

    def __call__(self, batch: pa.Table, ctx: "Context") -> pa.Table:
        result = run_query(self.query, {"batch": batch}, ctx.references)
        return _keep_json_columns(batch, result)


def _keep_json_columns(batch: pa.Table, result: pa.Table) -> pa.Table:
    """Carry over which columns hold JSON text (mixed types the spool couldn't type), for those the query kept."""
    metadata = batch.schema.metadata or {}
    if _JSON_COLUMNS not in metadata:
        return result
    kept = [c for c in json.loads(metadata[_JSON_COLUMNS]) if c in result.column_names]
    merged: dict[bytes, bytes] = {**(result.schema.metadata or {}), _JSON_COLUMNS: json.dumps(kept).encode()}
    return result.replace_schema_metadata(merged)


def sql(query: str) -> SqlMap:
    """A transform written in SQL, for `fc.to(map=...)`: the batch is the table `batch`, and the query's result is
    what the sink gets.

        fc.to(sink, map=fc.sql(\"\"\"
            select b.*, m.description
            from batch b left join {{ dataset('sap.material') }} m using (material)
        \"\"\"))

    `{{ dataset('name') }}` is the latest sealed state of a dataset that keeps its state (`keep_state=True`). Keep
    `b.*`, or at least the `_fc_key` column, so that each row stays its record; a row with no key fails the version. Deletes don't go through the query. Needs the `sql` extra.
    """
    names = references_in(query)
    out = SqlMap(query, names)
    object.__setattr__(out, _BATCH_MAP, True)  # a batch map, as `fc.batch_map` marks one
    return out


def is_sql(value: object) -> bool:
    return isinstance(value, SqlMap)


def referenced(maps: Sequence[object]) -> list[str]:
    """The reference datasets of the SQL maps among `maps`."""
    return [name for m in maps if isinstance(m, SqlMap) for name in m.references]

"""SQL over records, on embedded DuckDB (M3.1 plan 1 F1, F2; discovery FR-C2, FR-C4, FR-C5).

`fc.sql(query)` is a batch map: the batch is the table `batch`, and the query's result is what the sink gets. In the
query, `{{ dataset('name') }}` is the latest sealed state of another dataset, one that keeps its state
(`keep_state=True`): a reference. `fc.derived(query)` is a source whose rows are such a query over other datasets.
`{{ today }}` and `{{ now }}` are the date and time of the read or the delivery, in the time zone given; DuckDB's own
`current_date` and `now()` are the worker's clock, in UTC, and `feather validate` warns of them.

DuckDB runs in the worker's process, shut in: it can't read files or URLs (`enable_external_access = false`), it sees
only `batch` and the references the query names, and its memory is capped by `FEATHER_SQL_MEMORY` (default `1GB`). It
comes with the `sql` extra: `pip install "feather-connect[sql]"`.
"""

import json
import os
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyarrow as pa

from feather_connect.core.connector import CheckResult, ConnectorSpec, ReadRequest, StreamSchema
from feather_connect.core.errors import PermanentError
from feather_connect.core.partitions import _Unpartitioned  # pyright: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from feather_connect.core.context import Context
    from feather_connect.core.dataset import Dataset
    from feather_connect.core.flow import Flow, Problem

_REFERENCE = re.compile(r"\{\{\s*dataset\(\s*(['\"])([^'\"]+)\1\s*\)\s*\}\}")
_ANY_TEMPLATE = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_TIME = re.compile(r"\{\{\s*(today|now)\s*\}\}")
_WALL_CLOCK = re.compile(r"\b(current_date|current_timestamp|now\s*\(\s*\)|today\s*\(\s*\))", re.IGNORECASE)
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
    left = _ANY_TEMPLATE.findall(_TIME.sub("", _REFERENCE.sub("", query)))
    if left:
        raise ValueError(
            f"{left[0]} is not a reference: write {{{{ dataset('name') }}}}, {{{{ today }}}} or {{{{ now }}}}"
        )
    return tuple(names)


def uses_wall_clock(query: str) -> bool:
    """Whether the query reads DuckDB's clock, which is the worker's, rather than `{{ today }}` or `{{ now }}`."""
    return _WALL_CLOCK.search(_REFERENCE.sub("", query)) is not None


def _with_time(query: str, now: datetime | None, tz: str) -> str:
    if not _TIME.search(query):
        return query
    if now is None:
        raise PermanentError("the query uses {{ today }} or {{ now }}, and nothing here gives a time")
    local = now.astimezone(ZoneInfo(tz))

    def value(match: re.Match[str]) -> str:
        if match.group(1) == "today":
            return f"DATE '{local.date().isoformat()}'"
        return f"TIMESTAMPTZ '{local.isoformat()}'"

    return _TIME.sub(value, query)


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


def run_query(
    query: str,
    tables: Mapping[str, pa.Table],
    references: References | None,
    *,
    now: datetime | None = None,
    tz: str = "UTC",
) -> pa.Table:
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
        text = _with_time(_REFERENCE.sub(lambda m: aliases[m.group(2)], query), now, tz)
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
        result = run_query(self.query, {"batch": batch}, ctx.references, now=ctx.now, tz=ctx.tz)
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


class DerivedSource:
    """A source whose rows are a SQL query over other datasets' latest state: see `derived`."""

    spec: ClassVar[ConnectorSpec] = ConnectorSpec(
        name="feather.derived", strategies=frozenset({"snapshot_diff", "replace"}), maturity="beta"
    )
    connection = "feather.derived"  # no connection: it reads Feather's own store
    connectionless = True

    def __init__(self, query: str, *, tz: str = "UTC") -> None:
        try:
            ZoneInfo(tz)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError(f"{tz!r} is not an IANA time zone, such as Asia/Kolkata") from None
        self.query = query
        self.tz = tz
        self.references = references_in(query)

    def check(self, ctx: "Context") -> CheckResult:
        return CheckResult(ok=True)

    def discover(self, ctx: "Context") -> list[StreamSchema]:
        return []

    def read(self, ctx: "Context", req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        table = run_query(self.query, {}, ctx.references, now=ctx.now, tz=self.tz)
        yield table.to_pylist()


def derived(query: str, *, tz: str = "UTC") -> DerivedSource:
    """A source whose rows are a SQL query over other datasets, for a derived dataset (FR-C4):

        fc.Dataset(
            "pricing.effective_price",
            source=fc.derived(
                "select item, store, price from {{ dataset('sap.price') }} where valid_from <= {{ today }} "
                "qualify row_number() over (partition by item, store order by valid_from desc) = 1",
                tz="Asia/Kolkata",
            ),
            key=("item", "store"),
            strategy=fc.strategy.snapshot_diff(),
        )

    Each `{{ dataset('name') }}` is that dataset's latest sealed state (it must keep its state). `{{ today }}`
    and `{{ now }}` are the date and time of the read in `tz`. It is read on its flow's triggers: a cron recomputes
    with no change at the source, which is how a future-dated price takes effect. What follows the read is any
    dataset's: `snapshot_diff` turns the rows into upserts and deletes. It isn't partitioned. Needs the `sql` extra.
    """
    return DerivedSource(query, tz=tz)


def reference_problems(flows: "Sequence[Flow]") -> "list[Problem]":
    """The mistakes in the flows' references that show without running them (M3.1 F1): a reference to a dataset no
    flow reads, or to one that doesn't keep its state, which a reference reads; and SQL with DuckDB not installed. A
    `<package>/<name>` of another package is checked when its flow runs."""
    from feather_connect.core.flow import Problem

    datasets = {d.name: d for flow in flows for d in flow.datasets}
    problems: list[Problem] = []
    for flow in flows:
        for i, dataset in enumerate(flow.datasets):
            if isinstance(dataset.source, DerivedSource):
                problems += _derived_problems(
                    dataset, dataset.source, datasets, f"{flow.name}.reads[{i}].source"
                )
        for i, route in enumerate(flow.routes):
            if not isinstance(route.map, SqlMap):
                continue
            path = f"{flow.name}.deliver[{i}].map"
            problems += _named_problems(route.map.references, datasets, path)
            if uses_wall_clock(route.map.query):
                problems.append(_wall_clock(path))
            if _missing_duckdb():
                problems.append(
                    Problem(
                        "error", "sql_unavailable", "SQL needs DuckDB, which isn't installed", path, INSTALL
                    )
                )
    return problems + _cycles(flows)


def _derived_problems(
    dataset: "Dataset", source: DerivedSource, datasets: "Mapping[str, Dataset]", path: str
) -> "list[Problem]":
    from feather_connect.core.flow import Problem

    found = _named_problems(source.references, datasets, path)
    if not source.references:
        found.append(
            Problem(
                "warning",
                "derived_reads_nothing",
                f"the derived dataset {dataset.name!r} reads no dataset",
                path,
                "name its inputs with {{ dataset('name') }}",
            )
        )
    if not isinstance(dataset.partition, _Unpartitioned):
        found.append(
            Problem(
                "error",
                "derived_partitioned",
                f"the derived dataset {dataset.name!r} is partitioned, and a derived dataset isn't yet",
                path,
                "leave partition out: one read covers it all",
            )
        )
    if uses_wall_clock(source.query):
        found.append(_wall_clock(path))
    if _missing_duckdb():
        found.append(
            Problem("error", "sql_unavailable", "SQL needs DuckDB, which isn't installed", path, INSTALL)
        )
    return found


def _wall_clock(path: str) -> "Problem":
    from feather_connect.core.flow import Problem

    return Problem(
        "warning",
        "sql_wall_clock",
        "the SQL reads DuckDB's clock (current_date, now()), which is the worker's, in UTC",
        path,
        "use {{ today }} or {{ now }}, the time of the read or delivery in its time zone",
    )


def _cycles(flows: "Sequence[Flow]") -> "list[Problem]":
    from feather_connect.core.flow import Problem

    reads = {
        d.name: set(d.source.references)
        for f in flows
        for d in f.datasets
        if isinstance(d.source, DerivedSource)
    }
    found: list[Problem] = []
    for start in sorted(reads):
        stack, seen = [start], set[str]()
        while stack:
            for nxt in reads.get(stack.pop(), set[str]()):
                if nxt == start:
                    found.append(
                        Problem(
                            "error",
                            "derived_cycle",
                            f"the derived dataset {start!r} reads itself, through other derived datasets",
                            start,
                            "break the cycle: a derived dataset can't be its own input",
                        )
                    )
                    stack = []
                    break
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
    return found


def _named_problems(names: Sequence[str], datasets: "Mapping[str, Dataset]", path: str) -> "list[Problem]":
    from feather_connect.core.flow import Problem

    found: list[Problem] = []
    for name in names:
        if "/" in name:
            continue
        dataset = datasets.get(name)
        if dataset is None:
            found.append(
                Problem(
                    "error",
                    "reference_unknown",
                    f"the SQL reads {name!r}, which no flow of the project reads",
                    path,
                    "name a dataset a flow reads, or another package's as <package>/<dataset>",
                )
            )
        elif not dataset.keep_state:
            found.append(
                Problem(
                    "error",
                    "reference_without_state",
                    f"the SQL reads {name!r}, which doesn't keep its state",
                    path,
                    f"set keep_state=True on the dataset {name!r}: a reference reads its latest state",
                )
            )
    return found


def _missing_duckdb() -> bool:
    import importlib.util

    return importlib.util.find_spec("duckdb") is None

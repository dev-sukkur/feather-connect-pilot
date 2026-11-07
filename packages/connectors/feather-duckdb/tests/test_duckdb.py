"""`duckdb.Query` and `duckdb.Table` on a temporary file: the kit, keys, modes, names, chunking and the token."""

import logging
import os
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb as duck
import pyarrow as pa
import pytest

import feather_connect as fc
from feather_connect.connectors import duckdb  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.testing.conformance import SinkConformance, SourceConformance
from feather_connect.testing.readme import run_readme_examples

NOW = datetime(2026, 11, 7, tzinfo=UTC)


def context(database: str, **config: Any) -> Context:
    return Context({"database": database, **config}, logging.getLogger("duckdb-test"), NOW, "UTC")


def batch(
    records: list[ApplyRecord],
    *,
    mode: Any = "upsert",
    chunk: int = 0,
    last: bool = True,
    partition: str = "*",
    version: str = "v1",
) -> ApplyBatch:
    return ApplyBatch(
        "r", version, chunk, partition, mode, records, f"r:{version}:{chunk}:0", chunk == 0, last
    )


def up(key: Any, **payload: Any) -> ApplyRecord:
    return ApplyRecord(key, "upsert", payload)


def query(path: Path, statement: str) -> list[tuple[Any, ...]]:
    with duck.connect(str(path)) as conn:
        return conn.execute(statement).fetchall()


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return tmp_path / "lake.duckdb"


class TestQuery(SourceConformance):
    @pytest.fixture
    def ctx(self, db: Path) -> Context:
        return context(str(db))

    @pytest.fixture
    def source(self, db: Path) -> Any:
        with duck.connect(str(db)) as conn:
            conn.execute("create table items (id varchar, n integer, name varchar, updated varchar)")
        return duckdb.Query("lake", sql="select * from items")

    @pytest.fixture
    def seed(self, db: Path) -> Callable[[list[dict[str, Any]]], None]:
        def load(rows: list[dict[str, Any]]) -> None:
            table = pa.Table.from_pylist(rows)
            with duck.connect(str(db)) as conn:
                conn.register("seed", table)
                conn.execute("delete from items")
                conn.execute("insert into items select id, n, name, updated from seed")

        return load

    @pytest.fixture
    def permanent_fault(self, db: Path) -> Callable[[], None]:
        def drop() -> None:
            with duck.connect(str(db)) as conn:
                conn.execute("drop table items")

        return drop


class TestTable(SinkConformance):
    @pytest.fixture
    def ctx(self, db: Path) -> Context:
        return context(str(db))

    @pytest.fixture
    def sink(self) -> Any:
        return duckdb.Table("lake", table="items", key_columns=["id"])

    @pytest.fixture
    def fresh_sink(self) -> Callable[[], Any]:
        return lambda: duckdb.Table("lake", table="items", key_columns=["id"])

    @pytest.fixture
    def read_back(self, db: Path) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        def back() -> Mapping[Any, Mapping[str, Any]]:
            if not db.exists():
                return {}
            with duck.connect(str(db)) as conn:
                there = conn.execute("select 1 from information_schema.tables where table_name = 'items'")
                if not there.fetchall():
                    return {}
                found = conn.execute("select id, n, name, updated from items").fetchall()
            return {r[0]: {"id": r[0], "n": r[1], "name": r[2], "updated": r[3]} for r in found}

        return back

    @pytest.fixture
    def permanent_fault(self, sink: Any) -> Callable[[], None]:
        def break_it() -> None:
            sink.key_columns = ()  # an upsert without keys is refused

        return break_it


def test_append_keeps_fc_columns(db: Path):
    sink = duckdb.Table("lake", table="events")
    sink.apply(
        context(str(db)), batch([up("e1", id="e1", kind="a"), up("e2", id="e2", kind="b")], mode="append")
    )
    found = query(
        db,
        "select id, _fc_key, _fc_partition, _fc_version_id, _fc_op, _fc_applied_at is not null from events order by id",
    )
    assert found == [("e1", "e1", "*", "v1", "upsert", True), ("e2", "e2", "*", "v1", "upsert", True)]


def test_upsert_on_composite_key(db: Path):
    with duck.connect(str(db)) as conn:
        # no primary key: merge
        conn.execute("create table lines (order_id integer, line integer, qty integer)")
    sink = duckdb.Table("lake", table="lines", key_columns=["order_id", "line"])
    ctx = context(str(db))
    sink.apply(ctx, batch([up((1, 1), order_id=1, line=1, qty=2), up((1, 2), order_id=1, line=2, qty=5)]))
    sink.apply(ctx, batch([up((1, 1), order_id=1, line=1, qty=3)], version="v2"))
    sink.apply(ctx, batch([ApplyRecord((1, 2), "delete", None)], version="v3"))
    assert query(db, "select order_id, line, qty, _fc_key from lines") == [(1, 1, 3, "[1,1]")]
    with duck.connect(str(db)) as conn:
        # a primary key: insert or replace
        conn.execute("create table keyed (id varchar primary key, v integer)")
    keyed = duckdb.Table("lake", table="keyed", key_columns=["id"])
    keyed.apply(ctx, batch([up("a", id="a", v=1)], version="k1"))  # its own idempotency keys
    keyed.apply(ctx, batch([up("a", id="a", v=2)], version="k2"))
    assert query(db, "select id, v from keyed") == [("a", 2)]


def test_replace_partition_is_atomic(db: Path):
    with duck.connect(str(db)) as conn:
        conn.execute("create table daily (id varchar, day varchar, v integer)")
    sink = duckdb.Table("lake", table="daily", key_columns=["id"])
    ctx = context(str(db))
    sink.apply(ctx, batch([up("a", id="a", day="d1", v=1)], mode="replace_partition", partition="day=d1"))
    common: dict[str, Any] = {"mode": "replace_partition", "partition": "day=d1", "version": "v2"}
    sink.apply(ctx, batch([up("c", id="c", day="d1", v=2)], chunk=0, last=False, **common))
    with pytest.raises(PermanentError, match="nope"):
        sink.apply(ctx, batch([up("d", id="d", day="d1", nope=1)], chunk=1, **common))
    assert query(db, "select id from daily") == [("a",)]
    sink.apply(ctx, batch([up("d", id="d", day="d1", v=2)], chunk=1, **common))
    assert query(db, "select id from daily order by id") == [("c",), ("d",)]
    moved = ApplyRecord("c", "upsert", {"id": "c", "day": "d2", "v": 3}, moved_from="day=d1")
    sink.apply(ctx, batch([moved], mode="replace_partition", partition="day=d2", version="v3"))
    assert query(db, "select id, day from daily order by id") == [("c", "d2"), ("d", "d1")]
    stages = "select count(*) from information_schema.tables where table_name like '_feather_stage_%'"
    assert query(db, stages) == [(0,)]


def test_new_column_is_added_on_append(db: Path):
    sink = duckdb.Table("lake", table="events")
    ctx = context(str(db))
    sink.apply(ctx, batch([up("e1", id="e1")], mode="append"))
    sink.apply(ctx, batch([up("e2", id="e2", extra=5)], mode="append", version="v2"))
    assert query(db, "select id, extra from events order by id") == [("e1", None), ("e2", 5)]


def test_identifiers_are_quoted(db: Path):  # Review Focus 3
    sink = duckdb.Table("lake", table='order-lines "x"', key_columns=["id"])
    sink.apply(context(str(db)), batch([up("1", id="1", select=5)], mode="append"))
    assert query(db, 'select id, "select" from "order-lines ""x"""') == [("1", 5)]


def test_unknown_column_on_upsert_names_it(db: Path):  # Review Focus 3
    sink = duckdb.Table("lake", table="items", key_columns=["id"])
    ctx = context(str(db))
    sink.apply(ctx, batch([up("1", id="1")]))
    with pytest.raises(PermanentError, match="no_such_col"):
        sink.apply(ctx, batch([up("2", id="2", no_such_col=1)], version="v2"))


def test_long_write_is_split_into_chunks(db: Path):
    sink = duckdb.Table("lake", table="events", write_rows=2)
    sink.apply(context(str(db)), batch([up(f"e{n}", id=f"e{n}") for n in range(5)], mode="append"))
    assert sink.writes == 3 and query(db, "select count(*) from events") == [(5,)]


def test_token_never_appears_in_errors(tmp_path: Path):
    ctx = context(str(tmp_path / "no-such-dir" / "x.duckdb"), token="md-secret-token-123")
    result = duckdb.Query("lake", sql="select 1").check(ctx)
    assert not result.ok and "md-secret-token-123" not in result.message


def test_a_query_reads_a_partition_and_since(db: Path):
    with duck.connect(str(db)) as conn:
        conn.execute(
            "create table sales as select * from (values (1, date '2026-11-06', 5), "
            "(2, date '2026-11-07', 9), (3, date '2026-11-07', 1)) t(id, day, u)"
        )
    source = duckdb.Query("lake", sql="select * from sales")
    req = ReadRequest("day=2026-11-07", fc.partition.by_day("day", tz="UTC"), since=5, stamp_field="u")
    assert [r["id"] for b in source.read(context(str(db)), req) for r in b.to_pylist()] == [2]


def test_readme_example_runs(tmp_path: Path):
    names = run_readme_examples(Path(__file__).resolve().parents[1] / "README.md", tmp_path)
    (route,) = names["flow"].routes
    assert isinstance(route.sink, duckdb.Table)


@pytest.mark.skipif(not os.environ.get("MOTHERDUCK_TOKEN"), reason="needs MOTHERDUCK_TOKEN; runs nightly")
def test_motherduck_round_trip():
    token = os.environ["MOTHERDUCK_TOKEN"]
    ctx = context("md:", token=token)
    table = f"feather_test_{os.getpid()}"
    sink = duckdb.Table("lake", table=table, key_columns=["id"])
    sink.apply(ctx, batch([up("1", id="1", v=1)]))
    source = duckdb.Query("lake", sql=f"select * from {table}")
    try:
        read = [
            r["id"] for b in source.read(ctx, ReadRequest("*", fc.partition.none())) for r in b.to_pylist()
        ]
        assert read == ["1"]
    finally:
        with duck.connect("md:", config={"motherduck_token": token}) as conn:
            conn.execute(f"drop table if exists {table}")

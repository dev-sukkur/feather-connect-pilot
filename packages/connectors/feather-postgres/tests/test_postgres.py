"""`postgres.Query` and `postgres.Table` on a scratch database: the kit, push-down, keys, modes, names and errors."""

import logging
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

import feather_connect as fc
from feather_connect.connectors import postgres  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.testing.conformance import SinkConformance, SourceConformance
from feather_connect.testing.readme import run_readme_examples

ITEMS = "create table items (id text primary key, n int, name text, updated text)"


def batch(
    records: list[ApplyRecord],
    *,
    mode: Any = "upsert",
    chunk: int = 0,
    first: bool = True,
    last: bool = True,
    partition: str = "*",
    version: str = "v1",
) -> ApplyBatch:
    return ApplyBatch("r", version, chunk, partition, mode, records, f"r:{version}:{chunk}:0", first, last)


def up(key: Any, **payload: Any) -> ApplyRecord:
    return ApplyRecord(key, "upsert", payload)


def rows(conn: psycopg.Connection[Any], table: str = "items", order: str = "id") -> list[tuple[Any, ...]]:
    return conn.execute(
        sql.SQL("select * from {} order by {}").format(sql.Identifier(table), sql.Identifier(order))
    ).fetchall()


@pytest.mark.postgres
class TestQuery(SourceConformance):
    @pytest.fixture
    def ctx(self, make_ctx: Callable[..., Context]) -> Context:
        return make_ctx()

    @pytest.fixture
    def source(self, target: psycopg.Connection[Any]) -> Any:
        target.execute(ITEMS)
        return postgres.Query("wh", table="items")

    @pytest.fixture
    def seed(self, target: psycopg.Connection[Any]) -> Callable[[list[dict[str, Any]]], None]:
        def load(seeded: list[dict[str, Any]]) -> None:
            target.execute("truncate items")
            with (
                target.cursor() as cursor,
                cursor.copy("copy items (id, n, name, updated) from stdin") as copy,
            ):
                for row in seeded:
                    copy.write_row((row["id"], row["n"], row["name"], row["updated"]))

        return load

    @pytest.fixture
    def permanent_fault(self, target: psycopg.Connection[Any]) -> Callable[[], None]:
        def drop() -> None:
            target.execute("drop table items")

        return drop


@pytest.mark.postgres
class TestTable(SinkConformance):
    @pytest.fixture
    def ctx(self, make_ctx: Callable[..., Context]) -> Context:
        return make_ctx()

    @pytest.fixture
    def sink(self, target: psycopg.Connection[Any]) -> Any:
        target.execute(ITEMS)
        return postgres.Table("wh", table="items", key_columns=["id"])

    @pytest.fixture
    def fresh_sink(self) -> Callable[[], Any]:
        return lambda: postgres.Table("wh", table="items", key_columns=["id"])

    @pytest.fixture
    def read_back(self, target: psycopg.Connection[Any]) -> Callable[[], Mapping[Any, Mapping[str, Any]]]:
        def back() -> Mapping[Any, Mapping[str, Any]]:
            with target.cursor(row_factory=dict_row) as cursor:
                found = cursor.execute("select id, n, name, updated from items").fetchall()
            return {row["id"]: row for row in found}

        return back

    @pytest.fixture
    def permanent_fault(self, target: psycopg.Connection[Any]) -> Callable[[], None]:
        def drop() -> None:
            target.execute("drop table items")

        return drop


@pytest.mark.postgres
def test_bounds_and_since_are_pushed_down_as_parameters(target, make_ctx):
    target.execute("create table sales (id int, store text, day date, updated timestamptz)")
    target.execute(
        """insert into sales values (1, 'a', '2026-11-06', '2026-11-06 10:00Z'), (2, 'a', '2026-11-07', '2026-11-07 09:00Z'),
           (3, 'b', '2026-11-07', '2026-11-07 11:00Z'), (4, 'a', '2026-11-07', '2026-11-07 12:00Z')"""
    )
    source = postgres.Query("wh", table="sales")
    req = ReadRequest(
        "day=2026-11-07",
        fc.partition.by_day("day", tz="UTC"),
        since=datetime(2026, 11, 7, 10, tzinfo=UTC),
        stamp_field="updated",
    )
    got = sorted(row["id"] for batch in source.read(make_ctx(), req) for row in batch)
    assert got == [3, 4]
    statement, params = source.last_statement
    assert '"day" >= %s and "day" < %s' in statement and '"updated" >= %s' in statement
    assert "2026" not in statement and len(params) == 3
    fields = ReadRequest("store=b", fc.partition.by_fields("store"))
    assert [row["id"] for batch in source.read(make_ctx(), fields) for row in batch] == [3]


@pytest.mark.postgres
def test_server_side_cursor_is_used_for_large_reads(target, make_ctx):
    target.execute("create table big as select g as id from generate_series(1, 250) g")
    source = postgres.Query("wh", sql="select id from big order by id", batch_rows=100)
    sizes = [len(batch) for batch in source.read(make_ctx(), ReadRequest("*", fc.partition.none()))]
    assert sizes == [100, 100, 50]


@pytest.mark.postgres
def test_upsert_on_composite_key(target, make_ctx):
    target.execute("create table lines (order_id int, line int, qty int, primary key (order_id, line))")
    sink = postgres.Table("wh", table="lines", key_columns=["order_id", "line"])
    sink.apply(
        make_ctx(), batch([up((1, 1), order_id=1, line=1, qty=2), up((1, 2), order_id=1, line=2, qty=5)])
    )
    sink.apply(make_ctx(), batch([up((1, 1), order_id=1, line=1, qty=3)], version="v2"))
    sink.apply(make_ctx(), batch([ApplyRecord((1, 2), "delete", None)], version="v3"))
    found = target.execute("select order_id, line, qty, _fc_key, _fc_op from lines").fetchall()
    assert found == [(1, 1, 3, "[1,1]", "upsert")]


@pytest.mark.postgres
def test_append_keeps_fc_columns(target, make_ctx):
    target.execute("create table events (id text, kind text)")
    sink = postgres.Table("wh", table="events", key_columns=["id"])
    sink.apply(make_ctx(), batch([up("e1", id="e1", kind="a"), up("e1", id="e1", kind="b")], mode="append"))
    found = target.execute(
        "select id, kind, _fc_key, _fc_partition, _fc_version_id, _fc_op, _fc_applied_at is not null from events"
        " order by kind"
    ).fetchall()
    assert found == [
        ("e1", "a", "e1", "*", "v1", "upsert", True),
        ("e1", "b", "e1", "*", "v1", "upsert", True),
    ]
    applied = target.execute("select idempotency_key, applied from _feather_applied").fetchall()
    assert applied == [("r:v1:0:0", 2)]


@pytest.mark.postgres
def test_replace_partition_is_atomic_across_chunks(target, make_ctx):
    target.execute("create table daily (id text primary key, day text, v int)")
    sink = postgres.Table("wh", table="daily", key_columns=["id"])
    ctx = make_ctx()
    old = [up("a", id="a", day="d1", v=1), up("b", id="b", day="d1", v=1)]
    sink.apply(ctx, batch(old, mode="replace_partition", partition="day=d1"))
    assert [r[0] for r in rows(target, "daily")] == ["a", "b"]
    new: dict[str, Any] = {"mode": "replace_partition", "partition": "day=d1", "version": "v2"}
    sink.apply(ctx, batch([up("c", id="c", day="d1", v=2)], chunk=0, first=True, last=False, **new))
    with pytest.raises(PermanentError, match="no column"):  # chunk 2 fails: the old slice is untouched
        sink.apply(ctx, batch([up("d", id="d", day="d1", nope=1)], chunk=1, first=False, last=True, **new))
    assert [r[0] for r in rows(target, "daily")] == ["a", "b"]
    sink.apply(ctx, batch([up("d", id="d", day="d1", v=2)], chunk=1, first=False, last=True, **new))
    assert [(r[0], r[2]) for r in rows(target, "daily")] == [("c", 2), ("d", 2)]
    stages = target.execute(
        "select count(*) from pg_tables where tablename like '_feather_stage_%'"
    ).fetchone()
    assert stages == (0,)
    sink.apply(
        ctx, batch([], mode="replace_partition", partition="day=d1", version="v3")
    )  # a slice now empty
    assert rows(target, "daily") == []


@pytest.mark.postgres
def test_moved_from_removes_the_old_row_in_replace_partition(target, make_ctx):
    target.execute("create table stock (sku text primary key, store text, qty int)")
    sink = postgres.Table("wh", table="stock", key_columns=["sku"])
    ctx = make_ctx()
    sink.apply(
        ctx, batch([up("s1", sku="s1", store="a", qty=1)], mode="replace_partition", partition="store=a")
    )
    moved = ApplyRecord("s1", "upsert", {"sku": "s1", "store": "b", "qty": 4}, moved_from="store=a")
    sink.apply(ctx, batch([moved], mode="replace_partition", partition="store=b", version="v2"))
    assert target.execute("select sku, store, qty, _fc_partition from stock").fetchall() == [
        ("s1", "b", 4, "store=b")
    ]


@pytest.mark.postgres
def test_identifiers_are_quoted(target, make_ctx):  # Review Focus 3
    target.execute('create table "order-lines" (id text primary key, "select" int)')
    sink = postgres.Table("wh", table="order-lines", key_columns=["id"])
    sink.apply(make_ctx(), batch([up("1", id="1", select=5)]))
    assert target.execute('select id, "select" from "order-lines"').fetchall() == [("1", 5)]
    odd = postgres.Table("wh", table="x; drop table items", key_columns=["id"])
    with pytest.raises(PermanentError, match="no table public.x; drop table items"):
        odd.apply(make_ctx(), batch([up("1", id="1")]))
    source = postgres.Query("wh", table="order-lines")
    assert [
        r["select"] for b in source.read(make_ctx(), ReadRequest("*", fc.partition.none())) for r in b
    ] == [5]


@pytest.mark.postgres
def test_unknown_column_fails_the_version_naming_it(target, make_ctx):  # Review Focus 3
    target.execute(ITEMS)
    sink = postgres.Table("wh", table="items", key_columns=["id"])
    with pytest.raises(PermanentError, match="no_such_col"):
        sink.apply(make_ctx(), batch([up("1", id="1", no_such_col=1)]))


@pytest.mark.postgres
def test_constraint_violation_on_one_row_rejects_only_that_row(target, make_ctx):
    target.execute(
        "create table priced (id text primary key, price int check (price >= 0), name text not null)"
    )
    sink = postgres.Table("wh", table="priced", key_columns=["id"])
    result = sink.apply(
        make_ctx(),
        batch(
            [
                up("1", id="1", price=5, name="a"),
                up("2", id="2", price=-1, name="b"),
                up("3", id="3", price=1, name="c"),
            ]
        ),
    )
    assert result.applied == 2 and [(r.key, r.error_class) for r in result.rejected] == [
        ("2", "CheckViolation")
    ]
    assert [r[0] for r in rows(target, "priced")] == ["1", "3"]
    again = sink.apply(
        make_ctx(), batch([up("1", id="1", price=99, name="z")])
    )  # the same chunk: the first answer
    assert (again.applied, [r.key for r in again.rejected]) == (2, ["2"])


def test_connection_error_is_transient():
    ctx = Context(
        {"dsn": "postgresql://nobody@127.0.0.1:1/none", "connect_timeout": 2},
        logging.getLogger("t"),
        datetime(2026, 11, 7, tzinfo=UTC),
        "UTC",
    )
    source = postgres.Query("wh", table="items")
    with pytest.raises(TransientError, match="connecting to 'wh'"):
        list(source.read(ctx, ReadRequest("*", fc.partition.none())))
    assert not source.check(ctx).ok


@pytest.mark.postgres
def test_readme_example_runs(target, target_url, tmp_path: Path, monkeypatch):
    monkeypatch.setenv("WAREHOUSE_DSN", target_url)
    names = run_readme_examples(Path(__file__).resolve().parents[1] / "README.md", tmp_path)
    flow = names["flow"]
    (route,) = flow.routes
    assert isinstance(route.sink, postgres.Table) and isinstance(flow.datasets[0].source, postgres.Query)


@pytest.mark.postgres
def test_staged_chunks_survive_a_server_crash(target, make_ctx):  # Review C2
    # An unlogged table is emptied by crash recovery, while its chunks are marked applied: it must be a logged one.
    target.execute("create table daily (id text primary key, day text, v int)")
    sink = postgres.Table("wh", table="daily", key_columns=["id"])
    sink.apply(
        make_ctx(),
        batch([up("c", id="c", day="d1", v=2)], mode="replace_partition", partition="day=d1", last=False),
    )
    persistence = target.execute(
        r"select relpersistence from pg_class where relname like '\_feather\_stage\_%'"
    ).fetchall()
    assert persistence == [("p",)]

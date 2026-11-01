import psycopg
import pytest

from feather_connect.storage.postgres.migrate import migrate

pytestmark = pytest.mark.postgres

EXPECTED_TABLES = {
    "tenant",
    "run",
    "task",
    "task_log",
    "project",
    "flow",
    "dataset",
    "route",
    "connection",
    "version",
    "run_slice",
    "spool_version",
    "spool_chunk",
    "spool_snapshot",
    "record_state",
    "partition_status",
    "delivery",
    "delivery_attempt",
    "quarantine",
    "inbox",
    "inbox_event_id",
    "override",
    "budget",
    "budget_window",
    "worker",
    "schedule",
}


def tables_in(url: str, schema: str) -> set[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            """select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace
               where n.nspname = %s and c.relkind in ('r', 'p')""",
            (schema,),
        ).fetchall()
    return {r[0] for r in rows}


def tables_without(url: str, schema: str, column: str, except_: set[str]) -> list[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            """select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace
               where n.nspname = %s and c.relkind in ('r', 'p')
                 and not exists (select 1 from pg_attribute a
                                 where a.attrelid = c.oid and a.attname = %s and not a.attisdropped)""",
            (schema, column),
        ).fetchall()
    return sorted(r[0] for r in rows if r[0] not in except_)


def extensions(url: str) -> set[str]:
    with psycopg.connect(url) as conn:
        return {r[0] for r in conn.execute("select extname from pg_extension").fetchall()}


def index_def(url: str, name: str) -> str:
    with psycopg.connect(url) as conn:
        row = conn.execute(
            "select indexdef from pg_indexes where schemaname = 'feather' and indexname = %s", (name,)
        ).fetchone()
    assert row is not None, f"no index {name}"
    return row[0]


def test_migrate_creates_all_tables_and_is_idempotent(pg_url: str):
    migrate(pg_url)
    migrate(pg_url)
    assert EXPECTED_TABLES <= tables_in(pg_url, "feather")


def test_every_table_has_tenant_id(pg_url: str):  # FR-I1
    migrate(pg_url)
    assert tables_without(pg_url, "feather", "tenant_id", except_={"alembic_version"}) == []


def test_no_extensions_are_needed(pg_url: str):  # NFR-07
    migrate(pg_url)
    assert extensions(pg_url) == {"plpgsql"}


def test_partial_index_on_open_deliveries_exists(pg_url: str):
    migrate(pg_url)
    assert "status <> 'applied'" in index_def(pg_url, "delivery_open_idx")

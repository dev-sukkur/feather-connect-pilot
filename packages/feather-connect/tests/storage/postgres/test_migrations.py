import psycopg
import pytest
from real_stack import scratch_database, server_url

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
    "registry",
    "registry_file",
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


def test_users_and_tokens_of_an_m1_database_become_admins(pg_url: str):  # flow packages §14 task 0
    with scratch_database(server_url()) as url:
        migrate(url, "0014")
        with psycopg.connect(url, autocommit=True) as conn:
            conn.execute(
                """insert into feather.app_user (tenant_id, user_id, username, password_hash, created_at)
                   values ('default', 'u1', 'ann', 'h', now()), ('t2', 'u2', 'bob', 'h', now())"""
            )
            conn.execute(
                """insert into feather.api_token (tenant_id, token_id, name, token_hash, created_at)
                   values ('default', 't1', 'ci', 'h1', now())"""
            )
        migrate(url)
        with psycopg.connect(url, autocommit=True) as conn:
            users = conn.execute("select username, role from feather.app_user order by username").fetchall()
            tokens = conn.execute("select name, role, created_by from feather.api_token").fetchall()
            assert users == [("ann", "admin"), ("bob", "admin")] and tokens == [("ci", "admin", None)]
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute("update feather.app_user set role = 'viewer' where username = 'ann'")
            with pytest.raises(psycopg.errors.NotNullViolation):  # a new row must say its role
                conn.execute(
                    """insert into feather.app_user (tenant_id, user_id, username, password_hash, created_at)
                       values ('default', 'u3', 'cy', 'h', now())"""
                )

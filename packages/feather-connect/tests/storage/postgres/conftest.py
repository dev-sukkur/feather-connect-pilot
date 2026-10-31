"""Fixtures for the tests that need Postgres. Start it with `docker compose -f deploy/compose.test.yaml up -d`."""

from collections.abc import Iterator

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool

from feather_connect.storage.postgres.migrate import migrate


@pytest.fixture(scope="session")
def pool(pg_url: str) -> Iterator[ConnectionPool[psycopg.Connection[DictRow]]]:
    migrate(pg_url)
    with ConnectionPool[psycopg.Connection[DictRow]](
        pg_url, min_size=1, max_size=12, kwargs={"row_factory": dict_row}, open=True
    ) as connection_pool:
        yield connection_pool


@pytest.fixture
def pg(
    pg_url: str, pool: ConnectionPool[psycopg.Connection[DictRow]]
) -> Iterator[psycopg.Connection[DictRow]]:
    """An autocommit connection to the migrated database, which is emptied first."""
    with psycopg.Connection[DictRow].connect(pg_url, autocommit=True, row_factory=dict_row) as conn:
        tables = conn.execute(
            """select c.relname from pg_class c join pg_namespace n on n.oid = c.relnamespace
               where n.nspname = 'feather' and c.relkind in ('r', 'p') and not c.relispartition
                 and c.relname <> 'alembic_version'"""
        ).fetchall()
        names = sql.SQL(", ").join(sql.Identifier("feather", t["relname"]) for t in tables)
        conn.execute(sql.SQL("truncate {} restart identity cascade").format(names))
        conn.execute("alter sequence feather.seal_seq restart")
        yield conn

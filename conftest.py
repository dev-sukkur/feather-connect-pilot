"""Fixtures shared by the tests that need Postgres. Start it with
`docker compose -f deploy/compose.test.yaml up -d`."""

import subprocess
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool
from real_stack import scratch_database, server_url, stop_gracefully


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    """A fresh, empty database on the test server, dropped when the session ends."""
    try:
        with scratch_database(server_url()) as url:
            yield url
    except psycopg.OperationalError as error:
        pytest.fail(
            f"the test Postgres is not reachable ({error}). "
            "Start it with: docker compose -f deploy/compose.test.yaml up -d",
            pytrace=False,
        )


@pytest.fixture(scope="session")
def pool(pg_url: str) -> Iterator[ConnectionPool[psycopg.Connection[DictRow]]]:
    """A pool on the migrated test database, for the stores and the server."""
    from feather_connect.storage.postgres.migrate import migrate

    migrate(pg_url)
    with ConnectionPool[psycopg.Connection[DictRow]](
        pg_url, min_size=1, max_size=12, kwargs={"row_factory": dict_row}, open=True
    ) as connection_pool:
        yield connection_pool


@pytest.fixture
def pg_connect() -> Iterator[Callable[..., tuple[Any, Any]]]:
    """`connect`, with what it opened closed when the test ends. A pool left open has four threads and a session for
    the rest of the run, because `connect` registers its close with `atexit`, which also keeps it from being collected.
    """
    from feather_connect.storage.postgres.connect import connect

    stores: list[Any] = []

    def opener(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
        store, spool = connect(*args, **kwargs)
        stores.append(store)
        return store, spool

    yield opener
    for store in stores:
        store.close()


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


@pytest.fixture
def db(pg_url: str) -> Iterator[tuple[Any, Any]]:
    """A store and a spool on the migrated test database."""
    from feather_connect.storage.postgres.connect import connect
    from feather_connect.storage.postgres.migrate import migrate

    migrate(pg_url)
    store, spool = connect(pg_url)
    yield store, spool
    store.close()


@pytest.fixture
def client_engine(pg_url: str) -> Iterator[Any]:
    """An engine to submit tasks with, as a server does."""
    from feather_engine_dbos import DbosEngine

    engine = DbosEngine(pg_url)
    yield engine
    engine.close()


@pytest.fixture
def workers() -> Iterator[list[subprocess.Popen[bytes]]]:
    """A list for a test to put its worker processes in. They are stopped when it ends."""
    procs: list[subprocess.Popen[bytes]] = []
    yield procs
    # Let each drain, since the next test shares the database. A run reads succeeded a moment before its last task's
    # workflow ends: killed in that moment, the workflow stays PENDING, and with no sweeper here it holds the one slot
    # of its concurrency key, so that the next test's delivery on the same route waits for good.
    for proc in procs:
        if proc.poll() is None:
            stop_gracefully(proc)
    for proc in procs:
        try:
            proc.wait(timeout=45)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

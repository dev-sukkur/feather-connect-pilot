"""Open a Postgres store and spool on one connection pool."""

import atexit

from psycopg.rows import dict_row

from feather_connect.clock import Clock
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.storage.postgres.support import Pool, session_options


def connect(
    database_url: str,
    *,
    clock: Clock | None = None,
    tenant: str = "default",
    min_size: int = 1,
    max_size: int = 10,
) -> tuple[PostgresStore, PostgresSpool]:
    """Return a store and a spool for `tenant` that share a pool of connections. The schema must be migrated.

    The pool closes with `store.close()`, or else when the process exits.
    """
    pool = Pool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        kwargs={"row_factory": dict_row, **session_options(database_url)},
        check=Pool.check_connection,
        open=True,
    )
    atexit.register(pool.close)
    return PostgresStore(pool, clock, tenant), PostgresSpool(pool, clock, tenant)

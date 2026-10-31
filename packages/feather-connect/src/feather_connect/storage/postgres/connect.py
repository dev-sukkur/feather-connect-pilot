"""Open a Postgres store and spool on one connection pool."""

import atexit

from psycopg.rows import dict_row

from feather_connect.clock import Clock
from feather_connect.storage.port import Store
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.storage.postgres.support import Pool


def connect(
    database_url: str,
    *,
    clock: Clock | None = None,
    tenant: str = "default",
    min_size: int = 1,
    max_size: int = 10,
) -> tuple[Store, PostgresSpool]:
    """Return a store and a spool for `tenant` that share a pool of connections. The schema must be migrated.

    The pool closes when the process exits.
    """
    pool = Pool(
        database_url, min_size=min_size, max_size=max_size, kwargs={"row_factory": dict_row}, open=True
    )
    atexit.register(pool.close)
    return PostgresStore(pool, clock, tenant), PostgresSpool(pool, clock, tenant)

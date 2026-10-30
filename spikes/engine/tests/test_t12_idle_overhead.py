import time

import psycopg
import pytest

from conftest import record
from spike.config import PG_URL

SQL_TX = "select sum(xact_commit + xact_rollback) from pg_stat_database"
SQL_CONNS = ("select count(*) from pg_stat_activity where backend_type = 'client backend'"
             " and pid <> pg_backend_pid()")


@pytest.mark.parametrize("n_workers", [1, 3])
def test_t12_idle_overhead(workers, engine_name, n_workers):
    """Extra measurement for the ops-footprint score: Postgres load from idle workers."""
    for i in range(n_workers):
        workers.start(f"w{i + 1}", slots=4)
    time.sleep(5)
    with psycopg.connect(PG_URL, autocommit=True) as c:
        tx0, t0 = c.execute(SQL_TX).fetchone()[0], time.monotonic()
        time.sleep(30)
        tx1, t1 = c.execute(SQL_TX).fetchone()[0], time.monotonic()
        conns = c.execute(SQL_CONNS).fetchone()[0]
    record(engine_name, f"T12 workers={n_workers}",
           idle_tx_per_s=round(float(tx1 - tx0) / (t1 - t0), 1), client_connections=conns)

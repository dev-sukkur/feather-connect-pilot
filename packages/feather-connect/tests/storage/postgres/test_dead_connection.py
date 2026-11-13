"""A store's transaction starts again on a new connection when its pooled one is gone before the first statement:
Postgres restarted, or the network or a proxy dropped it, after the pool's check."""

from collections.abc import Iterator

import psycopg
import pytest
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool

from feather_connect.clock import FrozenClock
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.testing.store_contract import T0

pytestmark = pytest.mark.postgres

Pg = psycopg.Connection[DictRow]


@pytest.fixture
def one(pg: Pg, pg_url: str) -> Iterator[ConnectionPool[Pg]]:
    """A pool of one connection with no check on checkout, so the next transaction gets the one this test kills."""
    with ConnectionPool[Pg](
        pg_url, min_size=1, max_size=1, kwargs={"row_factory": dict_row}, open=True
    ) as found:
        found.wait()
        yield found


def kill_the_pooled_connection(pg: Pg, pool: ConnectionPool[Pg]) -> None:
    with pool.connection() as conn:
        pid = conn.info.backend_pid
    pg.execute("select pg_terminate_backend(%s)", (pid,))


def test_a_transaction_on_a_dead_connection_starts_again_on_a_new_one(
    pg: Pg, one: ConnectionPool[Pg]
) -> None:
    store = PostgresStore(one, FrozenClock(T0))
    run_id = store.runs.create_run("f", "manual", "ann", {})
    kill_the_pooled_connection(pg, one)
    assert [run.run_id for run in store.runs.list_runs()] == [run_id]
    assert store.runs.get_run(run_id).flow == "f"  # and the pool has a working connection again

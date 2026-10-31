from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import psycopg
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

from feather_connect.clock import FrozenClock
from feather_connect.storage.port import BudgetSpec, Store, parse_limits
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.testing.store_contract import T0

pytestmark = pytest.mark.postgres

Pg = psycopg.Connection[DictRow]


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(T0)


@pytest.fixture
def store(pg: Pg, pool: ConnectionPool[Pg], clock: FrozenClock) -> Store:
    return PostgresStore(pool, clock)


def test_threads_never_exceed_the_window(store: Store):
    store.budgets.define(BudgetSpec("z", parse_limits(["100/minute"])))
    with ThreadPoolExecutor(8) as pool:
        grants = list(pool.map(lambda _: store.budgets.acquire("z", 1).granted, range(400)))
    assert sum(grants) == 100


def test_all_windows_update_together_or_not_at_all(store: Store):
    store.budgets.define(BudgetSpec("z", parse_limits(["10/minute", "3/day"])))
    with ThreadPoolExecutor(8) as pool:
        grants = list(pool.map(lambda _: store.budgets.acquire("z", 2).granted, range(8)))
    assert sum(grants) == 1  # the day window holds one grant of 2
    assert [u.used for u in store.budgets.usage("z")] == [
        2,
        2,
    ]  # the minute window was spared by the refusals


def test_budget_survives_a_new_store_instance(pg: Pg, pg_url: str, clock: FrozenClock):
    spec = BudgetSpec("z", parse_limits(["10/minute"]))
    first, _ = connect(pg_url, clock=clock, max_size=2)
    first.budgets.define(spec)
    first.budgets.acquire("z", 7)
    second, _ = connect(pg_url, clock=clock, max_size=2)
    second.budgets.define(spec)  # defining it again keeps what was used
    assert [u.used for u in second.budgets.usage("z")] == [7]
    clock.advance(timedelta(seconds=30))
    assert second.budgets.acquire("z", 8).granted  # three left, five refilled
    assert not first.budgets.acquire("z", 1).granted

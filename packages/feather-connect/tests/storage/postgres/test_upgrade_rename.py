"""The upgrade from the project mode renames an M1 database's history to the package's names (flow packages §13)."""

import psycopg
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

from feather_connect.clock import FrozenClock
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.storage.postgres.upgrade import UpgradeError, rename_to_package
from feather_connect.testing.store_contract import T0, seal

pytestmark = pytest.mark.postgres
Pg = psycopg.Connection[DictRow]
NAMES = {"flow": {"orders": "acme/orders"}, "dataset": {"shop.items": "acme/shop.items"}, "route": {"lake": "acme/lake"}}


@pytest.fixture
def store(pg: Pg, pool: ConnectionPool[Pg]) -> PostgresStore:
    return PostgresStore(pool, FrozenClock(T0))


def _m1_history(store: PostgresStore) -> tuple[str, str]:
    done = store.runs.create_run("orders", "schedule", "scheduler", {}, status="succeeded")
    queued = store.runs.create_run("orders", "manual", "ann", {})
    store.schedules.advance_tick("orders", "t1", None, T0)
    seal(store, "v1", ["lake"], ds="shop.items", run=done)
    return done, queued


def test_the_history_takes_the_package_names_and_running_it_again_changes_nothing(
    store: PostgresStore, pg_url: str, pg: Pg
) -> None:
    done, queued = _m1_history(store)
    counts = rename_to_package(pg_url, "acme", "1.0.0", NAMES, T0)
    assert counts["run.flow"] == 2 and counts["schedule.flow"] == 1
    assert counts["version.dataset"] == 1 and counts["delivery.route"] == 1
    assert counts["tagged"] == 2  # the queued run, and the one whose delivery isn't applied yet
    assert {r.flow for r in store.runs.list_runs()} == {"acme/orders"}
    tagged = store.runs.get_run(queued)
    assert tagged is not None and dict(tagged.params) == {"package": "acme", "version": "1.0.0"}
    assert store.packages.renames() == {
        ("flow", "orders"): "acme/orders",
        ("dataset", "shop.items"): "acme/shop.items",
        ("route", "lake"): "acme/lake",
    }
    again = rename_to_package(pg_url, "acme", "1.0.0", NAMES, T0)
    assert not any(again.values())  # nothing left to rename: safe to run twice


def test_a_name_the_package_has_already_is_refused_and_nothing_changes(store: PostgresStore, pg_url: str) -> None:
    store.runs.create_run("orders", "manual", "ann", {})
    store.schedules.advance_tick("orders", "t1", None, T0)
    store.schedules.advance_tick("acme/orders", "t1", None, T0)  # the package's flow ticked already
    with pytest.raises(UpgradeError, match="already"):
        rename_to_package(pg_url, "acme", "1.0.0", NAMES, T0)
    assert {r.flow for r in store.runs.list_runs()} == {"orders"}  # one transaction: all or nothing
    assert store.packages.renames() == {}

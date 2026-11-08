import secrets
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool, PoolTimeout

from feather_connect.clock import FrozenClock
from feather_connect.core.connector import InboundRequest
from feather_connect.core.records import Record
from feather_connect.storage.port import InvalidTransition, StaleWriter, StateRow, Store
from feather_connect.storage.postgres import support
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.leader import PostgresLeaderLock
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.postgres.store import OLDEST_OPEN_SQL, PostgresStore
from feather_connect.storage.spool import Spool, records_to_table
from feather_connect.testing.store_contract import T0, StoreContract, draft, seal

pytestmark = pytest.mark.postgres

Pg = psycopg.Connection[DictRow]


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(T0)


@pytest.fixture
def store(pg: Pg, pool: ConnectionPool[Pg], clock: FrozenClock) -> Store:
    return PostgresStore(pool, clock)


@pytest.fixture
def spool(pg: Pg, pool: ConnectionPool[Pg], clock: FrozenClock) -> Spool:
    return PostgresSpool(pool, clock)


class TestPostgresStore(StoreContract):
    pass


def in_threads(n: int, fn: Callable[[int], Any]) -> list[Any]:
    """Run `fn` in `n` threads at once. Each result is the return value, or the exception that was raised."""

    def run(i: int) -> Any:
        try:
            return fn(i)
        except Exception as error:  # noqa: BLE001
            return error

    with ThreadPoolExecutor(n) as threads:
        return list(threads.map(run, range(n)))


def test_seal_is_one_transaction(store: Store, pg: Pg):
    pg.execute(
        """create function feather.fail_on_boom() returns trigger language plpgsql as $body$
           begin if new.route = 'boom' then raise exception 'boom'; end if; return new; end $body$"""
    )
    pg.execute(
        "create trigger fail_on_boom before insert on feather.delivery for each row"
        " execute function feather.fail_on_boom()"
    )
    try:
        store.versions.stage(draft())
        with pytest.raises(psycopg.errors.RaiseException):
            store.versions.seal(["v1"], ["a", "boom"])  # the second delivery fails
        assert store.ledger.list_deliveries() == []
        assert store.versions.get("v1").status == "staged"
    finally:
        pg.execute("drop trigger fail_on_boom on feather.delivery")
        pg.execute("drop function feather.fail_on_boom()")


def test_concurrent_seal_assigns_one_sequence(store: Store):
    store.versions.stage(draft())
    results = in_threads(8, lambda _: store.versions.seal(["v1"], ["a", "b"])[0].seal_seq)
    assert len(set(results)) == 1 and isinstance(results[0], int)
    assert sorted((d.route, d.version_id) for d in store.ledger.list_deliveries()) == [
        ("a", "v1"),
        ("b", "v1"),
    ]


def test_concurrent_transition_applies_once(store: Store):  # I3
    seal(store, "v1", ["a"])
    store.ledger.transition("a", "v1", "running", actor="t")
    results = in_threads(
        8, lambda _: store.ledger.transition("a", "v1", "applied", actor="t", rows_applied=1)
    )
    assert sum(isinstance(r, InvalidTransition) for r in results) == 7
    assert [r.status for r in results if not isinstance(r, Exception)] == ["applied"]
    assert len(store.ledger.attempts("a", "v1")) == 2  # running, applied


def test_concurrent_finish_slice_reports_completion_once(store: Store):
    store.versions.register_slices("r1", "d", [f"p{i}" for i in range(8)])
    results = in_threads(8, lambda i: store.versions.finish_slice("r1", "d", f"p{i}"))
    assert results.count(True) == 1 and results.count(False) == 7


def test_tenant_setting_is_set_per_transaction(store: Store, pg: Pg):
    pg.execute(
        """create function feather.tenant_to_reason() returns trigger language plpgsql as $body$
           begin new.reason = current_setting('feather.tenant_id'); return new; end $body$"""
    )
    pg.execute(
        "create trigger tenant_to_reason before insert on feather.run for each row"
        " execute function feather.tenant_to_reason()"
    )
    try:
        run_id = store.runs.create_run("f", "manual", "ann", {})
        assert store.runs.get_run(run_id).reason == "default"
    finally:
        pg.execute("drop trigger tenant_to_reason on feather.run")
        pg.execute("drop function feather.tenant_to_reason()")


def test_tenants_do_not_see_each_other(pool: ConnectionPool[Pg], pg: Pg, clock: FrozenClock):
    mine, other = PostgresStore(pool, clock, "mine"), PostgresStore(pool, clock, "other")
    seal(mine, "v1", ["a"])
    run_id = mine.runs.create_run("f", "manual", "ann", {})
    assert other.versions.list_versions("d") == [] and other.ledger.list_deliveries() == []
    assert other.runs.list_runs() == []
    with pytest.raises(KeyError):
        other.runs.get_run(run_id)


def test_oldest_open_uses_the_partial_index(store: Store, pg: Pg):
    pg.execute(
        """insert into feather.delivery (tenant_id, route, version_id, dataset, partition, seal_seq, status)
           select 'default', 'a', 'v' || i, 'd', 'p', i, 'applied' from generate_series(1, 10000) i"""
    )
    pg.execute(
        """insert into feather.delivery (tenant_id, route, version_id, dataset, partition, seal_seq, status)
           values ('default', 'a', 'open', 'd', 'p', 10001, 'pending')"""
    )
    pg.execute("analyze feather.delivery")
    params = {"tenant": "default", "route": "a", "partition": "p", "closed": ["applied", "skipped"]}
    plan = "\n".join(
        next(iter(r.values())) for r in pg.execute("explain " + OLDEST_OPEN_SQL, params).fetchall()
    )
    assert "delivery_open_idx" in plan
    oldest = store.ledger.oldest_open("a", "p")
    assert oldest is not None and oldest.version_id == "open"


def test_task_logs_and_attempts_land_in_the_partition_of_their_month(store: Store, clock: FrozenClock):
    rid = store.runs.create_run("f", "manual", "ann", {})
    task = store.runs.start_task(rid, "apply", "ref")
    seal(store, "v1", ["a"])
    store.runs.log(task, "info", "this month")
    clock.advance(timedelta(days=70))  # two months on
    store.runs.log(task, "info", "later")
    store.ledger.transition("a", "v1", "running", actor="t")
    assert store.runs.task_logs(task) == ["info: this month", "info: later"]
    assert store.ledger.attempts("a", "v1")[0].at == T0 + timedelta(days=70)


def test_datetime_watermarks_compare_as_datetimes(store: Store):
    early, late = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
    seal(store, "v1", ["a"], wm=early)
    seal(store, "v2", ["a"], wm=late)
    assert store.versions.latest_watermark("d") == late
    assert [v.version_id for v in store.versions.since_watermark("d", "p", late)] == ["v2"]


def test_connect_opens_a_store_and_a_spool_on_one_pool(
    pg: Pg, pg_url: str, clock: FrozenClock, pg_connect: Any
):
    store, spool = pg_connect(pg_url, clock=clock, tenant="t1", max_size=2)
    run_id = store.runs.create_run("f", "manual", "ann", {"x": 1})
    spool.put_chunk("v1", 0, "data", records_to_table([Record("a", "upsert", {"x": 1}, 1)]))
    assert store.runs.get_run(run_id).params == {"x": 1} and len(spool.list_chunks("v1")) == 1


def pool_of(store: Store) -> Any:
    return store.runs._db.pool  # type: ignore[attr-defined]


def test_pool_sessions_have_timeouts_and_keepalives(
    pg: Pg, pg_url: str, clock: FrozenClock, pg_connect: Any
):  # F9
    store, _ = pg_connect(pg_url, clock=clock, max_size=1)
    with pool_of(store).connection() as conn:
        given = conn.info.get_parameters()
        assert given["connect_timeout"] == str(support.CONNECT_TIMEOUT_S)
        assert given["keepalives"] == "1"
        assert given["keepalives_idle"] == str(support.KEEPALIVES_IDLE_S)
        assert given["application_name"] == "feather"
        shown = conn.execute("show statement_timeout").fetchone()
        assert shown == {"statement_timeout": "2min"}


def test_the_statement_timeout_limits_a_statement_and_not_a_transaction(
    pg: Pg, pg_url: str, clock: FrozenClock, pg_connect: Any, monkeypatch: pytest.MonkeyPatch
):  # F9
    monkeypatch.setattr(support, "STATEMENT_TIMEOUT_MS", 400)
    store, _ = pg_connect(pg_url, clock=clock, max_size=1)
    pool = pool_of(store)
    with pool.connection() as conn:  # longer than the limit in all, in short statements
        for _ in range(3):
            conn.execute("select pg_sleep(0.2)")
    with pytest.raises(psycopg.errors.QueryCanceled), pool.connection() as conn:
        conn.execute("select pg_sleep(2)")


def test_options_in_the_url_are_kept(pg: Pg, pg_url: str, clock: FrozenClock, pg_connect: Any):  # F9
    url = f"{pg_url}?options=-c%20search_path%3Dfeather"
    store, _ = pg_connect(url, clock=clock, max_size=1)
    with pool_of(store).connection() as conn:
        assert conn.execute("show search_path").fetchone() == {"search_path": "feather"}
        assert conn.execute("show statement_timeout").fetchone() == {"statement_timeout": "2min"}


def test_the_leader_lock_session_has_the_same_settings(pg_url: str):  # F9
    lock = PostgresLeaderLock(pg_url, 7_262_410_909)
    assert lock.try_acquire()
    try:
        session = lock._conn
        assert session is not None
        assert session.info.get_parameters()["keepalives_idle"] == str(support.KEEPALIVES_IDLE_S)
        assert session.execute("show statement_timeout").fetchone() == ("2min",)
    finally:
        lock.release()


def test_closing_a_store_closes_its_pool(pg: Pg, pg_url: str, clock: FrozenClock):  # F10
    store, _ = connect(pg_url, clock=clock, max_size=1)
    pool = pool_of(store)
    store.close()
    assert pool.closed
    store.close()  # closing again is harmless


def test_seal_batch_undoes_all_of_it_when_the_last_write_fails(store: Store, pg: Pg):
    """The events, the version and the deliveries are one transaction, whichever write it is that fails."""
    store.inbox.put("d", InboundRequest("POST", "/", {}, b"{}", T0))
    [event] = store.inbox.take("d", 1)
    pg.execute(
        """create function feather.refuse() returns trigger language plpgsql as
           $$ begin if new.route = 'second' then raise exception 'refused'; end if; return new; end $$"""
    )
    pg.execute(
        "create trigger refuse before insert on feather.delivery for each row execute function feather.refuse()"
    )
    try:
        with pytest.raises(psycopg.errors.RaiseException):
            store.inbox.seal_batch([event.event_seq], [draft("v1", complete=False)], ["first", "second"])
    finally:
        pg.execute("drop trigger refuse on feather.delivery")
        pg.execute("drop function feather.refuse()")
    assert store.inbox.take("d", 1) == [event]
    with pytest.raises(KeyError):
        store.versions.get("v1")
    assert store.ledger.list_deliveries() == []


def test_inbox_is_partitioned_by_day_a_week_ahead(store: Store, pg: Pg, clock: FrozenClock):
    store.inbox.ensure_partitions()
    days = store.inbox.days()
    assert all(T0.date() + timedelta(days=n) in days for n in range(8))
    clock.advance(timedelta(days=30))  # a day nobody made room for
    store.inbox.put("d", InboundRequest("POST", "/", {}, b"{}", clock.now()))
    assert clock.now().date() in store.inbox.days()
    plan = pg.execute(
        "explain select * from feather.inbox where tenant_id = 'default' and day = %s", (T0.date(),)
    ).fetchall()
    assert "inbox_20261018" in str(plan) and "inbox_20261117" not in str(plan)


def test_inbox_keeps_tenants_apart(pool: ConnectionPool[Pg], pg: Pg, clock: FrozenClock):
    mine, other = PostgresStore(pool, clock, "mine"), PostgresStore(pool, clock, "other")
    request = InboundRequest("POST", "/", {}, b"{}", T0)
    assert mine.inbox.put("d", request, "e1") and other.inbox.put("d", request, "e1")
    [event] = mine.inbox.take("d", 10)
    with pytest.raises(StaleWriter):
        other.inbox.seal_batch([event.event_seq], [], [])
    other.inbox.seal_batch([other.inbox.take("d", 1)[0].event_seq], [], [])
    assert mine.inbox.take("d", 10) == [event]
    assert other.inbox.drop_day(T0.date()) is False  # the partition holds the other tenant's event too


def test_inbox_event_ids_hold_across_threads(store: Store):
    results = in_threads(
        8, lambda n: store.inbox.put("d", InboundRequest("POST", "/", {}, b"{}", T0), "same")
    )
    assert sorted(results) == [False] * 7 + [True] and len(store.inbox.take("d", 10)) == 1


def test_a_row_too_big_for_an_index_is_not_an_outage(store: Store):
    with pytest.raises(psycopg.errors.ProgramLimitExceeded) as found:
        store.state.put_many("d", [StateRow(secrets.token_urlsafe(2400), "_", 1, {}, "v")])
    assert not store.is_unavailable(found.value)
    assert store.is_unavailable(psycopg.OperationalError("connection lost"))
    assert store.is_unavailable(PoolTimeout("no connection in 30 s"))
    assert not store.is_unavailable(psycopg.errors.UniqueViolation("duplicate key"))


def test_the_key_index_is_written_in_the_seals_own_transaction(store: PostgresStore, spool: Spool):
    """A seal that rolls back leaves no key behind, and one that commits leaves every key: the index is never out of
    step with the versions."""
    spool.bind(store.versions)
    spool.put_chunk("v1", 0, "data", records_to_table([Record("k", "upsert", {"x": 1}, 1)]))
    store.versions.stage(draft("v1"))
    db = store.runs._db  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError), db.tx() as conn:
        store.versions.seal_in(conn, ["v1"], ["a"], None)  # type: ignore[attr-defined]
        assert conn.execute("select count(*) as n from feather.record_key").fetchone()["n"] == 1
        raise RuntimeError("the caller's transaction fails after the seal")
    assert store.versions.get("v1").status == "staged"
    assert store.versions.key_history("d", "k") == []
    store.versions.seal(["v1"], ["a"])
    assert [s.version_id for s in store.versions.key_history("d", "k")] == ["v1"]


def test_a_row_hash_above_the_signed_range_comes_back_whole(store: PostgresStore, spool: Spool):
    spool.bind(store.versions)
    big = 2**64 - 5
    spool.put_chunk("v1", 0, "data", records_to_table([Record("k", "upsert", {"x": 1}, big)]))
    seal(store, "v1", ["a"])
    assert store.versions.key_history("d", "k")[0].row_hash == big

from datetime import datetime, timedelta

import psycopg
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import InboundRequest
from feather_connect.core.records import Record
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.retention import kept_days, kept_inbox_days, retention_once
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import InboxDay, Store, VersionDraft
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.storage.spool import MemorySpool, Spool, records_to_table, snapshot_table
from feather_connect.testing.store_contract import T0


def stage(store: Store, spool: Spool, version_id: str, partition: str = "p") -> None:
    store.versions.stage(VersionDraft(version_id, "d", partition, "r1", True, 1, 0, "h", None, None, "sha"))
    spool.put_chunk(version_id, 0, "data", records_to_table([Record("a", "upsert", {"x": 1}, 1)]))
    spool.put_chunk(version_id, 0, "snapshot", snapshot_table([("a", 1)]))


def stage_and_seal(
    store: Store, spool: Spool, version_id: str, routes: list[str], partition: str = "p"
) -> None:
    stage(store, spool, version_id, partition)
    store.versions.seal([version_id], routes)


def apply(store: Store, version_id: str, route: str = "a") -> None:
    store.ledger.transition(route, version_id, "running", actor="t")
    store.ledger.transition(route, version_id, "applied", actor="t")


class RetentionTests:
    """The retention tests, for any backend that provides `store`, `spool` and `clock`."""

    def test_retention_drops_a_day_with_nothing_open(self, store: Store, spool: Spool, clock: FrozenClock):
        stage_and_seal(store, spool, "v1", ["a"])
        apply(store, "v1")
        clock.advance(timedelta(days=30))
        r = retention_once(store, spool, now=clock.now())
        assert r.dropped_days == [T0.date()] and r.kept_days == {}
        assert spool.list_chunks("v1") == [] and spool.days() == []

    def test_retention_keeps_recent_days(self, store: Store, spool: Spool, clock: FrozenClock):
        stage_and_seal(store, spool, "v1", ["a"])
        apply(store, "v1")
        clock.advance(timedelta(days=13))
        assert retention_once(store, spool, now=clock.now()).dropped_days == []
        assert len(spool.list_chunks("v1")) == 1
        clock.advance(timedelta(days=2))
        assert retention_once(store, spool, now=clock.now(), keep_days=30).dropped_days == []

    def test_retention_keeps_a_day_with_an_open_delivery(  # Review Focus 1
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        stage_and_seal(store, spool, "v1", ["a", "b"])
        apply(store, "v1")  # route b has not
        clock.advance(timedelta(days=30))
        r = retention_once(store, spool, now=clock.now())
        assert r.dropped_days == [] and "open delivery" in r.kept_days[T0.date()]
        assert len(spool.list_chunks("v1")) == 1
        apply(store, "v1", "b")
        assert retention_once(store, spool, now=clock.now()).dropped_days == [T0.date()]

    def test_retention_keeps_a_day_with_a_version_not_yet_sealed(
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        stage(store, spool, "v1")
        clock.advance(timedelta(days=30))
        r = retention_once(store, spool, now=clock.now())
        assert r.dropped_days == [] and "not sealed" in r.kept_days[T0.date()]

    def test_kept_days_says_what_a_pass_would_keep_and_changes_nothing(  # F14
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        stage(store, spool, "v1")
        clock.advance(timedelta(days=30))
        found = kept_days(store, spool, now=clock.now())
        assert "not sealed" in found[T0.date()]
        assert len(spool.list_chunks("v1")) == 1
        assert retention_once(store, spool, now=clock.now()).kept_days == found

    def test_retention_keeps_the_latest_snapshot_of_each_partition(
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        for version_id, partition in (("v1", "p"), ("v2", "p"), ("v3", "q")):
            stage_and_seal(store, spool, version_id, ["a"], partition)
            apply(store, version_id)
        clock.advance(timedelta(days=30))
        retention_once(store, spool, now=clock.now())
        assert spool.snapshot_versions() == ["v2", "v3"]
        retention_once(store, spool, now=clock.now() + timedelta(days=300))
        assert spool.snapshot_versions() == ["v2", "v3"]  # nothing newer replaces them

    def test_retention_keeps_the_snapshot_of_a_delivery_that_is_still_open(
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        for version_id in ("v1", "v2"):
            stage_and_seal(store, spool, version_id, ["a"])
        clock.advance(timedelta(days=30))
        retention_once(store, spool, now=clock.now())
        assert spool.snapshot_versions() == ["v1", "v2"]  # v1 still has a delivery to make

    def test_retention_removes_old_runs_and_logs(self, store: Store, spool: Spool, clock: FrozenClock):
        run = store.runs.create_run("f", "manual", "t", {})
        task = store.runs.start_task(run, "plan", "plan:1")
        store.runs.log(task, "info", "old")
        store.runs.finish_task(task, "succeeded")
        store.runs.set_run_status(run, "succeeded")
        clock.advance(timedelta(days=31))
        r = retention_once(store, spool, now=clock.now())
        assert r.deleted_runs == 0 and r.deleted_logs == 1
        assert store.runs.task_logs(task) == [] and store.runs.get_run(run).status == "succeeded"
        clock.advance(timedelta(days=150))
        assert retention_once(store, spool, now=clock.now()).deleted_runs == 1
        assert store.runs.list_runs() == [] and store.runs.list_tasks(run) == []

    def test_retention_removes_old_deliveries_and_manifests(
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        for version_id in ("v1", "v2"):
            stage_and_seal(store, spool, version_id, ["a"])
            apply(store, version_id)
        clock.advance(timedelta(days=181))
        r = retention_once(store, spool, now=clock.now())
        assert r.deleted_deliveries == 2 and r.deleted_versions == 0
        clock.advance(timedelta(days=220))
        r = retention_once(store, spool, now=clock.now())
        assert r.deleted_versions == 1  # v1; v2 is the newest of its partition
        with pytest.raises(KeyError):
            store.versions.get("v1")
        assert store.versions.get("v2").status == "sealed"
        assert spool.snapshot_versions() == ["v2"]

    def test_retention_is_safe_to_repeat(self, store: Store, spool: Spool, clock: FrozenClock):
        stage_and_seal(store, spool, "v1", ["a"])
        apply(store, "v1")
        clock.advance(timedelta(days=30))
        retention_once(store, spool, now=clock.now())
        again = retention_once(store, spool, now=clock.now())
        assert again.dropped_days == [] and again.kept_days == {} and again.deleted_runs == 0

    def test_the_next_snapshot_diff_run_finds_no_changes_after_30_days(  # Review Focus 2
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        engine = LocalEngine(clock)
        source = Source(rows=[{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        sink = Sink("a")
        dataset = fc.Dataset("d", source=source, key="id", strategy=fc.strategy.snapshot_diff())
        flow = fc.Flow("f", reads=dataset, deliver=[fc.to(sink)])
        runtime = Runtime([flow], store, spool, engine, code_sha="sha", clock=clock)
        engine.register(runtime.tasks())

        def run() -> str:
            run_id = runtime.start_run("f")
            engine.run_until_idle()
            return run_id

        run()
        source.load([{"id": 1, "v": "a"}, {"id": 2, "v": "changed"}])
        second = run()
        clock.advance(timedelta(days=30))
        retention_once(store, spool, now=clock.now())
        assert len(spool.snapshot_versions()) == 1  # the first run's snapshot went
        third = run()
        assert store.runs.get_run(third).status == "succeeded"
        assert store.versions.sealed_for_run(second, "d") != []
        assert store.versions.sealed_for_run(third, "d") == []  # no changes, so no version
        assert len(sink.calls) == 2

    # The inbox.

    def _event(self, store: Store, at: datetime) -> int:
        store.inbox.put("d", InboundRequest("POST", "/", {}, b"{}", at))
        return store.inbox.take("d", 1000)[-1].event_seq

    def test_an_unsealed_event_keeps_its_day_for_as_long_as_it_takes(  # the promise of ingress
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        seq = self._event(store, T0)
        clock.advance(timedelta(days=400))
        r = retention_once(store, spool, now=clock.now())
        assert r.dropped_inbox_days == []
        assert f"1 events not sealed, such as event {seq}" in r.kept_inbox_days[T0.date()]
        assert kept_inbox_days(store, now=clock.now()) == r.kept_inbox_days
        assert [e.event_seq for e in store.inbox.take("d", 10)] == [seq]

    def test_one_unsealed_event_keeps_a_day_of_sealed_ones(
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        first = self._event(store, T0)
        second = self._event(store, T0)
        store.inbox.seal_batch([first], [], [])
        clock.advance(timedelta(days=30))
        r = retention_once(store, spool, now=clock.now())
        assert r.dropped_inbox_days == [] and str(second) in r.kept_inbox_days[T0.date()]

    def test_a_sealed_day_goes_seven_days_after_sealing_not_after_arriving(
        self, store: Store, spool: Spool, clock: FrozenClock
    ):
        seq = self._event(store, T0)
        clock.advance(timedelta(days=10))
        store.inbox.seal_batch([seq], [], [])  # a long outage: sealed ten days late
        clock.advance(timedelta(days=6))
        r = retention_once(store, spool, now=clock.now())
        assert r.dropped_inbox_days == [] and r.kept_inbox_days == {}
        assert T0.date() in store.inbox.days()
        clock.advance(timedelta(days=2))
        assert retention_once(store, spool, now=clock.now()).dropped_inbox_days == [T0.date()]
        assert T0.date() not in store.inbox.days()

    def test_a_recent_day_stays(self, store: Store, spool: Spool, clock: FrozenClock):
        store.inbox.seal_batch([self._event(store, T0)], [], [])
        clock.advance(timedelta(days=6))
        assert retention_once(store, spool, now=clock.now()).dropped_inbox_days == []
        assert T0.date() in store.inbox.days()

    def test_an_event_that_turns_up_after_the_check_is_not_dropped(
        self, store: Store, spool: Spool, clock: FrozenClock, monkeypatch: pytest.MonkeyPatch
    ):
        """`drop_day` looks again, in the transaction that drops: it has the last word."""
        self._event(store, T0)
        clock.advance(timedelta(days=30))
        monkeypatch.setattr(store.inbox, "day_status", lambda day: InboxDay(0, None, None))
        r = retention_once(store, spool, now=clock.now())
        assert r.dropped_inbox_days == [] and "not sealed" in r.kept_inbox_days[T0.date()]
        assert len(store.inbox.take("d", 10)) == 1


class TestMemoryRetention(RetentionTests):
    @pytest.fixture
    def clock(self) -> FrozenClock:
        return FrozenClock(T0)

    @pytest.fixture
    def store(self, clock: FrozenClock) -> Store:
        return MemoryStore(clock)

    @pytest.fixture
    def spool(self, clock: FrozenClock) -> Spool:
        return MemorySpool(clock)


@pytest.mark.postgres
class TestPostgresRetention(RetentionTests):
    @pytest.fixture
    def clock(self) -> FrozenClock:
        return FrozenClock(T0)

    @pytest.fixture
    def store(
        self,
        pg: psycopg.Connection[DictRow],
        pool: ConnectionPool[psycopg.Connection[DictRow]],
        clock: FrozenClock,
    ) -> Store:
        return PostgresStore(pool, clock)

    @pytest.fixture
    def spool(
        self,
        pg: psycopg.Connection[DictRow],
        pool: ConnectionPool[psycopg.Connection[DictRow]],
        clock: FrozenClock,
    ) -> Spool:
        return PostgresSpool(pool, clock)

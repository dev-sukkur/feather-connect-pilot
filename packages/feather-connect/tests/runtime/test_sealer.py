import json
import logging
import secrets
import threading
from collections.abc import Iterator, Sequence
from datetime import timedelta
from typing import Any, ClassVar

import psycopg
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory
from feather_connect.core.connector import ConnectorSpec, Deleted, InboundRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.partitions import by_bucket, by_fields
from feather_connect.engine.local import LocalEngine
from feather_connect.engine.port import RetryPolicy
from feather_connect.ids import stable_id
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.sealer import seal_once
from feather_connect.runtime.sweeper import sweep_once
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import StaleWriter, Store, Version
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.storage.spool import MemorySpool, Spool, table_to_records
from feather_connect.testing.store_contract import T0

Pg = psycopg.Connection[DictRow]


class Row(BaseModel):
    id: int
    x: int


class Hooks:
    """A push source that reads each request's body as JSON: an object, a list, or `{"_deleted": ...}`."""

    spec: ClassVar[ConnectorSpec] = memory.Source.spec
    connection = "hooks"
    unsigned = True

    def __init__(self) -> None:
        self.during_receive: Any = None
        self.failure: Exception | None = None  # what a body with `"fail": true` raises

    def verify(self, ctx: Context, request: InboundRequest) -> None:
        pass

    def receive(self, ctx: Context, request: InboundRequest) -> list[Any]:
        if self.during_receive is not None:
            during, self.during_receive = self.during_receive, None
            during()
        body = json.loads(request.body)
        if body == "explode":
            raise PermanentError(f"can't read {request.body!r}")
        if isinstance(body, dict) and body.get("fail") and self.failure is not None:
            raise self.failure
        rows = body if isinstance(body, list) else [body]
        return [Deleted(r["_deleted"]) if isinstance(r, dict) and "_deleted" in r else r for r in rows]


class World:
    def __init__(self, store: Store, spool: Spool, clock: FrozenClock, **dataset: Any) -> None:
        self.store, self.spool, self.clock = store, spool, clock
        self.source = Hooks()
        self.sink, self.log = memory.Sink("dst"), memory.Sink("log")
        self.hooks = fc.Dataset("hooks", source=self.source, key="id", strategy=fc.strategy.push(), **dataset)
        self.flow = fc.Flow(
            "demo",
            reads=self.hooks,
            deliver=[fc.to(self.sink), fc.to(self.log, mode="append")],
            triggers=[fc.on_event(self.hooks)],
        )
        self.engine = LocalEngine(clock)
        self.runtime = Runtime([self.flow], store, spool, self.engine, clock=clock)
        self.engine.register(self.runtime.tasks())

    def post(self, *bodies: Any, event_id: str | None = None) -> None:
        for body in bodies:
            raw = body if isinstance(body, bytes) else json.dumps(body).encode()
            request = InboundRequest("POST", "/", {}, raw, self.clock.now())
            self.store.inbox.put("hooks", request, event_id)

    def seal(self, **kwargs: Any) -> list[Version]:
        return seal_once(self.runtime, now=self.clock.now(), **kwargs)

    def seal_due(self) -> list[Version]:
        self.clock.advance(timedelta(seconds=1))
        return self.seal()

    def records_of(self, version: Version) -> list[Any]:
        return [
            record
            for chunk in self.spool.list_chunks(version.version_id)
            for record in table_to_records(self.spool.get_chunk(version.version_id, chunk.chunk_no))
        ]

    def drain(self) -> None:
        self.engine.run_until_idle()


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(T0)


@pytest.fixture
def world(clock: FrozenClock) -> World:
    return World(MemoryStore(clock), MemorySpool(clock), clock)


def test_seals_after_max_age(world: World):
    world.post({"id": 1}, {"id": 2}, {"id": 3})
    world.clock.advance(timedelta(seconds=0.5))
    assert world.seal() == []
    world.clock.advance(timedelta(seconds=0.5))
    [version] = world.seal()
    assert [r.key for r in world.records_of(version)] == ["1", "2", "3"]


def test_seals_at_max_events(world: World):
    world.post(*({"id": n} for n in range(500)))
    [version] = world.seal()
    assert version.upserts == 500


def test_a_big_backlog_seals_in_batches(world: World):
    world.post(*({"id": n} for n in range(25)))
    assert [v.upserts for v in world.seal(max_events=10)] == [10, 10]  # what remains is young and few
    assert [v.upserts for v in world.seal_due()] == [5]


def test_groups_events_by_partition(clock: FrozenClock):
    world = World(MemoryStore(clock), MemorySpool(clock), clock, partition=by_bucket("id", 4))
    world.post(*({"id": n} for n in range(16)))
    versions = world.seal_due()
    assert len(versions) > 1 and len({v.partition for v in versions}) == len(versions)
    assert all(not v.complete for v in versions)
    assert sum(v.upserts for v in versions) == 16


def test_keeps_per_key_order(world: World):  # FR-B6
    world.post({"id": 1, "x": "first"}, {"id": 1, "x": "second"})
    [version] = world.seal_due()
    assert [r.payload["x"] for r in world.records_of(version)] == ["first", "second"]
    world.drain()
    assert world.sink.rows == [{"id": 1, "x": "second"}]


def test_version_is_incomplete_and_only_explicit_deletes(world: World):  # I4
    world.post({"id": 1}, {"id": 2})
    world.seal_due()
    world.drain()
    world.post({"id": 3})
    [only_upserts] = world.seal_due()
    assert (only_upserts.complete, only_upserts.upserts, only_upserts.deletes) == (False, 1, 0)
    world.drain()
    assert sorted(r["id"] for r in world.sink.rows) == [1, 2, 3]  # what wasn't mentioned stays
    world.post({"_deleted": {"id": 1}})
    [with_delete] = world.seal_due()
    assert (with_delete.upserts, with_delete.deletes) == (0, 1)
    assert [r.op for r in world.records_of(with_delete)] == ["delete"]
    world.drain()
    assert sorted(r["id"] for r in world.sink.rows) == [2, 3]


def test_bad_event_is_quarantined_and_the_rest_seal(world: World):  # Review Focus 4
    world.post({"id": 1}, "explode", {"id": 3})
    [version] = world.seal_due()
    assert [r.key for r in world.records_of(version)] == ["1", "3"]
    [entry] = world.store.quarantine.list_open()
    assert "event" in entry.error and entry.dataset == "hooks" and "PermanentError" in entry.error
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None  # it isn't tried for ever


@pytest.mark.parametrize(
    "poison",
    [
        b'{"no_key": 1}',
        b'{"id": null}',
        b'{"id": 2, "_fc_key": "x"}',
        b'{"id": 2, "x": "nul \\u0000 byte"}',
        b'{"id": 2, "x": "lone \\ud800 surrogate"}',
        b'{"id": 2, "x\\u0000": 1}',
        b'{"id": "nul \\u0000 key"}',
        b'{"id": 2, "x": NaN}',
        b'{"id": 2, "x": [{"y": Infinity}]}',
        b"[1]",
        b"7",
        b'{"_deleted": {"no_key": 1}}',
    ],
)
def test_a_poison_payload_costs_only_itself(world: World, poison: bytes):
    world.post({"id": 1}, poison, {"id": 3})
    [version] = world.seal_due()
    assert [r.key for r in world.records_of(version)] == ["1", "3"]
    assert len(world.store.quarantine.list_open()) == 1
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None


def test_a_later_good_record_closes_a_rejected_key(clock: FrozenClock):
    world = World(MemoryStore(clock), MemorySpool(clock), clock, schema=Row)
    world.post({"id": 1, "x": "bad"})
    assert world.seal_due() == []
    assert [q.key for q in world.store.quarantine.list_open()] == ["1"]
    world.post({"id": 1, "x": 5})
    world.seal_due()
    assert world.store.quarantine.list_open() == []


def test_a_failed_read_of_the_secret_leaves_the_events_waiting(world: World, monkeypatch, caplog):
    def no_secret(connection: str) -> Context:
        raise PermanentError("secret HOOK_SECRET is not set")

    monkeypatch.setattr(world.runtime, "make_context", no_secret)
    world.post({"id": 1})
    assert world.seal_due() == []
    assert world.store.quarantine.list_open() == []
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is not None
    assert "PermanentError" in caplog.text


def test_nothing_of_a_request_reaches_the_log(world: World, caplog):
    secret = "payload-that-must-stay-out-of-logs"
    with caplog.at_level(logging.DEBUG):
        world.post(b'"explode"', {"id": 1, "x": secret}, {"id": 2, "x": f"\x00{secret}"})
        world.source.during_receive = lambda: None
        world.seal_due()
    assert secret not in caplog.text and "explode" not in caplog.text


def test_crash_between_seal_and_submit_is_recovered_by_the_sweeper(world: World, monkeypatch):
    def crash(route: str, version_id: str) -> None:
        raise RuntimeError("the engine went away")

    world.post({"id": 1})
    with monkeypatch.context() as patch:
        patch.setattr(world.runtime, "submit_apply", crash)
        [version] = world.seal_due()  # sealed, and the failed submits don't undo that
    assert world.store.ledger.get("dst", version.version_id).status == "pending"
    world.drain()
    assert world.sink.rows == []
    world.clock.advance(timedelta(minutes=2))
    sweep_once(world.store, world.runtime, world.engine, now=world.clock.now())
    world.drain()
    assert world.sink.rows == [{"id": 1}]


def test_repeated_seal_once_seals_nothing_new(world: World):
    world.post({"id": 1})
    assert len(world.seal_due()) == 1
    assert world.seal_due() == []
    assert len(world.store.versions.list_versions("hooks")) == 1


def test_an_event_id_retry_does_not_reach_an_append_route_twice(world: World):  # Review Focus 5
    world.post({"id": 1}, event_id="e1")
    world.post({"id": 1}, event_id="e1")
    world.seal_due()
    world.post({"id": 1}, event_id="e1")  # and after its seal
    world.seal_due()
    world.drain()
    assert len(world.log.rows) == 1


def test_an_append_route_gets_each_update_of_a_key_in_order(world: World):
    world.post({"id": 1, "x": "first"}, {"id": 1, "x": "second"})
    world.seal_due()
    world.drain()
    assert [r["x"] for r in world.log.rows] == ["first", "second"]


def test_on_event_flow_needs_no_run_per_event(world: World):
    world.post({"id": 1})
    [first] = world.seal_due()
    world.post({"id": 2})
    [second] = world.seal_due()
    assert first.run_id == second.run_id
    run = world.store.runs.get_run(first.run_id)
    assert run.trigger == "stream" and run.flow == "demo"
    world.drain()
    assert world.store.runs.get_run(first.run_id).status == "succeeded"
    world.clock.advance(timedelta(days=1))
    world.post({"id": 3})
    [tomorrow] = world.seal_due()
    assert tomorrow.run_id != first.run_id


def test_a_restarted_sealer_finds_the_days_stream_run(world: World):
    world.post({"id": 1})
    [first] = world.seal_due()
    fresh = Runtime([world.flow], world.store, world.spool, world.engine, clock=world.clock)
    world.post({"id": 2})
    world.clock.advance(timedelta(seconds=1))
    [second] = seal_once(fresh, now=world.clock.now())
    assert second.run_id == first.run_id


def test_a_stream_run_does_not_count_as_an_active_run(world: World):
    world.post({"id": 1})
    world.seal_due()  # its delivery is open, so its run is running
    assert world.runtime.active_runs("demo") == []


def test_a_manual_run_of_a_push_flow_reads_nothing_and_ends(world: World):
    world.runtime.start_run("demo")
    world.drain()
    [run] = world.store.runs.list_runs(flow="demo")
    assert run.status == "succeeded" and world.store.versions.list_versions("hooks") == []


def test_a_push_partition_can_not_be_verified_against_its_source(world: World):
    with pytest.raises(PermanentError, match="push"):
        world.runtime.verify_partition("hooks", "p")


def test_a_paused_flow_holds_its_events(world: World):
    world.post({"id": 1})
    world.runtime.pause_flow("demo", actor="me")
    assert world.seal_due() == []
    world.runtime.resume_flow("demo", actor="me")
    assert len(world.seal_due()) == 1


def test_a_failing_dataset_does_not_stop_another(clock: FrozenClock, caplog):
    store, spool = MemoryStore(clock), MemorySpool(clock)

    class Broken(Hooks):
        connection = "broken"

    def context(connection: str) -> Context:
        if connection == "broken":
            raise RuntimeError("no context")
        return Context({}, logging.getLogger(connection), clock.now(), "UTC")

    bad = fc.Dataset("bad", source=Broken(), key="id", strategy=fc.strategy.push())
    good = fc.Dataset("good", source=Hooks(), key="id", strategy=fc.strategy.push())
    flows = [
        fc.Flow("f1", reads=bad, deliver=[fc.to(memory.Sink("a"))]),
        fc.Flow("f2", reads=good, deliver=[fc.to(memory.Sink("b"))]),
    ]
    engine = LocalEngine(clock)
    runtime = Runtime(flows, store, spool, engine, clock=clock, make_context=context)
    for name in ("bad", "good"):
        store.inbox.put(name, InboundRequest("POST", "/", {}, b'{"id": 1}', clock.now()))
    clock.advance(timedelta(seconds=1))
    assert [v.dataset for v in seal_once(runtime, now=clock.now())] == ["good"]
    assert "sealer" in caplog.text and "bad" in caplog.text


def test_keep_state_tracks_moves_and_latest(clock: FrozenClock):
    world = World(MemoryStore(clock), MemorySpool(clock), clock, keep_state=True, partition=by_fields("b"))
    world.post({"id": 1, "b": "a"})
    [first] = world.seal_due()
    world.post({"id": 1, "b": "z"})
    [second] = world.seal_due()
    assert first.partition != second.partition
    assert world.records_of(second)[0].moved_from == first.partition
    assert world.store.state.get_many("hooks", ["1"])["1"].partition == second.partition


# Two sealers, and a crash, on both stores.


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.postgres)])
def backend(request: pytest.FixtureRequest, clock: FrozenClock) -> Iterator[tuple[Store, Spool]]:
    if request.param == "memory":
        yield MemoryStore(clock), MemorySpool(clock)
        return
    request.getfixturevalue("pg")
    pool: ConnectionPool[Pg] = request.getfixturevalue("pool")
    yield PostgresStore(pool, clock), PostgresSpool(pool, clock)


@pytest.fixture
def any_world(backend: tuple[Store, Spool], clock: FrozenClock) -> World:
    return World(*backend, clock)


def events_of(world: World, versions: Sequence[Version]) -> list[str]:
    return sorted(r.key for v in versions for r in world.records_of(v))


def test_a_sealer_overtaken_while_it_reads_seals_nothing_twice(any_world: World):
    world = any_world
    world.post({"id": 1}, {"id": 2})
    world.clock.advance(timedelta(seconds=1))
    inner: list[Version] = []
    world.source.during_receive = lambda: inner.extend(world.seal())
    outer = world.seal()
    assert len(inner) == 1 and outer == []
    assert len(world.store.versions.list_versions("hooks")) == 1
    assert events_of(world, inner) == ["1", "2"]


def test_a_sealer_fenced_while_it_writes_leaves_the_seal_to_the_other(any_world: World, monkeypatch):
    world = any_world
    world.post({"id": 1}, {"id": 2})
    world.clock.advance(timedelta(seconds=1))
    version_id = stable_id("hooks", "_", str(world.store.inbox.take("hooks", 1)[0].event_seq))
    real = world.spool.put_chunk
    inner: list[Version] = []

    def overtaken(*args: Any, **kwargs: Any) -> Any:
        monkeypatch.setattr(world.spool, "put_chunk", real)
        inner.extend(world.seal())  # claims the version, so this writer's chunk is refused
        return real(*args, **kwargs)

    monkeypatch.setattr(world.spool, "put_chunk", overtaken)
    assert world.seal() == []
    assert [v.version_id for v in inner] == [version_id]
    assert events_of(world, inner) == ["1", "2"]
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None


def test_a_rerun_after_a_crash_before_the_seal_gives_one_version(any_world: World, monkeypatch):
    world = any_world
    world.post({"id": 1}, {"id": 2})
    with monkeypatch.context() as patch:

        def crash(*args: Any, **kwargs: Any) -> None:
            raise ConnectionError("power cut")  # as a crash does, it blames no event

        patch.setattr(world.store.inbox, "seal_batch", crash)
        assert world.seal_due() == []  # the chunks are written and nothing is sealed
    world.post({"id": 3})
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["1", "2", "3"]
    assert len(world.store.versions.list_versions("hooks")) == 1


def test_a_sealer_that_lost_the_events_is_refused_and_leaves_no_chunks(any_world: World, monkeypatch):
    """The other sealer sealed the first event under its own id, so this one's id differs and no claim fences it."""
    world = any_world
    world.post({"id": 1}, {"id": 2})
    world.clock.advance(timedelta(seconds=1))
    [first, second] = world.store.inbox.take("hooks", 2)
    real = world.store.inbox.take
    monkeypatch.setattr(world.store.inbox, "take", lambda dataset, limit: [second])
    seal_batch = world.store.inbox.seal_batch

    def beaten(*args: Any, **kwargs: Any) -> Any:
        monkeypatch.setattr(world.store.inbox, "seal_batch", seal_batch)
        seal_batch([second.event_seq], [], [])  # the other sealer got there first
        return seal_batch(*args, **kwargs)

    monkeypatch.setattr(world.store.inbox, "seal_batch", beaten)
    assert world.seal() == []
    version_id = stable_id("hooks", "_", str(second.event_seq))
    assert world.spool.list_chunks(version_id) == []
    with pytest.raises(KeyError):
        world.store.versions.get(version_id)
    monkeypatch.setattr(world.store.inbox, "take", real)
    assert first in real("hooks", 2)


def test_seal_batch_refuses_a_sealed_event(any_world: World):
    world = any_world
    world.post({"id": 1})
    [event] = world.store.inbox.take("hooks", 1)
    world.store.inbox.seal_batch([event.event_seq], [], [])
    with pytest.raises(StaleWriter):
        world.store.inbox.seal_batch([event.event_seq], [], [])


@pytest.mark.postgres
def test_two_sealers_in_threads_give_each_event_one_version(
    pg: Pg, pool: ConnectionPool[Pg], clock: FrozenClock
):
    world = World(PostgresStore(pool, clock), PostgresSpool(pool, clock), clock)
    world.post(*({"id": n} for n in range(40)))
    clock.advance(timedelta(seconds=1))
    sealed: list[Version] = []
    start = threading.Barrier(2)

    def sealer() -> None:
        start.wait()
        for _ in range(10):
            sealed.extend(world.seal(max_events=7))

    threads = [threading.Thread(target=sealer) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    versions = world.store.versions.list_versions("hooks")
    keys = sorted(int(r.key) for v in versions for r in world.records_of(v))
    if world.store.inbox.oldest_age("hooks", clock.now()) is None:
        assert keys == list(range(40))
    else:
        assert len(keys) == len(set(keys))
    assert sorted(v.version_id for v in sealed) == sorted(v.version_id for v in versions)


# What the store can't take must cost only its own event (S1).


def nested(depth: int, kind: str) -> Any:
    value: Any = 1
    for _ in range(depth):
        value = [value] if kind == "list" else {"a": value}
    return value


@pytest.mark.parametrize("kind", ["list", "dict"])
def test_a_too_deeply_nested_event_costs_only_itself(any_world: World, kind: str):
    world = any_world
    world.post({"id": 1}, {"id": 2, "a": nested(200, kind)}, {"id": 3})
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["1", "3"]
    [entry] = world.store.quarantine.list_open()
    assert entry.key == "2" and "nested" in entry.error
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None


def test_nesting_up_to_the_limit_seals(any_world: World):
    world = any_world
    world.post({"id": 1, "a": nested(31, "list")}, {"id": 2, "a": nested(31, "dict")})
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["1", "2"]
    assert world.store.quarantine.list_open() == []


def test_a_too_long_partition_key_costs_only_its_event(backend: tuple[Store, Spool], clock: FrozenClock):
    world = World(*backend, clock, partition=by_fields("p"))
    world.post({"id": 1, "p": "a"}, {"id": 2, "p": secrets.token_urlsafe(2400)}, {"id": 3, "p": "a"})
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["1", "3"]
    [entry] = world.store.quarantine.list_open()
    assert entry.key == "2" and "partition" in entry.error
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None


def test_a_too_long_key_costs_only_its_event(backend: tuple[Store, Spool], clock: FrozenClock):
    world = World(*backend, clock, keep_state=True)
    world.post({"id": 1}, {"id": secrets.token_urlsafe(2400)}, {"id": 3})
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["1", "3"]
    [entry] = world.store.quarantine.list_open()
    assert "key" in entry.error
    assert set(world.store.state.get_many("hooks", ["1", "3"])) == {"1", "3"}


def test_a_batch_that_fails_for_one_event_seals_the_rest(any_world: World, monkeypatch):
    """Whatever no check foresaw: the batch is split until the one event that fails is alone, with its reason."""
    world = any_world
    real = world.spool.put_chunk

    def refuses_2(version_id: str, chunk_no: int, kind: Any, table: Any, writer: Any = None) -> None:
        if "2" in table.column("_fc_key").to_pylist():
            raise ValueError("the spool can't take record 2")
        real(version_id, chunk_no, kind, table, writer)

    monkeypatch.setattr(world.spool, "put_chunk", refuses_2)
    world.post(*({"id": n} for n in range(1, 6)))
    sealed = world.seal_due()
    assert events_of(world, sealed) == ["1", "3", "4", "5"]
    [entry] = world.store.quarantine.list_open()
    assert entry.key.startswith("event:") and "ValueError" in entry.error and "record 2" in entry.error
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None
    world.drain()
    assert sorted(r["id"] for r in world.sink.rows) == [1, 3, 4, 5]


def test_a_store_outage_mid_batch_quarantines_nothing(any_world: World, monkeypatch):
    world = any_world

    def away(*args: Any, **kwargs: Any) -> None:
        raise ConnectionError("the database went away")

    monkeypatch.setattr(world.spool, "put_chunk", away)
    world.post({"id": 1}, {"id": 2})
    assert world.seal_due() == []
    assert world.store.quarantine.list_open() == []
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is not None
    monkeypatch.undo()
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["1", "2"]


# A failed `receive` that may pass when tried again leaves its event waiting, and a quarantined one can be had back (S2).

RETRY = RetryPolicy()


def test_a_transient_receive_error_leaves_the_event_for_later(any_world: World):
    world = any_world
    world.source.failure = TransientError("the connector's API answered 503")
    world.post({"id": 1}, {"id": 2, "fail": True}, {"id": 3})
    [before] = world.seal_due()
    assert events_of(world, [before]) == ["1"]  # what came before it seals, and what came after it waits
    assert world.store.quarantine.list_open() == []
    world.clock.advance(RETRY.first_delay / 2)
    assert world.seal() == []  # it waits its turn, and doesn't spin
    world.source.failure = None
    world.clock.advance(RETRY.first_delay)
    [after] = world.seal()
    assert [r.key for r in world.records_of(after)] == ["2", "3"]
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None


def test_a_receive_error_that_doesnt_pass_is_quarantined_after_its_tries(any_world: World):
    world = any_world
    world.source.failure = RuntimeError("a bug in the connector")
    world.post({"id": 2, "fail": True}, {"id": 3})
    for _ in range(RETRY.max_attempts - 1):
        assert world.seal_due() == []
        assert world.store.quarantine.list_open() == []
        world.clock.advance(RETRY.max_delay)
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["3"]
    [entry] = world.store.quarantine.list_open()
    assert entry.key.startswith("event:") and f"{RETRY.max_attempts} attempts" in entry.error


def test_a_permanent_receive_error_is_quarantined_at_once(any_world: World):
    world = any_world
    world.source.failure = PermanentError("the event is of a kind this connector doesn't know")
    world.post({"id": 2, "fail": True}, {"id": 3})
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["3"]
    [entry] = world.store.quarantine.list_open()
    assert "PermanentError" in entry.error


def quarantined_event(world: World) -> int:
    world.source.failure = PermanentError("a schema change the deploy got wrong")
    world.post({"id": 2, "fail": True})
    assert world.seal_due() == []
    [entry] = world.store.quarantine.list_open()
    return int(entry.key.removeprefix("event:"))


def test_a_quarantined_event_can_be_processed_again(any_world: World):
    world = any_world
    seq = quarantined_event(world)
    world.source.failure = None  # the fix is deployed
    assert world.store.inbox.reprocess("hooks", seq) is True
    [version] = world.seal_due()
    assert events_of(world, [version]) == ["2"]
    assert world.store.inbox.reprocess("hooks", seq) is False  # it is sealed now


def test_a_quarantined_event_can_be_let_go(any_world: World):
    world = any_world
    seq = quarantined_event(world)
    assert world.store.inbox.release("hooks", seq) is True
    world.source.failure = None
    assert world.seal_due() == []
    assert world.store.inbox.oldest_age("hooks", world.clock.now()) is None
    assert world.store.inbox.reprocess("hooks", seq) is False


def test_a_quarantined_event_keeps_its_inbox_day(any_world: World):
    world = any_world
    seq = quarantined_event(world)
    [day] = world.store.inbox.days()
    assert world.store.inbox.drop_day(day) is False
    world.store.inbox.release("hooks", seq)
    assert world.store.inbox.drop_day(day) is True


# Within a batch each key keeps its order across partitions, and its last write wins (S3).


@pytest.fixture
def moving_world(backend: tuple[Store, Spool], clock: FrozenClock) -> World:
    world = World(*backend, clock, keep_state=True, partition=by_fields("p"))
    world.post({"id": 1, "p": "a", "x": 0})
    world.seal_due()
    world.drain()
    return world


def ends_with(world: World, partition: str, payload: dict[str, Any]) -> None:
    world.drain()
    state = world.store.state.get_many("hooks", ["1"])["1"]
    assert (state.partition, dict(state.payload)) == (f"p={partition}", payload)
    assert [r for r in world.sink.rows if r["id"] == 1] == [payload]


def test_a_key_that_moves_twice_in_a_batch_ends_where_it_went_last(moving_world: World):
    world = moving_world
    world.post({"id": 1, "p": "b", "x": 1}, {"id": 1, "p": "c", "x": 2}, {"id": 1, "p": "b", "x": 3})
    world.seal_due()
    ends_with(world, "b", {"id": 1, "p": "b", "x": 3})
    assert [r["x"] for r in world.log.rows] == [0, 1, 2, 3]  # an append route still gets each, in order


def test_a_key_that_moves_back_among_other_keys_ends_where_it_went_last(moving_world: World):
    world = moving_world
    world.post(
        {"id": 2, "p": "b"},
        {"id": 1, "p": "b", "x": 1},
        {"id": 3, "p": "c"},
        {"id": 1, "p": "c", "x": 2},
        {"id": 4, "p": "b"},
        {"id": 1, "p": "b", "x": 3},
    )
    world.seal_due()
    ends_with(world, "b", {"id": 1, "p": "b", "x": 3})
    world.drain()
    assert sorted(r["id"] for r in world.sink.rows) == [1, 2, 3, 4]


def test_a_key_that_moves_twice_in_one_event_ends_where_it_went_last(moving_world: World):
    world = moving_world
    world.post([{"id": 1, "p": "b", "x": 1}, {"id": 1, "p": "c", "x": 2}, {"id": 1, "p": "b", "x": 3}])
    world.seal_due()
    ends_with(world, "b", {"id": 1, "p": "b", "x": 3})


@pytest.mark.parametrize("named", ["b", "a", "c"])
def test_a_delete_after_a_move_in_a_batch_deletes(moving_world: World, named: str):
    """The delete names the partition the key moved to, the one it left, or another the batch has seen."""
    world = moving_world
    world.post({"id": 3, "p": "c"}, {"id": 1, "p": "b", "x": 1}, {"_deleted": {"id": 1, "p": named}})
    world.seal_due()
    world.drain()
    assert world.store.state.get_many("hooks", ["1"]) == {}
    assert [r for r in world.sink.rows if r["id"] == 1] == []

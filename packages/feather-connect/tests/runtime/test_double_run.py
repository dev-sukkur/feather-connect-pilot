"""F4: an extract or a seal that runs twice, in any order, must leave a sealed version as it was sealed (I1).

Each test has one execution come in at the worst moment of another, on both stores.
"""

import logging
import random
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.context import Context
from feather_connect.ids import stable_id
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import run_extract
from feather_connect.runtime.seal import run_seal
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import StaleWriter, Store, Version
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.postgres.store import PostgresStore
from feather_connect.storage.spool import ChunkKind, MemorySpool, Spool, table_to_records

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
ROUTES = [fc.to(Sink("a"))]
KINDS: tuple[ChunkKind, ...] = ("data", "snapshot", "full", "moves")
DAY_18, DAY_19 = "date=2026-10-18", "date=2026-10-19"


class Died(Exception):
    """An execution that stops here, as a killed worker does."""


class HookedSource(Source):
    """A memory source that calls `before` once when a read starts, and `during` once after a read's first row."""

    def __init__(self) -> None:
        super().__init__()
        self.before: list[Callable[[], None]] = []
        self.during: list[Callable[[], None]] = []

    def read(self, ctx: Any, req: Any) -> Iterator[Any]:
        if self.before:
            self.before.pop(0)()
        for batch in super().read(ctx, req):
            for i, row in enumerate(batch):
                yield [row]
                if i == 0 and self.during:
                    self.during.pop(0)()


def contents(deps: RuntimeDeps, version_id: str) -> dict[tuple[str, int], list[tuple[str, str, int]]]:
    """What a version holds, chunk by chunk, in a form that can be compared."""
    found: dict[tuple[str, int], list[tuple[str, str, int]]] = {}
    for kind in KINDS:
        for chunk in deps.spool.list_chunks(version_id, kind):
            table = deps.spool.get_chunk(version_id, chunk.chunk_no, kind)
            if kind == "data":
                found[(kind, chunk.chunk_no)] = [(r.key, r.op, r.row_hash) for r in table_to_records(table)]
            else:
                found[(kind, chunk.chunk_no)] = [(str(row), "", 0) for row in table.to_pylist()]
    return found


def data_rows(deps: RuntimeDeps, version: Version) -> int:
    return sum(chunk.rows for chunk in deps.spool.list_chunks(version.version_id))


class DoubleRunTests:
    @pytest.fixture
    def clock(self) -> FrozenClock:
        return FrozenClock(T0)

    @pytest.fixture
    def deps(self, store: Store, spool: Spool, clock: FrozenClock) -> RuntimeDeps:
        def make_context(connection: str) -> Context:
            return Context({}, logging.getLogger(connection), clock.now(), "UTC")

        return RuntimeDeps(store, spool, clock, "sha", make_context)

    def stamped(self) -> Any:
        return fc.Dataset(
            "d",
            source=HookedSource(),
            key="id",
            strategy=fc.strategy.by_stamp("u"),
            partition=fc.partition.by_day("date", tz="UTC"),
        )

    def snapshot(self, **kw: Any) -> Any:
        return fc.Dataset("d", source=HookedSource(), key="id", strategy=fc.strategy.snapshot_diff(), **kw)

    def sliced(self) -> Any:
        return fc.Dataset(
            "d",
            source=HookedSource(),
            key="id",
            strategy=fc.strategy.snapshot_diff(),
            partition=fc.partition.by_day("date", tz="UTC"),
        )

    def run(
        self, ds: Any, deps: RuntimeDeps, partition: str = "_", stage_only: bool = False
    ) -> list[Version]:
        return run_extract(ds, "r1", partition, routes=ROUTES, stage_only=stage_only, deps=deps)

    def test_a_late_by_stamp_read_does_not_delete_what_the_other_copy_sealed(self, deps: RuntimeDeps):
        """The review's scenario: `_slice` clears a version while the read is under way, after the other copy sealed."""
        ds = self.stamped()
        rows = [{"id": 1, "date": "2026-10-18", "u": T0}, {"id": 2, "date": "2026-10-18", "u": T0}]
        ds.source.load(rows)
        version_id = stable_id("r1", "d", DAY_18)
        sealed: dict[str, Any] = {}

        def the_other_copy() -> None:
            sealed["versions"] = self.run(ds, deps, "*")
            sealed["contents"] = contents(deps, version_id)

        # The late copy has started its read when the other one starts and seals, before the late one's first row lands.
        ds.source.before.append(the_other_copy)
        late = self.run(ds, deps, "*")

        assert sealed["contents"][("data", 0)]
        assert contents(deps, version_id) == sealed["contents"]
        assert [v.version_id for v in late] == [version_id]
        assert data_rows(deps, deps.store.versions.get(version_id)) == 2

    def test_a_late_read_with_other_rows_does_not_overwrite_sealed_chunks(self, deps: RuntimeDeps):
        ds = self.snapshot()
        ds.source.load([{"id": 1, "v": "first"}, {"id": 2, "v": "first"}])
        version_id = stable_id("r1", "d", "_")
        sealed: dict[str, Any] = {}

        def the_other_copy() -> None:
            ds.source.load([{"id": 1, "v": "other"}])
            self.run(ds, deps)
            sealed.update(contents(deps, version_id))
            ds.source.load([{"id": 1, "v": "late"}, {"id": 3, "v": "late"}])

        ds.source.during.append(the_other_copy)
        self.run(ds, deps)

        assert sealed
        assert contents(deps, version_id) == sealed
        version = deps.store.versions.get(version_id)
        assert version.status == "sealed" and version.upserts == data_rows(deps, version) == 1

    def test_a_copy_that_started_later_and_died_does_not_let_the_first_seal_nothing(
        self, deps: RuntimeDeps, monkeypatch: pytest.MonkeyPatch
    ):
        """The first copy has written all it has when the second clears the version and dies: sealing must not go on."""
        ds = self.snapshot()
        ds.source.load([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        version_id = stable_id("r1", "d", "_")
        real_seal = deps.store.versions.seal

        def seal_after_the_other_copy(*args: Any) -> list[Version]:
            monkeypatch.setattr(deps.store.versions, "seal", real_seal)

            def die() -> None:
                raise Died

            ds.source.during.append(die)
            with pytest.raises(Died):
                self.run(ds, deps)
            return real_seal(*args)

        monkeypatch.setattr(deps.store.versions, "seal", seal_after_the_other_copy)
        with pytest.raises(StaleWriter):
            self.run(ds, deps)
        found = deps.store.versions.get_many([version_id])
        assert not [v for v in found if v.status == "sealed" and data_rows(deps, v) != v.upserts]

        # The retry the engine makes gets the whole version sealed.
        again = self.run(ds, deps)
        assert [v.status for v in again] == ["sealed"]
        assert data_rows(deps, again[0]) == 2

    def test_a_late_seal_task_leaves_the_sealed_chunks_alone(
        self, deps: RuntimeDeps, monkeypatch: pytest.MonkeyPatch
    ):
        ds = self.sliced()
        ds.source.load([{"id": 1, "date": "2026-10-18"}, {"id": 2, "date": "2026-10-19"}])
        run_extract(ds, "r0", DAY_18, routes=ROUTES, stage_only=False, deps=deps)
        run_extract(ds, "r0", DAY_19, routes=ROUTES, stage_only=False, deps=deps)
        # Key 1 leaves the slice, so its seal has a delete to write.
        ds.source.load([{"id": 2, "date": "2026-10-19"}, {"id": 3, "date": "2026-10-18"}])
        deps.store.versions.register_slices("r1", "d", [DAY_18, DAY_19])
        self.run(ds, deps, DAY_18, stage_only=True)
        self.run(ds, deps, DAY_19, stage_only=True)
        version_id = stable_id("r1", "d", DAY_18)
        sealed: dict[str, Any] = {}
        real_get = deps.store.versions.get

        def the_other_seal_goes_first(wanted: str) -> Version:
            monkeypatch.setattr(deps.store.versions, "get", real_get)
            found = real_get(wanted)  # the late copy has read the version as staged
            run_seal(ds, "r1", routes=ROUTES, deps=deps)
            sealed.update(contents(deps, version_id))
            return found

        monkeypatch.setattr(deps.store.versions, "get", the_other_seal_goes_first)
        versions = run_seal(ds, "r1", routes=ROUTES, deps=deps)

        assert sealed
        assert contents(deps, version_id) == sealed
        assert [v.status for v in versions] == ["sealed"]
        delivered = table_to_records(deps.spool.get_chunk(version_id, 0))
        assert {(r.key, r.op) for r in delivered} == {("1", "delete"), ("3", "upsert")}

    def test_a_second_copy_of_a_finished_slice_leaves_it_alone(self, deps: RuntimeDeps):
        """Its seal task may already be working from the slice, so the read that finished it stands."""
        ds = self.sliced()
        ds.source.load([{"id": 1, "date": "2026-10-18", "v": "first"}])
        deps.store.versions.register_slices("r1", "d", [DAY_18])
        self.run(ds, deps, DAY_18, stage_only=True)
        assert deps.store.versions.finish_slice("r1", "d", DAY_18)
        version_id = stable_id("r1", "d", DAY_18)
        before = contents(deps, version_id)

        ds.source.load([{"id": 1, "date": "2026-10-18", "v": "late"}])
        assert self.run(ds, deps, DAY_18, stage_only=True) == []

        assert contents(deps, version_id) == before
        assert [v.version_id for v in deps.store.versions.staged_for_run("r1", "d")] == [version_id]

    def test_copies_racing_on_threads_seal_one_whole_version(self, deps: RuntimeDeps):
        ds = self.snapshot()
        ds.source.load([{"id": i, "v": "a"} for i in range(50)])
        version_id = stable_id("r1", "d", "_")

        def one_copy(_: int) -> None:
            for attempt in range(50):
                try:
                    self.run(ds, deps)
                except (
                    StaleWriter,
                    KeyError,
                ):  # a copy that lost its claim tries again later, as the engine has it do
                    time.sleep(random.uniform(0, 0.02 * attempt))
                    continue
                return
            raise AssertionError("a copy never got through")

        with ThreadPoolExecutor(4) as threads:
            list(threads.map(one_copy, range(4)))

        version = deps.store.versions.get(version_id)
        assert version.status == "sealed"
        assert version.upserts == data_rows(deps, version) == 50
        assert deps.spool.list_chunks(version_id, "snapshot")
        assert deps.store.ledger.get("a", version_id).status == "pending"


class TestMemoryDoubleRun(DoubleRunTests):
    @pytest.fixture
    def store(self, clock: FrozenClock) -> Store:
        return MemoryStore(clock)

    @pytest.fixture
    def spool(self, clock: FrozenClock) -> Spool:
        return MemorySpool(clock)


@pytest.mark.postgres
class TestPostgresDoubleRun(DoubleRunTests):
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

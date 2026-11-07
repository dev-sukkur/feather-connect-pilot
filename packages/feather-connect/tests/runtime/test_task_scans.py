"""A daily stream run holds a day of tasks, so what runs per tick or per apply must not read them all.

Nothing here times anything. It records each statement that a tick, its apply and a sweep send, on a run holding many
tasks and versions, and looks at how many statements there are, how many rows each gives back and how Postgres plans it.
"""

import json
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from psycopg.rows import DictRow
from psycopg_pool import ConnectionPool

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.runtime.scheduler import tick
from feather_connect.runtime.sweeper import sweep_once
from feather_connect.storage.postgres.spool import PostgresSpool
from feather_connect.storage.postgres.store import PostgresStore, _json, _unjson  # pyright: ignore[reportPrivateUsage]
from feather_connect.storage.postgres.store import _Db as Db  # pyright: ignore[reportPrivateUsage]

pytestmark = pytest.mark.postgres

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
MANY = 20_000
FEW_ROWS = 20  # the most that a statement of a tick needs from either table


@dataclass
class Statement:
    query: str
    params: Any
    rows: int


@dataclass
class Recorder:
    statements: list[Statement] = field(default_factory=list[Statement])


class _Recording:
    """A connection that notes what is sent through it."""

    def __init__(self, conn: psycopg.Connection[DictRow], recorder: Recorder) -> None:
        self._conn = conn
        self._recorder = recorder

    def execute(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        cursor = self._conn.execute(query, params, **kwargs)
        self._recorder.statements.append(Statement(str(query), params, cursor.rowcount))
        return cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    found = Recorder()
    real = Db.tx

    @contextmanager
    def tx(self: Db) -> Generator[Any]:
        with real(self) as conn:
            yield _Recording(conn, found)

    monkeypatch.setattr(Db, "tx", tx)
    return found


class World:
    def __init__(self, pool: ConnectionPool[psycopg.Connection[DictRow]], pg: Any) -> None:
        self.pg = pg
        self.clock = FrozenClock(T0)
        self.store = PostgresStore(pool, self.clock)
        self.engine = LocalEngine(self.clock)
        self.source = Source(rows=[self.row(0)])
        dataset = fc.Dataset("d", source=self.source, key="id", strategy=fc.strategy.by_stamp("at"))
        flow = fc.Flow("f", reads=dataset, deliver=[fc.to(Sink("a"))], triggers=[fc.every("30s")])
        self.rt = Runtime(
            [flow], self.store, PostgresSpool(pool, self.clock), self.engine, clock=self.clock, code_sha="sha"
        )
        self.engine.register(self.rt.tasks())
        self.ticks = 0

    @staticmethod
    def row(n: int) -> dict[str, Any]:
        return {"id": n, "at": (T0 + timedelta(seconds=n)).isoformat()}

    def one_tick(self) -> None:
        """A tick with one new row: its plan, extract and apply, and a sweep that finds nothing to do."""
        self.ticks += 1
        self.source.load([self.row(self.ticks)])
        self.clock.advance(timedelta(seconds=30))
        tick(self.rt, now=self.clock.now())
        self.engine.run_until_idle()
        sweep_once(self.store, self.rt, self.engine, now=self.clock.now())

    def fill(self, run_id: str, count: int) -> None:
        """Give the run `count` finished tasks and `count` sealed versions, as a long day would have left it."""
        self.pg.execute(
            """insert into feather.task (tenant_id, task_id, run_id, kind, ref, status, started_at, finished_at)
               select 'default', 'old-' || n, %(run)s, (array['plan', 'extract', 'apply', 'skip'])[n %% 4 + 1],
                      'old:' || n, case when n %% 4 = 3 then 'skipped' else 'succeeded' end, %(at)s, %(at)s
               from generate_series(1, %(count)s) n""",
            {"run": run_id, "count": count, "at": T0},
        )
        self.pg.execute(
            """insert into feather.version (tenant_id, version_id, dataset, partition, run_id, complete, upserts,
                                            deletes, content_hash, code_sha, status, seal_seq, sealed_at, tick)
               select 'default', 'old-' || n, 'old', '*', %(run)s, false, 1, 0, 'h', 'sha', 'sealed',
                      nextval('feather.seal_seq'), %(at)s, 'old-' || n
               from generate_series(1, %(count)s) n""",
            {"run": run_id, "count": count, "at": T0},
        )
        self.pg.execute("analyze feather.task")
        self.pg.execute("analyze feather.version")


@pytest.fixture
def world(pool: ConnectionPool[psycopg.Connection[DictRow]], pg: Any) -> Iterator[World]:
    found = World(pool, pg)
    tick(found.rt, now=found.clock.now())  # the first only notes the time
    yield found


def _nodes(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans", ()):
        yield from _nodes(child)


def _seq_scans(pg: Any, statement: Statement) -> list[str]:
    """Return the tables of the two that grow which Postgres would read whole to answer the statement."""
    found = pg.execute("explain (format json) " + statement.query, statement.params).fetchone()
    plan = found["QUERY PLAN"]
    plan = json.loads(plan) if isinstance(plan, str) else plan
    return [
        node["Relation Name"]
        for node in _nodes(plan[0]["Plan"])
        if node["Node Type"] == "Seq Scan" and node.get("Relation Name") in ("task", "version")
    ]


def _touches_a_run(statement: Statement) -> bool:
    return "feather.task" in statement.query or "feather.version" in statement.query


def test_a_tick_on_a_run_of_many_tasks_costs_what_it_does_on_a_small_one(world: World, recorder: Recorder):
    world.one_tick()  # the day's run and the first of what is made once a day
    world.one_tick()
    recorder.statements.clear()
    world.one_tick()
    small = len(recorder.statements)

    run_id = world.rt.stream_run("f", world.clock.now())
    world.fill(run_id, MANY)
    recorder.statements.clear()
    world.one_tick()

    assert len(recorder.statements) == small
    big = [s for s in recorder.statements if _touches_a_run(s)]
    assert big
    too_many = [(s.rows, s.query) for s in big if s.rows > FEW_ROWS]
    assert too_many == []
    assert [(_seq_scans(world.pg, s), s.query) for s in big if _seq_scans(world.pg, s)] == []
    assert world.store.runs.get_run(run_id).status == "succeeded"


def test_the_reads_of_a_stream_run_use_their_indexes(world: World, recorder: Recorder):
    world.one_tick()
    run_id = world.rt.stream_run("f", world.clock.now())
    world.fill(run_id, MANY)
    runs, versions = world.store.runs, world.store.versions
    recorder.statements.clear()
    assert len(runs.list_tasks(run_id, limit=50, newest_first=True)) == 50
    assert runs.list_tasks(run_id, status="running", kinds=("plan", "extract"), limit=20) == []
    assert runs.list_tasks(run_id, status="failed", kinds=("apply",), limit=1) == []
    assert len(runs.list_tasks(run_id, kinds=("plan",), limit=1, newest_first=True)) == 1
    assert len(runs.tasks_by_ref(run_id, "old:19999")) == 1
    assert len(versions.newest_sealed_for_run(run_id, "old", 50)) == 50
    assert len(versions.sealed_for_run(run_id, "old", tick="old-5")) == 1
    assert world.store.reads.retried_tasks([run_id]) == {}
    used = [s for s in recorder.statements if _touches_a_run(s)]
    assert [(_seq_scans(world.pg, s), s.query) for s in used if _seq_scans(world.pg, s)] == []


def test_a_tick_does_not_read_every_watermark_of_its_dataset(world: World, recorder: Recorder):  # S6
    world.one_tick()
    world.one_tick()
    newest = world.pg.execute(
        """select partition, watermark from feather.version
           where dataset = 'd' and status = 'sealed' and watermark is not null order by seal_seq desc limit 1"""
    ).fetchone()
    older = _unjson(newest["watermark"])
    older = older - timedelta(days=1) if isinstance(older, datetime) else type(older)(T0 - timedelta(days=1))
    world.pg.execute(  # a year of earlier versions, sealed before the ones the ticks made
        """insert into feather.version (tenant_id, version_id, dataset, partition, run_id, complete, upserts,
                                        deletes, content_hash, code_sha, status, seal_seq, sealed_at, watermark)
           select 'default', 'mark-' || n, 'd', %(partition)s, 'old-run', false, 1, 0, 'h', 'sha', 'sealed',
                  -n, %(at)s, %(mark)s
           from generate_series(1, %(count)s) n""",
        {"partition": newest["partition"], "count": MANY, "at": T0, "mark": _json(older)},
    )
    world.pg.execute("analyze feather.version")
    recorder.statements.clear()
    world.one_tick()
    read = [
        (s.rows, s.query) for s in recorder.statements if "feather.version" in s.query and s.rows > FEW_ROWS
    ]
    assert read == []

"""The flows of the chaos tests, and connectors that report to the test through tables of the database under test.

A *hold point* stops a connector mid-call for the test to act at a known moment: see `pass_point`. The tables are made
by `tests/chaos/conftest.py`.
"""

import os
import time
from collections.abc import Iterator
from typing import Any

import psycopg

import feather_connect as fc
from feather_connect.connectors import simulation
from feather_connect.core.connector import ApplyBatch, ApplyResult, CheckResult, ConnectorSpec, ReadRequest
from feather_connect.core.context import Context
from feather_connect.runtime import sealer
from feather_connect.storage import spool

ROWS = 100
CHUNK_ROWS = 20  # so that ROWS make 5 chunks
SLOW_S = 0.1  # what a chunk takes to apply, so that routes overlap and a kill finds others under way too

# Only in a worker or a server: the test process loads this module too, and shares the setting with whatever it runs
# next.
if "FEATHER_CHAOS_URL" in os.environ:
    spool.CHUNK_ROWS = CHUNK_ROWS


def _who() -> str:
    """Name this process: the id of its worker and which start of that worker it is."""
    return os.environ["FEATHER_CHAOS_WHO"]


def _connect() -> psycopg.Connection[Any]:
    """Open a session that bears this process's name, so that the test can see the process die with it."""
    return psycopg.connect(
        os.environ["FEATHER_CHAOS_URL"], autocommit=True, connect_timeout=5, application_name=_who()
    )


def pass_point(conn: psycopg.Connection[Any], point: str) -> None:
    """Stop here until the test releases this process, if the test has armed `point` and it has holds left.

    The row in `chaos.reached` is committed before the wait: seeing it, the test knows this process is at this line.
    """
    armed = conn.execute(
        "update chaos.holds set remaining = remaining - 1 where point = %s and remaining > 0 returning point",
        (point,),
    ).fetchone()
    if armed is None:
        return
    row = conn.execute(
        "insert into chaos.reached (point, who) values (%s, %s) returning id", (point, _who())
    ).fetchone()
    assert row is not None
    while True:
        released = conn.execute("select released from chaos.reached where id = %s", (row[0],)).fetchone()
        assert released is not None
        if released[0]:
            return
        time.sleep(0.05)


def _hold_the_sealer() -> None:
    """Give the sealer two hold points: `seal:written`, once a version's chunks are written and before `seal_batch`, and
    `seal:sealed`, after `seal_batch` and before the applies are submitted. The sealer has no hooks, so they wrap two of
    its steps, in this process only."""
    write, after_seal = sealer._write, sealer._after_seal  # pyright: ignore[reportPrivateUsage]

    def written(*args: Any, **kwargs: Any) -> Any:
        draft = write(*args, **kwargs)
        with _connect() as conn:
            pass_point(conn, "seal:written")
        return draft

    def sealed(*args: Any, **kwargs: Any) -> None:
        with _connect() as conn:
            pass_point(conn, "seal:sealed")
        after_seal(*args, **kwargs)

    sealer._write = written  # pyright: ignore[reportPrivateUsage]
    sealer._after_seal = sealed  # pyright: ignore[reportPrivateUsage]


if "FEATHER_CHAOS_URL" in os.environ:
    _hold_the_sealer()


class ChaosSource(simulation.Source):
    """Numbered rows, handed over a chunk at a time, one metered call per row if the connection has a budget.

    After each chunk's rows it passes the point `read:<connection>:<chunk number>`, by which time that chunk is in the
    spool. Each read leaves a row in `chaos.reads`.
    """

    spec = ConnectorSpec(name="chaos.source", strategies=frozenset({"replace"}))

    def __init__(self, connection: str, rows: int = ROWS) -> None:
        super().__init__(connection, generator={"id": simulation.seq()}, rows_per_partition=rows)

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        with _connect() as conn:
            conn.execute(
                "insert into chaos.reads (connection, who) values (%s, %s)", (self.connection, _who())
            )
            for batch in super().read(ctx, req):
                for start in range(0, len(batch), CHUNK_ROWS):
                    rows = batch[start : start + CHUNK_ROWS]
                    if ctx.budget is not None:
                        for _ in rows:
                            ctx.budget.acquire()
                    yield rows
                    pass_point(conn, f"read:{self.connection}:{start // CHUNK_ROWS}")


class SlowSink:
    """Applies a chunk slowly, in two halves, so a chunk can be caught half written.

    Two different things are recorded, and they answer different questions:

    `chaos.attempts` has a row for every call of `apply`, with no dedupe: what was *attempted*.

    `chaos.sink_rows` is the destination. It dedupes as a sink is told to, on the batch's idempotency key
    (`on conflict do nothing`), and it has a row per effective write. Its key includes the idempotency key on purpose: if
    it were only the record's key, no duplicate could ever show. A record written under two idempotency keys, because a
    version was delivered twice or a re-run put it in another chunk, has two rows here, which an idempotent sink would
    not have caught either. A repeat under the same idempotency key is what the sink is there to absorb, and has one.
    """

    spec = ConnectorSpec(name="chaos.slow_sink", modes=frozenset({"upsert"}))

    def __init__(self, connection: str) -> None:
        self.connection = connection

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        with _connect() as conn:
            attempt = conn.execute(
                """insert into chaos.attempts (route, idempotency_key, chunk_no, who) values (%s, %s, %s, %s)
                   returning id""",
                (batch.route, batch.idempotency_key, batch.chunk_no, _who()),
            ).fetchone()
            assert attempt is not None
            half = len(batch.records) // 2
            self._write(conn, batch, batch.records[:half])
            pass_point(conn, f"apply:{batch.route}:{batch.chunk_no}")
            time.sleep(SLOW_S)
            self._write(conn, batch, batch.records[half:])
            conn.execute("update chaos.attempts set finished = true where id = %s", (attempt[0],))
        return ApplyResult(len(batch.records))

    @staticmethod
    def _write(conn: psycopg.Connection[Any], batch: ApplyBatch, records: Any) -> None:
        conn.execute(
            """insert into chaos.sink_rows (route, idempotency_key, key)
               select %s, %s, unnest(%s::text[]) on conflict do nothing""",
            (batch.route, batch.idempotency_key, [str(record.key) for record in records]),
        )


def _flow(name: str, connection: str, sinks: list[str], rows: int = ROWS) -> fc.Flow:
    dataset = fc.Dataset(
        f"{name}_rows", source=ChaosSource(connection, rows), key="id", strategy=fc.strategy.replace()
    )
    return fc.Flow(name, reads=dataset, deliver=[fc.to(SlowSink(sink)) for sink in sinks])


main = _flow("main", "source", ["slow_a", "slow_b", "slow_c"])
fits = _flow("fits", "fits_source", ["fits_sink"], rows=2)  # two calls, and its budget holds two
too_big = _flow("too_big", "big_source", ["big_sink"], rows=5)  # five calls, and its budget holds three
# Feather's own simulation source: a package may not bring a push source of its own, which the server would run.
hooks = fc.Dataset(
    "hooks",
    source=simulation.Source(
        "hooks", generator={"id": simulation.seq()}, rows_per_partition=0, unsigned=True
    ),
    key="id",
    strategy=fc.strategy.push(),
)
push = fc.Flow("push", reads=hooks, deliver=[fc.to(SlowSink("push_sink"))], triggers=[fc.on_event(hooks)])

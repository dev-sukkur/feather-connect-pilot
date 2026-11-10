"""A flow that reports what ran it: each run reads `ROWS` fresh rows, so each run seals a version and delivers it, and
the sink writes each record once per idempotency key, with the code's version and featherlib's, to `hosted.rows`.

The sink's connection has a credential, `${secret:db:out/token}`, which the host gets from its supervisor; it notes
a digest of it with each attempt. A row in `hosted.fail_load` for a version makes that version's code fail to import in
a host (where `FEATHER_HOSTED_URL` is set), but not in the load check, which has a clean environment.

A *hold point* stops the sink mid-apply for a test to act at a known moment: a row in `hosted.holds` arms one, and the
process waits until the test releases its row in `hosted.reached`. The tables are made by `tests/support/hosts.py`.
"""

import hashlib
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg

import feather_connect as fc
from feather_connect.connectors import memory, simulation
from feather_connect.core.connector import ApplyBatch, ApplyResult, CheckResult, ConnectorSpec, ReadRequest
from feather_connect.core.context import Context

ROWS = 5
_YAML = Path(__file__).resolve().parents[1] / "feather.yaml"
CODE = next(
    line.split(":", 1)[1].strip()
    for line in _YAML.read_text(encoding="utf-8").splitlines()
    if line.startswith("version:")
)


def _lib() -> str | None:
    try:
        import featherlib  # pyright: ignore[reportMissingImports]
    except ImportError:
        return None
    return str(featherlib.VERSION)  # pyright: ignore[reportUnknownMemberType]


def _connect() -> psycopg.Connection[Any]:
    return psycopg.connect(
        os.environ["FEATHER_HOSTED_URL"],
        autocommit=True,
        connect_timeout=5,
        application_name=f"hosted-{os.getpid()}",
    )


def _fail_load_if_told() -> None:
    if not os.environ.get("FEATHER_HOSTED_URL"):
        return  # the load check
    with _connect() as conn:
        if conn.execute("select 1 from hosted.fail_load where code = %s", (CODE,)).fetchone():
            raise RuntimeError(f"hosted {CODE} was told not to load")


def pass_point(conn: psycopg.Connection[Any], point: str) -> None:
    armed = conn.execute(
        "update hosted.holds set remaining = remaining - 1 where point = %s and remaining > 0 returning point",
        (point,),
    ).fetchone()
    if armed is None:
        return
    row = conn.execute(
        "insert into hosted.reached (point, code, pid) values (%s, %s, %s) returning id",
        (point, CODE, os.getpid()),
    ).fetchone()
    assert row is not None
    while True:
        released = conn.execute("select released from hosted.reached where id = %s", (row[0],)).fetchone()
        if released is not None and released[0]:
            return
        time.sleep(0.05)


class HostedSource(simulation.Source):
    spec = ConnectorSpec(name="hosted.source", strategies=frozenset({"replace"}))

    def __init__(self, connection: str) -> None:
        super().__init__(connection, generator={"id": simulation.seq()}, rows_per_partition=ROWS)

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        stamp = time.time_ns()  # a new version every run
        for batch in super().read(ctx, req):
            yield [{**row, "stamp": stamp} for row in batch]


class HostedSink(memory.Sink):
    spec = ConnectorSpec(name="hosted.sink", modes=memory.Sink.spec.modes)

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        token = str(ctx.config.get("token") or "")
        with _connect() as conn:
            conn.execute(
                """insert into hosted.attempts (idempotency_key, code, pid, token, feather_env)
                   values (%s, %s, %s, %s, %s)""",
                (
                    batch.idempotency_key,
                    CODE,
                    os.getpid(),
                    hashlib.sha256(token.encode()).hexdigest()[:12],
                    ",".join(sorted(k for k in os.environ if k.startswith("FEATHER_"))),
                ),
            )
            pass_point(conn, f"apply:{CODE}")
            conn.execute(
                """insert into hosted.rows (route, idempotency_key, key, code, lib)
                   select %s, %s, unnest(%s::text[]), %s, %s on conflict do nothing""",
                (batch.route, batch.idempotency_key, [str(r.key) for r in batch.records], CODE, _lib()),
            )
        return ApplyResult(len(batch.records))


dataset = fc.Dataset("rows", source=HostedSource("src"), key="id", strategy=fc.strategy.replace())
flow = fc.Flow(
    "feed", reads=dataset, deliver=[fc.to(HostedSink("out"), name="out")], triggers=[fc.every("2s")]
)
_fail_load_if_told()

"""Side-effect routes (M2 plan 2 stage A2, design §6.5): an applied delivery is compensated on request, as a task,
each chunk once; only a side-effect route's sink can, and only an applied delivery."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import ApplyBatch, ApplyResult, ConnectorSpec
from feather_connect.core.context import Context
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import InvalidTransition
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 11, 12, 10, tzinfo=UTC)


class Postings(Sink):
    spec: ClassVar[ConnectorSpec] = replace(Sink.spec, name="test.postings", side_effect=True)

    def __init__(self, connection: str) -> None:
        super().__init__(connection)
        self.reversed: list[tuple[str, Any]] = []

    def compensate(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        self.reversed += [(batch.idempotency_key, r.key) for r in batch.records]
        return ApplyResult(len(batch.records))


def _world(sink: Any) -> tuple[Runtime, LocalEngine, MemoryStore]:
    clock = FrozenClock(T0)
    store, engine = MemoryStore(clock), LocalEngine(clock)
    ds = fc.Dataset(
        "orders", source=Source(rows=[{"id": 1}, {"id": 2}]), key="id", strategy=fc.strategy.replace()
    )
    flow = fc.Flow("f", reads=ds, deliver=[fc.to(sink, name="sap")])
    rt = Runtime([flow], store, MemorySpool(clock), engine, code_sha="sha", clock=clock)
    engine.register(rt.tasks())
    return rt, engine, store


def _applied(rt: Runtime, engine: LocalEngine, store: MemoryStore) -> str:
    run = rt.start_run("f")
    engine.run_until_idle()
    [version] = store.versions.sealed_for_run(run, "orders")
    assert store.ledger.get("sap", version.version_id).status == "applied"
    return version.version_id


def test_an_applied_posting_is_compensated_once_on_request():
    sink = Postings("sap")
    rt, engine, store = _world(sink)
    v = _applied(rt, engine, store)
    rt.compensate("sap", v, actor="ann", reason="posted twice by hand")
    engine.run_until_idle()
    assert sorted(k for _, k in sink.reversed) == ["1", "2"]
    delivery = store.ledger.get("sap", v)
    assert delivery.status == "compensated"
    [last] = store.ledger.attempts("sap", v)[-1:]
    assert last.actor == "ann" and last.reason == "compensated 2 records: posted twice by hand"
    with pytest.raises(InvalidTransition):
        rt.compensate("sap", v, actor="ann")  # once


def test_a_route_that_isnt_a_side_effect_route_cant_compensate():
    rt, engine, store = _world(Sink("sap"))
    v = _applied(rt, engine, store)
    with pytest.raises(InvalidTransition, match="not a side-effect route"):
        rt.compensate("sap", v, actor="ann")

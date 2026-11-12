"""Approvals on a route (M2 plan 2 stage A1): a delivery with a record that needs approval is held before anything
of it is applied; approved, it applies as it is; rejected, it is skipped and the next version goes ahead."""

import logging
from datetime import UTC, datetime
from typing import Any

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.context import Context
from feather_connect.engine.local import LocalEngine
from feather_connect.engine.port import TaskCall
from feather_connect.runtime.apply import approved, run_apply
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import run_extract
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 11, 12, 10, tzinfo=UTC)


class World:
    def __init__(self, approval: Any) -> None:
        clock = FrozenClock(T0)
        self.deps = RuntimeDeps(
            MemoryStore(clock),
            MemorySpool(),
            clock,
            "sha",
            lambda c: Context({}, logging.getLogger(c), clock.now(), "UTC"),
        )
        self.engine = LocalEngine(clock)
        self.sink = Sink("sap")
        self.ds = fc.Dataset("orders", source=Source(), key="id", strategy=fc.strategy.replace())
        self.route = fc.to(self.sink, name="sap", approval=approval)
        self.n = 0

    def seal(self, rows: list[dict[str, Any]]) -> str:
        self.n += 1
        self.ds.source.load(rows)
        [version] = run_extract(
            self.ds, f"r{self.n}", "_", routes=[self.route], stage_only=False, deps=self.deps
        )
        return version.version_id

    def apply(self, version_id: str) -> None:
        name = f"apply-{len(self.engine.submitted())}"
        self.engine.register({name: lambda ctx, a: run_apply(ctx, self.ds, self.route, a["v"], self.deps)})
        self.engine.submit(TaskCall(name, {"v": version_id}, name, retry=self.deps.retry))
        self.engine.run_until_idle()

    def status(self, version_id: str) -> str:
        return self.deps.store.ledger.get("sap", version_id).status


BIG = fc.approval.when(lambda r: r["total"] > 500, reason="over 500")


def test_a_delivery_with_a_big_order_is_held_and_applies_nothing_until_approved():
    w = World(BIG)
    v = w.seal([{"id": 1, "total": 100}, {"id": 2, "total": 900}])
    w.apply(v)
    assert w.status(v) == "held" and not w.sink.rows
    [held] = [a for a in w.deps.store.ledger.attempts("sap", v) if a.to_status == "held"]
    assert held.reason == "approval: 1 record to approve (over 500)"
    w.deps.store.ledger.transition("sap", v, "pending", actor="ann", reason="approved: checked")
    assert approved("sap", v, w.deps)
    w.apply(v)
    assert w.status(v) == "applied" and len(w.sink.rows) == 2  # all of it, as it was


def test_a_delivery_with_nothing_to_approve_applies_at_once():
    w = World(BIG)
    v = w.seal([{"id": 1, "total": 100}])
    w.apply(v)
    assert w.status(v) == "applied"


def test_a_held_delivery_blocks_the_next_and_a_rejected_one_lets_it_go():
    w = World(fc.approval.always())
    first, second = w.seal([{"id": 1, "total": 1}]), w.seal([{"id": 1, "total": 2}])
    w.apply(first)
    w.apply(second)
    assert (w.status(first), w.status(second)) == ("held", "pending")  # order holds (I2)
    w.deps.store.ledger.transition("sap", first, "skipped", actor="ann", reason="rejected: wrong")
    w.apply(second)
    assert w.status(second) == "held"  # the next needs its own approval


def test_the_spec_names_the_approval():
    flow = fc.Flow("f", reads=World(BIG).ds, deliver=[World(BIG).route])
    assert flow.spec_dict()["routes"][0]["approval"] == {"kind": "when", "reason": "over 500"}

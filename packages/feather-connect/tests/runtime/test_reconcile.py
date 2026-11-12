"""Reconcile (M2 plan 2 D3, design H5): a `by_stamp` read misses a row stamped before its watermark, such as one
back-dated into a closed period; a scheduled reconcile reads its lookback again and finds it, and nothing else."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.triggers import Reconcile
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime import scheduler
from feather_connect.runtime.catalogue import triggers_of
from feather_connect.runtime.extract import RECONCILE
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 11, 12, 10, tzinfo=UTC)


class World:
    def __init__(self) -> None:
        self.clock = FrozenClock(T0)
        self.store, self.engine, self.sink = MemoryStore(self.clock), LocalEngine(self.clock), Sink("erp")
        self.source = Source(rows=[])
        ds = fc.Dataset(
            "orders", source=self.source, key="id", strategy=fc.strategy.by_stamp("u", lookback="1m")
        )
        self.flow = fc.Flow(
            "f",
            reads=ds,
            deliver=[fc.to(self.sink, name="erp")],
            triggers=[fc.every("1h"), fc.reconcile(every="1d", lookback="30d")],
        )
        self.rt = Runtime(
            [self.flow], self.store, MemorySpool(self.clock), self.engine, code_sha="s", clock=self.clock
        )
        self.engine.register(self.rt.tasks())

    def applied(self) -> list[int]:
        return [d.rows_applied for d in self.store.ledger.list_deliveries(route="erp")]


def test_a_back_dated_row_is_found_by_the_reconcile_and_nothing_else_is_sent_again():
    w = World()
    rows: list[dict[str, Any]] = [{"id": i, "u": T0 - timedelta(hours=i)} for i in range(1, 4)]
    w.source.load(rows)
    scheduler.tick(w.rt, now=w.clock.now())  # notes the start
    w.rt.start_run("f")
    w.engine.run_until_idle()
    assert w.applied() == [3]
    rows.append({"id": 9, "u": T0 - timedelta(days=10)})  # back-dated into a closed period
    w.source.load(rows)
    w.clock.advance(timedelta(hours=1))
    [hourly] = scheduler.tick(w.rt, now=w.clock.now())
    w.engine.run_until_idle()
    assert hourly.trigger_id == "every:3600s" and w.applied() == [
        3
    ]  # the hourly read starts at its watermark
    w.clock.advance(timedelta(hours=23))
    found = scheduler.tick(w.rt, now=w.clock.now())
    w.engine.run_until_idle()
    [check] = [r for r in found if r.trigger_id.startswith("reconcile:")]
    assert w.store.runs.get_run(check.run_id).params[RECONCILE] == 30 * 86400
    assert w.applied() == [3, 1] and sorted(r["id"] for r in w.sink.rows) == [1, 2, 3, 9]


def test_reconcile_on_a_flow_with_no_by_stamp_dataset_is_a_problem():
    ds = fc.Dataset("d", source=Source(), key="id", strategy=fc.strategy.replace())
    flow = fc.Flow(
        "f", reads=ds, deliver=[fc.to(Sink("s"))], triggers=[fc.reconcile(every="1d", lookback="7d")]
    )
    assert [p.code for p in flow.validate()] == ["reconcile_needs_by_stamp"]


def test_a_reconcile_trigger_goes_through_a_flows_spec():
    w = World()
    [_, raw] = w.flow.spec_dict()["triggers"]
    assert raw == {"kind": "reconcile", "seconds": 86400.0, "lookback": 2592000.0}
    assert triggers_of(w.flow.spec_dict())[1] == Reconcile(timedelta(days=1), timedelta(days=30))
    assert (
        scheduler.trigger_id(Reconcile(timedelta(days=1), timedelta(days=30))) == "reconcile:86400s:2592000s"
    )


def test_reconcile_needs_durations_above_zero():
    with pytest.raises(ValueError, match="above zero"):
        fc.reconcile(every="0s", lookback="1d")

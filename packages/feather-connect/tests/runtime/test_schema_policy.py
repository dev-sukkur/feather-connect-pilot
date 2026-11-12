"""The schema policy (M2 plan 2 D1, design §4.7, H12): against the last version a route applied, an added field or a
widened type applies, and a dropped field or a changed type holds the delivery until an approver lets it go."""

from datetime import UTC, datetime
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.schema_policy import SchemaPolicy, judge
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 11, 12, 10, tzinfo=UTC)


class World:
    def __init__(
        self, dataset_policy: SchemaPolicy = "default", route_policy: SchemaPolicy | None = None
    ) -> None:
        clock = FrozenClock(T0)
        self.store, self.engine, self.sink = MemoryStore(clock), LocalEngine(clock), Sink("erp")
        self.source = Source(rows=[])
        ds = fc.Dataset(
            "orders",
            source=self.source,
            key="id",
            strategy=fc.strategy.replace(),
            schema_policy=dataset_policy,
        )
        flow = fc.Flow("f", reads=ds, deliver=[fc.to(self.sink, name="erp", schema_policy=route_policy)])
        self.rt = Runtime([flow], self.store, MemorySpool(clock), self.engine, code_sha="sha", clock=clock)
        self.engine.register(self.rt.tasks())

    def run(self, rows: list[dict[str, Any]]) -> tuple[str, str]:
        self.source.load(rows)
        run = self.rt.start_run("f")
        self.engine.run_until_idle()
        [version] = self.store.versions.sealed_for_run(run, "orders")
        delivery = self.store.ledger.get("erp", version.version_id)
        return version.version_id, delivery.status

    def reason(self, version_id: str) -> str:
        [held] = [a for a in self.store.ledger.attempts("erp", version_id) if a.to_status == "held"]
        return held.reason or ""


def test_an_added_field_applies_and_a_dropped_one_holds_until_approved():
    w = World()
    assert w.run([{"id": 1, "name": "a", "city": "x"}])[1] == "applied"
    assert w.run([{"id": 1, "name": "b", "city": "x", "phone": "1"}])[1] == "applied"  # added
    v, status = w.run([{"id": 1, "name": "c", "phone": "1"}])  # city dropped
    assert status == "held" and w.reason(v).startswith("schema_change: dropped city (against version ")
    assert w.sink.rows[-1]["name"] == "b"  # nothing of it reached the sink
    w.rt.approve("erp", v, actor="ann", reason="city moved to the address")
    w.engine.run_until_idle()
    assert w.store.ledger.get("erp", v).status == "applied" and w.sink.rows[-1]["name"] == "c"
    assert w.run([{"id": 1, "name": "d", "phone": "1"}])[1] == "applied"  # against the approved one now


def test_a_changed_type_holds_and_a_widened_one_applies():
    w = World()
    w.run([{"id": 1, "qty": 1}])
    assert w.run([{"id": 1, "qty": 2.5}])[1] == "applied"  # int64 to double widens
    v, status = w.run([{"id": 1, "qty": "three"}])
    assert status == "held" and "changed qty: double → string" in w.reason(v)


def test_a_field_null_in_every_row_says_nothing_of_its_type():
    w = World()
    w.run([{"id": 1, "note": "x"}])
    assert w.run([{"id": 1, "note": None}])[1] == "applied"
    assert w.run([{"id": 1, "note": "y"}])[1] == "applied"


def test_a_held_delivery_rejected_lets_the_next_go():
    w = World()
    w.run([{"id": 1, "a": 1, "b": 1}])
    v, _ = w.run([{"id": 1, "a": 2}])
    w.rt.reject("erp", v, actor="ann", reason="the source lost b")
    w.engine.run_until_idle()
    assert w.store.ledger.get("erp", v).status == "skipped"
    assert w.run([{"id": 1, "a": 3, "b": 3}])[1] == "applied"  # against the last applied, which has b


@pytest.mark.parametrize(
    ("dataset_policy", "route_policy", "rows", "status"),
    [
        ("strict", None, {"id": 1, "a": 1, "extra": 1}, "held"),  # strict: an added field holds too
        ("permissive", None, {"id": 1}, "applied"),  # permissive: a dropped field passes
        ("strict", "permissive", {"id": 1}, "applied"),  # the route's overrides the dataset's
        ("permissive", "default", {"id": 1}, "held"),
    ],
)
def test_the_policy_is_the_routes_or_else_the_datasets(
    dataset_policy: SchemaPolicy, route_policy: SchemaPolicy | None, rows: dict[str, Any], status: str
):
    w = World(dataset_policy, route_policy)
    w.run([{"id": 1, "a": 1}])
    assert w.run([rows])[1] == status


def test_a_policy_of_its_own_says_why():
    def no_money_changes(before: Any, after: Any) -> str | None:
        return "amount changed type" if before.get("amount") != after.get("amount") else None

    w = World(no_money_changes)
    w.run([{"id": 1, "amount": 1}])
    assert w.run([{"id": 1, "amount": 1, "other": 1}])[1] == "applied"
    v, status = w.run([{"id": 1, "amount": "1"}])
    assert status == "held" and w.reason(v).startswith("schema_change: amount changed type")


def test_judge_names_each_change():
    before = {"a": "int64", "b": "string", "c": "int32"}
    after = {"a": "string", "c": "int64", "d": "bool"}
    assert judge("default", before, after) == "schema_change: dropped b; changed a: int64 → string"
    assert judge("strict", before, after) == (
        "schema_change: added d; dropped b; changed a: int64 → string; c: int32 → int64"
    )
    assert judge("permissive", before, after) is None

"""A route added to a live flow (M2 plan 2 D2, H11): its first delivery in each partition is the whole slice read, from
the beginning for a `by_stamp` dataset, while the routes it joins get only what changed; then it follows live."""

from datetime import UTC, datetime, timedelta
from typing import Any

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

T0 = datetime(2026, 11, 12, 10, tzinfo=UTC)


class World:
    def __init__(self, strategy: Any, partition: Any = None) -> None:
        self.clock = FrozenClock(T0)
        self.store, self.spool = MemoryStore(self.clock), MemorySpool(self.clock)
        self.source = Source(rows=[])
        self.ds = fc.Dataset(
            "orders",
            source=self.source,
            key="id",
            strategy=strategy,
            **({"partition": partition} if partition is not None else {}),
        )
        self.sinks: dict[str, Sink] = {}
        self.deploy("a")

    def deploy(self, *routes: str) -> None:
        """Run the flow's code with these routes from now on, as a new deployment would."""
        for name in routes:
            self.sinks.setdefault(name, Sink(name))
        flow = fc.Flow("f", reads=self.ds, deliver=[fc.to(self.sinks[n], name=n) for n in routes])
        self.engine = LocalEngine(self.clock)
        self.rt = Runtime([flow], self.store, self.spool, self.engine, code_sha="sha", clock=self.clock)
        self.engine.register(self.rt.tasks())

    def run(self, rows: list[dict[str, Any]]) -> None:
        self.clock.advance(timedelta(minutes=1))
        self.source.load(rows)
        run = self.rt.start_run("f")
        self.engine.run_until_idle()
        assert self.store.runs.get_run(run).status == "succeeded"

    def got(self, route: str) -> list[Any]:
        return sorted(r["id"] for r in self.sinks[route].rows)


def test_a_route_added_to_a_by_stamp_flow_gets_every_row_then_only_changes():
    w = World(fc.strategy.by_stamp("u"))
    rows = [{"id": i, "u": T0 + timedelta(seconds=i)} for i in range(1, 4)]
    w.run(rows)
    rows.append({"id": 4, "u": T0 + timedelta(minutes=2)})
    w.run(rows)
    assert w.got("a") == [1, 2, 3, 4]
    w.deploy("a", "b")
    w.run(rows)  # nothing changed: a gets nothing more, b gets all of it
    a = w.store.ledger.list_deliveries(route="a")
    assert sum(d.rows_applied for d in a) == 4  # no row twice
    assert w.got("b") == [1, 2, 3, 4]
    rows.append({"id": 5, "u": T0 + timedelta(minutes=5)})
    w.run(rows)  # then live: both get the new row once
    assert w.got("a") == [1, 2, 3, 4, 5] and w.got("b") == [1, 2, 3, 4, 5]
    assert [d.status for d in w.store.ledger.list_deliveries(route="b")] == ["applied", "applied"]


def test_a_route_added_to_a_partitioned_snapshot_flow_gets_each_partition_whole():
    w = World(fc.strategy.snapshot_diff(), fc.partition.by_bucket("id", 2))
    rows = [{"id": i, "v": 1} for i in range(1, 5)]
    w.run(rows)
    first = len(w.store.ledger.list_deliveries(route="a"))
    w.deploy("a", "b")
    rows[1]["v"] = 2
    w.run(rows)
    b = w.store.ledger.list_deliveries(route="b")
    assert w.got("b") == [1, 2, 3, 4] and sum(d.rows_applied for d in b) == 4  # each partition whole
    assert len({d.partition for d in b}) == 2
    later = w.store.ledger.list_deliveries(route="a")[first:]
    assert sum(d.rows_applied for d in later) == 1  # the routes it joined: only the change


def test_a_new_flow_keeps_no_full_chunks_for_a_bootstrap():
    w = World(fc.strategy.by_stamp("u"))
    w.run([{"id": 1, "u": T0}])
    [delivery] = w.store.ledger.list_deliveries(route="a")
    assert w.spool.list_chunks(delivery.version_id, "full") == []

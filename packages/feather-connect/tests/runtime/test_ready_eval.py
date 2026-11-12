"""The readiness evaluator (M2 plan 1, tasks 3–7) on the memory store: it fires each ready period once, per scope,
from the catalogue spec and what the store holds, and reruns a period once per late change."""

from datetime import UTC, datetime, timedelta

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory
from feather_connect.project import prefixed
from feather_connect.runtime.catalogue import ActiveFlow
from feather_connect.runtime.ready_eval import evaluate, periods_of
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import VersionDraft

TZ = "Asia/Kolkata"
DAY_END = datetime(2026, 11, 8, 18, 30, tzinfo=UTC)  # 9 Nov 00:00 in Kolkata: the end of 8 Nov there
STORE_DAY = fc.partition.by_fields("store_id", fc.partition.by_day("date", tz=TZ))


def dataset(name, partition=STORE_DAY, strategy=None):
    return fc.Dataset(
        name,
        source=memory.Source(connection=f"src_{name}"),
        key="id",
        strategy=strategy or fc.strategy.replace(),
        partition=partition,
    )


def active(*flows):
    return [
        ActiveFlow(f.name, "acme", "1.0.0", (), f.overlap, f.catch_up, True, frozenset(), f.spec_dict())
        for f in prefixed(flows, "acme")
    ]


class World:
    def __init__(self, start: datetime) -> None:
        self.clock = FrozenClock(start)
        self.store = MemoryStore(self.clock)
        self.n = 0

    def seal(self, dataset: str, partition: str, routes=(), run_status="succeeded") -> tuple[str, str]:
        """A run made now that seals one version of `acme/<dataset>`'s `partition`."""
        self.n += 1
        run = self.store.runs.create_run("acme/ingest", "every", "test", {})
        version = f"v{self.n}"
        self.store.versions.stage(
            VersionDraft(version, f"acme/{dataset}", partition, run, True, 1, 0, "h", None, None, "sha")
        )
        self.store.versions.seal([version], [f"acme/{r}" for r in routes])
        self.store.runs.set_run_status(run, run_status)
        return run, version

    def evaluate(self, *flows):
        return evaluate(self.store, active(*flows), self.clock.now())

    def ready_params(self, run_id):
        return self.store.runs.get_run(run_id).params["ready"]


invoices, returns, payments = dataset("invoices"), dataset("returns"), dataset("payments")
ingest = fc.Flow(
    "ingest", reads=[invoices, returns, payments], deliver=[fc.to(memory.Sink(connection="staging"))]
)


def posting(**options):
    trigger = fc.on_ready(
        fc.ready([invoices, returns, payments], by="store_id"),
        each="day",
        tz=TZ,
        start=datetime(2026, 11, 8, tzinfo=UTC).astimezone(UTC).replace(hour=0)
        - timedelta(hours=5, minutes=30),
        **options,
    )
    return fc.Flow(
        "posting",
        reads=dataset("totals", fc.partition.none()),
        deliver=[fc.to(memory.Sink(connection="erp"))],
        triggers=[trigger],
    )


def part(store_id):
    return f"store_id={store_id}/date=2026-11-08"


def test_periods_are_the_days_or_hours_of_the_time_zone():
    days = periods_of("day", TZ, DAY_END - timedelta(days=1), DAY_END + timedelta(hours=1))
    assert [(p.id, p.end) for p in days] == [("2026-11-08", DAY_END)]
    hours = periods_of("hour", TZ, DAY_END - timedelta(hours=2), DAY_END)
    assert [p.id for p in hours] == ["2026-11-08T22:00", "2026-11-08T23:00"]


def test_a_store_day_fires_once_all_three_datasets_are_read_after_it_ended_and_each_store_on_its_own():
    world = World(DAY_END - timedelta(hours=1))
    flows = (ingest, posting())
    for name in ("invoices", "returns", "payments"):
        world.seal(name, part(1509))  # read before the day ended: not complete
    world.clock.advance(timedelta(hours=2))
    assert world.evaluate(*flows) == []
    for name in ("invoices", "returns", "payments"):
        world.seal(name, part(1509))
    for name in ("invoices", "returns"):
        world.seal(name, part(1600))  # payments of store 1600 are late
    [run] = world.evaluate(*flows)
    assert world.evaluate(*flows) == []  # once
    info = world.ready_params(run)
    assert (info["period"], info["scope"], info["reason"]) == ("2026-11-08", {"store_id": "1509"}, "ready")
    assert set(info["versions"]) == {"acme/invoices", "acme/returns", "acme/payments"}
    world.seal("payments", part(1600))
    [later] = world.evaluate(*flows)
    assert world.ready_params(later)["scope"] == {"store_id": "1600"}
    assert world.store.runs.get_run(later).params["package"] == "acme"


def test_a_late_version_reruns_the_period_once_with_rerun_and_not_with_warn():
    for on_late, reruns in (("rerun", 1), ("warn", 0)):
        world = World(DAY_END + timedelta(hours=1))
        flows = (ingest, posting(on_late=on_late))
        for name in ("invoices", "returns", "payments"):
            world.seal(name, part(1509))
        assert len(world.evaluate(*flows)) == 1
        world.clock.advance(timedelta(hours=1))
        world.seal("invoices", part(1509))  # an invoice edited after the posting
        late = world.evaluate(*flows)
        assert len(late) == reruns and world.evaluate(*flows) == []
        if late:
            assert world.ready_params(late[0])["reason"] == "late data"
            assert [f.seq for f in world.store.readiness.firings("acme/posting")] == [1, 0]


def test_each_run_fires_after_the_upstream_run_is_delivered_to_staging():
    world = World(DAY_END)
    staged = fc.Flow(
        "to-sap",
        reads=dataset("staged", fc.partition.none()),
        deliver=[fc.to(memory.Sink(connection="sap"))],
        triggers=[fc.on_ready(fc.ready(invoices, delivered_to="staging"))],
    )
    run, version = world.seal("invoices", part(1509), routes=["staging"])
    assert world.evaluate(ingest, staged) == []  # staging hasn't applied it
    world.store.ledger.transition("acme/staging", version, "running", actor="t")
    world.store.ledger.transition("acme/staging", version, "applied", actor="t")
    [fired] = world.evaluate(ingest, staged)
    assert world.ready_params(fired)["runs"] == [run] and world.evaluate(ingest, staged) == []


def test_at_most_every_gathers_the_runs_that_got_ready_meanwhile():
    world = World(DAY_END)
    gathered = fc.Flow(
        "warehouse",
        reads=dataset("staged", fc.partition.none()),
        deliver=[fc.to(memory.Sink(connection="dw"))],
        triggers=[fc.on_ready(fc.ready(invoices), at_most_every="4h")],
    )
    first, _ = world.seal("invoices", part(1))
    assert len(world.evaluate(ingest, gathered)) == 1
    second, _ = world.seal("invoices", part(2))
    third, _ = world.seal("invoices", part(3))
    world.clock.advance(timedelta(hours=1))
    assert world.evaluate(ingest, gathered) == []  # within 4 h of the last
    world.clock.advance(timedelta(hours=3))
    [run] = world.evaluate(ingest, gathered)
    assert world.ready_params(run)["runs"] == [second, third]


def test_a_push_dataset_is_ready_once_its_events_are_sealed_and_settled_or_closed():
    hooks = dataset("hooks", fc.partition.none(), fc.strategy.push())
    hooks_flow = fc.Flow("hooks", reads=hooks, deliver=[fc.to(memory.Sink(connection="staging"))])
    waiting = fc.Flow(
        "after-hooks",
        reads=dataset("x", fc.partition.none()),
        deliver=[fc.to(memory.Sink(connection="y"))],
        triggers=[fc.on_ready(fc.ready(hooks), each="hour", tz="UTC", start=DAY_END - timedelta(minutes=30))],
    )
    world = World(DAY_END + timedelta(minutes=31))  # 19:01 UTC: the hour from 18:00 ended a minute ago
    assert world.evaluate(hooks_flow, waiting) == []  # the hour ended a minute ago: it settles for 5
    world.store.readiness.close("acme/hooks", "_", "ops", "the till closed", world.clock.now())
    [fired] = world.evaluate(hooks_flow, waiting)
    assert world.ready_params(fired)["period"] == "2026-11-08T18:00"


def test_an_upstream_that_is_not_active_never_fires():
    world = World(DAY_END + timedelta(hours=1))
    for name in ("invoices", "returns", "payments"):
        world.seal(name, part(1509))
    assert world.evaluate(posting()) == []  # ingest, which reads them, isn't active

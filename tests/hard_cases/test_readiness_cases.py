"""Hard cases of readiness (design §16, readiness design §11) on a real Postgres: H13, H16 and H17.

They drive the evaluator the server runs, against the store a server and its package hosts share. What a firing queues
is a run tagged with its package version, which that version's host takes as it takes a clock tick's (chaos tests
`tests/chaos`), so these stop at the queued run.
"""

import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from real_stack import scratch_database, server_url

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory
from feather_connect.project import prefixed
from feather_connect.runtime.catalogue import ActiveFlow
from feather_connect.runtime.ready_eval import evaluate
from feather_connect.storage.port import VersionDraft
from feather_connect.storage.postgres.connect import connect
from feather_connect.storage.postgres.migrate import migrate

pytestmark = [pytest.mark.postgres]

DAY_END = datetime(2026, 11, 9, tzinfo=UTC)
STORE_DAY = fc.partition.by_fields("store_id", fc.partition.by_day("date", tz="UTC"))


def _dataset(name: str, partition=STORE_DAY) -> fc.Dataset:
    return fc.Dataset(
        name,
        source=memory.Source(connection=f"src_{name}"),
        key="id",
        strategy=fc.strategy.replace(),
        partition=partition,
    )


headers, lines = _dataset("headers"), _dataset("lines")
INGEST = fc.Flow("ingest", reads=[headers, lines], deliver=[fc.to(memory.Sink(connection="staging"))])
LINES_ONLY = fc.Flow(  # H13: waits for lines alone
    "lines-to-erp",
    reads=_dataset("line_totals", fc.partition.none()),
    deliver=[fc.to(memory.Sink(connection="erp"))],
    triggers=[
        fc.on_ready(fc.ready(lines, by="store_id"), each="day", tz="UTC", start=DAY_END - timedelta(days=1))
    ],
)
BOTH = fc.Flow(  # waits for both, by store, and reruns on late data
    "posting",
    reads=_dataset("totals", fc.partition.none()),
    deliver=[fc.to(memory.Sink(connection="erp2"))],
    triggers=[
        fc.on_ready(
            fc.ready([headers, lines], by="store_id"), each="day", tz="UTC", start=DAY_END - timedelta(days=1)
        )
    ],
)
FLOWS = [
    ActiveFlow(f.name, "acme", "1.0.0", (), f.overlap, f.catch_up, True, frozenset(), f.spec_dict())
    for f in prefixed([INGEST, LINES_ONLY, BOTH], "acme")
]


class Db:
    def __init__(self, url: str) -> None:
        self.clock = FrozenClock(DAY_END + timedelta(hours=1))
        self.store, _ = connect(url, clock=self.clock)
        self.n = 0

    def seal(self, dataset: str, store_id: int) -> None:
        self.n += 1
        run = self.store.runs.create_run("acme/ingest", "every", "test", {})
        version = f"{dataset}-{store_id}-{self.n}"
        partition = f"store_id={store_id}/date=2026-11-08"
        self.store.versions.stage(
            VersionDraft(version, f"acme/{dataset}", partition, run, True, 1, 0, "h", None, None, "s")
        )
        self.store.versions.seal([version], ["acme/staging"])
        self.store.runs.set_run_status(run, "succeeded")

    def fired(self, flow: str) -> list[tuple[str, int, str]]:
        return sorted((f.scope, f.seq, f.reason) for f in self.store.readiness.firings(f"acme/{flow}"))


@pytest.fixture
def db() -> Iterator[Db]:
    with scratch_database(server_url()) as url:
        migrate(url)
        found = Db(url)
        try:
            yield found
        finally:
            found.store.close()  # type: ignore[attr-defined]


def test_h13_lines_are_never_blocked_by_headers_and_readiness_gates_only_where_configured(db: Db):
    db.seal("lines", 1509)  # its headers haven't come
    evaluate(db.store, FLOWS, db.clock.now())
    assert db.fired("lines-to-erp") == [("store_id=1509", 0, "ready")]  # waits for lines only
    assert db.fired("posting") == []  # waits for both
    db.seal("headers", 1509)
    evaluate(db.store, FLOWS, db.clock.now())
    assert db.fired("posting") == [("store_id=1509", 0, "ready")]
    assert [r.flow for r in db.store.runs.list_runs("acme/ingest")] == [
        "acme/ingest"
    ] * 2  # ingest isn't gated


def test_h16_servers_racing_to_fire_one_period_queue_one_run(db: Db):
    db.seal("headers", 1509)
    db.seal("lines", 1509)
    runs: list[str] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def server() -> (
        None
    ):  # each a server whose evaluator reads the same store, as after a leader is killed mid-fire
        try:
            found = evaluate(db.store, FLOWS, db.clock.now())
            with lock:
                runs.extend(found)
        except BaseException as error:  # noqa: BLE001 - reported below
            errors.append(error)

    threads = [threading.Thread(target=server) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not errors
    posting = [r for r in runs if db.store.runs.get_run(r).flow == "acme/posting"]
    assert len(posting) == 1 and db.fired("posting") == [("store_id=1509", 0, "ready")]
    assert [
        r.run_id for r in db.store.runs.list_runs("acme/posting")
    ] == posting  # and no run without its firing


def test_h17_a_late_version_for_a_fired_period_reruns_it_exactly_once_and_holds_it(db: Db):
    db.seal("headers", 1509)
    db.seal("lines", 1509)
    [first] = [
        r
        for r in evaluate(db.store, FLOWS, db.clock.now())
        if db.store.runs.get_run(r).flow == "acme/posting"
    ]
    db.clock.advance(timedelta(hours=2))
    db.seal("lines", 1509)  # a line edited after the posting
    late = db.n
    reruns = [
        r
        for r in evaluate(db.store, FLOWS, db.clock.now())
        if db.store.runs.get_run(r).flow == "acme/posting"
    ]
    reruns += [
        r
        for r in evaluate(db.store, FLOWS, db.clock.now())
        if db.store.runs.get_run(r).flow == "acme/posting"
    ]
    assert len(reruns) == 1 and reruns[0] != first
    ready = db.store.runs.get_run(reruns[0]).params["ready"]
    assert ready["reason"] == "late data"
    assert ready["versions"]["acme/lines"]["store_id=1509/date=2026-11-08"] == f"lines-1509-{late}"
    assert db.fired("posting") == [("store_id=1509", 0, "ready"), ("store_id=1509", 1, "late data")]

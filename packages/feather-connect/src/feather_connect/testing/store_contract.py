"""The contract every store and spool backend must pass.

A backend's test module subclasses `StoreContract` and defines the fixtures `store`, `spool` and `clock`.
`clock` is the `FrozenClock` that `store` was built with, so tests can check the times it records.
"""

import pickle
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from feather_connect.clock import FrozenClock
from feather_connect.core.records import Record, row_hash
from feather_connect.storage.port import (
    CLOSED,
    TRANSITIONS,
    DeliveryStatus,
    InvalidTransition,
    StateRow,
    Store,
    VersionDraft,
    VersionSealed,
    delivery_cursor,
)
from feather_connect.storage.spool import Spool, records_to_table, snapshot_table, table_to_records

T0 = datetime(2026, 10, 18, 9, 0, tzinfo=UTC)


def draft(
    vid: str = "v1",
    ds: str = "d",
    part: str = "p",
    run: str = "r1",
    complete: bool = True,
    **kw: Any,
) -> VersionDraft:
    return VersionDraft(vid, ds, part, run, complete, kw.get("up", 1), 0, "h", None, kw.get("wm"), "sha")


def seal(store: Store, vid: str, routes: list[str], **kw: Any) -> None:
    store.versions.stage(draft(vid, **kw))
    store.versions.seal([vid], routes)


def upserts(*pairs: tuple[str, dict[str, Any]]) -> list[Record]:
    return [Record(key, "upsert", payload, row_hash(payload)) for key, payload in pairs]


class StoreContract:
    # versions and the ledger

    def test_seal_creates_one_pending_delivery_per_route(self, store: Store):
        store.versions.stage(draft())
        store.versions.seal(["v1"], ["a", "b"])
        assert (
            store.ledger.get("a", "v1").status == "pending"
            and store.ledger.get("b", "v1").status == "pending"
        )

    def test_seal_numbers_versions_in_order_and_stamps_the_time(self, store: Store, clock: FrozenClock):
        store.versions.stage(draft("v1"))
        store.versions.stage(draft("v2", part="q"))
        clock.advance(timedelta(minutes=5))
        sealed = store.versions.seal(["v2", "v1"], ["a"])
        assert [v.version_id for v in sealed] == ["v2", "v1"]
        assert sealed[0].seal_seq is not None and sealed[1].seal_seq == sealed[0].seal_seq + 1
        assert sealed[0].status == "sealed" and sealed[0].sealed_at == T0 + timedelta(minutes=5)
        assert store.versions.get("v1") == sealed[1]
        assert store.ledger.get("a", "v1").seal_seq == sealed[1].seal_seq

    def test_sealed_version_is_immutable(self, store: Store):  # I1
        store.versions.stage(draft())
        store.versions.seal(["v1"], ["a"])
        with pytest.raises(VersionSealed):
            store.versions.stage(draft(up=99))
        assert store.versions.get("v1").upserts == 1

    def test_a_staged_version_can_be_staged_again(self, store: Store):
        store.versions.stage(draft(up=1))
        assert store.versions.stage(draft(up=2)).upserts == 2
        assert store.versions.get("v1").status == "staged" and store.versions.get("v1").seal_seq is None

    def test_seal_is_idempotent(self, store: Store):
        store.versions.stage(draft())
        first = store.versions.seal(["v1"], ["a"])
        second = store.versions.seal(["v1"], ["a"])
        assert first[0].seal_seq == second[0].seal_seq and len(store.ledger.attempts("a", "v1")) == 0
        assert len(store.ledger.list_deliveries()) == 1

    def test_seal_of_an_unknown_version_changes_nothing(self, store: Store):
        store.versions.stage(draft("v1"))
        with pytest.raises(KeyError):
            store.versions.seal(["v1", "nope"], ["a"])
        assert store.versions.get("v1").status == "staged"

    def test_oldest_open_follows_seal_order_and_a_failure_blocks(self, store: Store):  # I2
        for v in ("v1", "v2"):
            store.versions.stage(draft(v))
            store.versions.seal([v], ["a"])
        oldest = store.ledger.oldest_open("a", "p")
        assert oldest is not None and oldest.version_id == "v1"
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "failed", actor="t", error="x")
        oldest = store.ledger.oldest_open("a", "p")
        assert oldest is not None and oldest.status == "failed"
        store.ledger.transition("a", "v1", "skipped", actor="op")
        oldest = store.ledger.oldest_open("a", "p")
        assert oldest is not None and oldest.version_id == "v2"

    def test_oldest_open_is_per_route_and_partition(self, store: Store):
        seal(store, "v1", ["a", "b"], part="p")
        seal(store, "v2", ["a"], part="q")
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "applied", actor="t")
        assert store.ledger.oldest_open("a", "p") is None
        b = store.ledger.oldest_open("b", "p")
        assert b is not None and b.version_id == "v1"
        a = store.ledger.oldest_open("a", "q")
        assert a is not None and a.version_id == "v2"
        assert store.ledger.oldest_open("b", "q") is None

    def test_illegal_transition_is_rejected(self, store: Store):  # I3
        seal(store, "v1", ["a"])
        with pytest.raises(InvalidTransition):
            store.ledger.transition("a", "v1", "applied", actor="t")  # pending cannot jump to applied
        assert store.ledger.get("a", "v1").status == "pending" and store.ledger.attempts("a", "v1") == []

    def test_final_statuses_cannot_be_left(self, store: Store):
        seal(store, "v1", ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "held", actor="t", reason="schema policy")
        store.ledger.transition("a", "v1", "skipped", actor="op")
        targets: list[DeliveryStatus] = ["pending", "running", "applied"]
        for to in targets:
            with pytest.raises(InvalidTransition):
                store.ledger.transition("a", "v1", to, actor="op")

    def test_the_legal_transitions_match_the_design(self):
        assert {k: sorted(v) for k, v in TRANSITIONS.items()} == {
            "pending": ["running"],
            "running": ["applied", "failed", "held", "pending"],
            "held": ["pending", "skipped"],
            "failed": ["pending", "skipped"],
            "applied": ["compensated", "pending"],
            "skipped": [],
            "compensated": [],
        }
        assert CLOSED == {"applied", "skipped", "compensated"}

    def test_every_transition_writes_an_attempt(self, store: Store, clock: FrozenClock):
        seal(store, "v1", ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        clock.advance(timedelta(seconds=30))
        store.ledger.transition("a", "v1", "applied", actor="t", rows_applied=3, code_sha="sha")
        a = store.ledger.attempts("a", "v1")
        assert [x.to_status for x in a] == ["running", "applied"] and a[1].code_sha == "sha"
        assert [x.from_status for x in a] == ["pending", "running"]
        assert a[1].at == T0 + timedelta(seconds=30) and a[1].actor == "t"
        d = store.ledger.get("a", "v1")
        assert d.rows_applied == 3 and d.attempts == 1

    def test_the_attempt_keeps_the_reason_and_error(self, store: Store):
        seal(store, "v1", ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "pending", actor="t", reason="backoff", error="timeout")
        attempt = store.ledger.attempts("a", "v1")[1]
        assert (attempt.reason, attempt.error) == ("backoff", "timeout")
        assert store.ledger.get("a", "v1").last_error == "timeout"
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "applied", actor="t", rows_applied=2, rows_rejected=1)
        d = store.ledger.get("a", "v1")
        assert d.last_error is None and (d.rows_applied, d.rows_rejected, d.attempts) == (2, 1, 2)

    def test_replay_returns_applied_to_pending(self, store: Store):
        seal(store, "v1", ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "applied", actor="t", rows_applied=5)
        d = store.ledger.transition("a", "v1", "pending", actor="op", reason="replay")
        assert (d.status, d.replays, d.retries) == ("pending", 1, 0)
        assert [x.to_status for x in store.ledger.attempts("a", "v1")][-1] == "pending"
        oldest = store.ledger.oldest_open("a", "p")
        assert oldest is not None and oldest.version_id == "v1"
        store.ledger.transition("a", "v1", "running", actor="t")
        assert store.ledger.transition("a", "v1", "applied", actor="t", rows_applied=0).rows_applied == 0

    def test_retrying_a_failed_delivery_counts_a_retry(self, store: Store):
        seal(store, "v1", ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "failed", actor="t", error="boom")
        d = store.ledger.transition("a", "v1", "pending", actor="op", reason="retry")
        assert (d.retries, d.replays, d.last_error) == (1, 0, "boom")

    def test_list_deliveries_filters_and_pages_in_seal_order(self, store: Store):
        seal(store, "v1", ["a", "b"], part="p")
        seal(store, "v2", ["a"], part="q")
        store.ledger.transition("a", "v1", "running", actor="t")
        everything = store.ledger.list_deliveries()
        assert [(d.route, d.version_id) for d in everything] == [("a", "v1"), ("b", "v1"), ("a", "v2")]
        assert [d.version_id for d in store.ledger.list_deliveries(route="a")] == ["v1", "v2"]
        assert [d.route for d in store.ledger.list_deliveries(status="running")] == ["a"]
        assert [d.version_id for d in store.ledger.list_deliveries(partition="q")] == ["v2"]
        first = store.ledger.list_deliveries(limit=2)
        rest = store.ledger.list_deliveries(cursor=delivery_cursor(first[-1]))
        assert first + rest == everything and len(rest) == 1

    def test_all_applied(self, store: Store):
        assert store.ledger.all_applied()
        seal(store, "v1", ["a"])
        assert not store.ledger.all_applied()
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "applied", actor="t")
        assert store.ledger.all_applied()

    def test_latest_sealed_and_listing(self, store: Store):
        assert store.versions.latest_sealed("d", "p") is None
        seal(store, "v1", ["a"])
        seal(store, "v2", ["a"])
        seal(store, "v3", ["a"], part="q")
        store.versions.stage(draft("v4"))  # staged only
        latest = store.versions.latest_sealed("d", "p")
        assert latest is not None and latest.version_id == "v2"
        assert [v.version_id for v in store.versions.list_versions("d")] == ["v4", "v3", "v2", "v1"]
        assert [v.version_id for v in store.versions.list_versions("d", "p", limit=2)] == ["v4", "v2"]
        assert store.versions.list_versions("other") == []

    def test_staged_for_run(self, store: Store):
        store.versions.stage(draft("v1", run="r1"))
        store.versions.stage(draft("v2", run="r1", part="q"))
        store.versions.stage(draft("v3", run="r2"))
        store.versions.stage(draft("v4", run="r1", ds="other"))
        store.versions.seal(["v2"], ["a"])
        assert [v.version_id for v in store.versions.staged_for_run("r1", "d")] == ["v1"]

    def test_sealed_for_run(self, store: Store):
        store.versions.stage(draft("v1", run="r1"))
        store.versions.stage(draft("v2", run="r1", part="q"))
        store.versions.stage(draft("v3", run="r2"))
        store.versions.stage(draft("v4", run="r1", ds="other"))
        store.versions.stage(draft("v5", run="r1", part="s"))  # staged only
        store.versions.seal(["v2", "v1", "v3", "v4"], ["a"])
        assert [v.version_id for v in store.versions.sealed_for_run("r1", "d")] == ["v2", "v1"]

    def test_discard_staged_removes_the_version(self, store: Store):
        store.versions.stage(draft("v1", run="r1"))
        store.versions.discard_staged("v1")
        assert store.versions.staged_for_run("r1", "d") == []
        with pytest.raises(KeyError):
            store.versions.get("v1")
        store.versions.discard_staged("v1")  # nothing there is fine

    def test_discard_staged_refuses_a_sealed_version(self, store: Store):  # I1
        store.versions.stage(draft("v1"))
        store.versions.seal(["v1"], ["a"])
        with pytest.raises(VersionSealed):
            store.versions.discard_staged("v1")
        assert store.versions.get("v1").status == "sealed"

    def test_latest_sealed_before_a_seal_number(self, store: Store):
        store.versions.stage(draft("v1"))
        first = store.versions.seal(["v1"], ["a"])[0]
        store.versions.stage(draft("v2"))
        second = store.versions.seal(["v2"], ["a"])[0]
        store.versions.stage(draft("v3", part="q"))
        store.versions.seal(["v3"], ["a"])
        store.versions.stage(draft("v4"))  # staged only
        assert second.seal_seq is not None and first.seal_seq is not None
        earlier = store.versions.latest_sealed("d", "p", before_seal_seq=second.seal_seq)
        assert earlier is not None and earlier.version_id == "v1"
        assert store.versions.latest_sealed("d", "p", before_seal_seq=first.seal_seq) is None

    def test_latest_watermark_ignores_staged_versions(self, store: Store):
        assert store.versions.latest_watermark("d") is None
        seal(store, "v1", ["a"], wm=10)
        seal(store, "v2", ["a"], part="q", wm=30)
        seal(store, "v3", ["a"], part="r")  # no watermark
        store.versions.stage(draft("v4", wm=99))
        assert store.versions.latest_watermark("d") == 30
        assert store.versions.latest_watermark("other") is None

    def test_since_watermark_is_inclusive_newest_first_and_skips_none(self, store: Store):
        seal(store, "v1", ["a"], wm=10)
        seal(store, "v2", ["a"], wm=20)
        seal(store, "v3", ["a"])  # no watermark
        seal(store, "v4", ["a"], wm=30)
        seal(store, "v5", ["a"], part="q", wm=40)
        store.versions.stage(draft("v6", wm=50))
        got = [v.version_id for v in store.versions.since_watermark("d", "p", 20)]
        assert got == ["v4", "v2"]
        assert store.versions.since_watermark("d", "p", 31) == []

    def test_finish_slice_reports_when_all_are_done(self, store: Store):
        store.versions.register_slices("r1", "d", ["p1", "p2"])
        assert store.versions.finish_slice("r1", "d", "p1") is False
        assert store.versions.finish_slice("r1", "d", "p1") is False  # repeated finish is safe
        assert store.versions.finish_slice("r1", "d", "p2") is True

    def test_slices_are_per_run_and_dataset_and_registering_again_keeps_progress(self, store: Store):
        store.versions.register_slices("r1", "d", ["p1", "p2"])
        store.versions.register_slices("r1", "e", ["p1"])
        store.versions.finish_slice("r1", "d", "p1")
        store.versions.register_slices("r1", "d", ["p1", "p2"])  # a replay
        assert store.versions.finish_slice("r1", "e", "p1") is True
        assert store.versions.finish_slice("r1", "d", "p2") is True

    def test_finishing_a_slice_that_was_not_registered_is_an_error(self, store: Store):
        store.versions.register_slices("r1", "d", ["p1"])
        with pytest.raises(KeyError):
            store.versions.finish_slice("r1", "d", "other")

    # the run journal

    def test_skipped_tick_is_recorded(self, store: Store):  # I6
        rid = store.runs.create_run("f", "cron", "scheduler", {}, status="skipped", reason="paused")
        run = store.runs.get_run(rid)
        assert run.reason == "paused" and run.status == "skipped" and run.created_at == T0

    def test_run_status_changes_and_lists(self, store: Store, clock: FrozenClock):
        first = store.runs.create_run("f", "manual", "ann", {"x": 1})
        second = store.runs.create_run("g", "manual", "ann", {})
        third = store.runs.create_run("f", "cron", "scheduler", {})
        assert store.runs.get_run(first).status == "queued" and store.runs.get_run(first).params == {"x": 1}
        clock.advance(timedelta(minutes=1))
        store.runs.set_run_status(first, "failed", "boom")
        store.runs.set_run_status(first, "failed")  # no reason given keeps the old one
        run = store.runs.get_run(first)
        assert (run.status, run.reason, run.updated_at) == ("failed", "boom", T0 + timedelta(minutes=1))
        assert [r.run_id for r in store.runs.list_runs()] == [third, second, first]
        assert [r.run_id for r in store.runs.list_runs(flow="f")] == [third, first]
        assert [r.run_id for r in store.runs.list_runs(status="failed")] == [first]
        page = store.runs.list_runs(limit=2)
        assert [r.run_id for r in store.runs.list_runs(cursor=page[-1].run_id)] == [first]

    def test_tasks_and_logs(self, store: Store, clock: FrozenClock):
        rid = store.runs.create_run("f", "manual", "ann", {})
        t1 = store.runs.start_task(
            rid, "extract", "ref1", dataset="d", partition="p", worker="w1", code_sha="s"
        )
        t2 = store.runs.start_task(rid, "apply", "ref2", route="a")
        store.runs.log(t1, "info", "started")
        store.runs.log(t1, "error", "bad row")
        clock.advance(timedelta(seconds=2))
        store.runs.finish_task(t1, "failed", "bad row")
        tasks = store.runs.list_tasks(rid)
        assert [t.task_id for t in tasks] == [t1, t2]
        assert tasks[0].status == "failed" and tasks[0].error == "bad row" and tasks[0].worker == "w1"
        assert tasks[0].finished_at == T0 + timedelta(seconds=2) and tasks[1].finished_at is None
        assert (tasks[1].status, tasks[1].route, tasks[1].dataset) == ("running", "a", None)
        assert store.runs.task_logs(t1) == ["info: started", "error: bad row"]
        assert store.runs.task_logs(t2) == []

    # record state and quarantine

    def test_record_state_round_trip(self, store: Store):
        a = StateRow("a", "p", 1, {"x": 1}, "v1")
        b = StateRow("b", "q", 2, {"x": [1, 2]}, "v1")
        store.state.put_many("d", [a, b])
        assert store.state.get_many("d", ["a", "b", "missing"]) == {"a": a, "b": b}
        assert store.state.get_many("other", ["a"]) == {}
        c = StateRow("a", "q", 3, {"x": 2}, "v2")
        store.state.put_many("d", [c])  # replaces the row for the same key
        assert store.state.get_many("d", ["a"]) == {"a": c}
        store.state.delete_many("d", ["a", "never-there"])
        assert store.state.get_many("d", ["a", "b"]) == {"b": b}

    def test_supersede_resolves_only_older_entries(self, store: Store):
        for v in ("v1", "v2", "v3"):
            seal(store, v, ["a", "b"])
        seq = {v: store.versions.get(v).seal_seq for v in ("v1", "v2", "v3")}
        assert all(s is not None for s in seq.values())
        store.quarantine.add("a", "v1", "k", "bad")
        store.quarantine.add("a", "v2", "k", "worse")
        store.quarantine.add("a", "v3", "k", "still bad")
        store.quarantine.add("a", "v1", "other", "bad")
        store.quarantine.add("b", "v1", "k", "bad")
        store.quarantine.add(None, None, "k", "rejected at extraction")
        assert store.quarantine.supersede("a", "k", seq["v3"] or 0) == 2
        remaining = {(q.route, q.version_id, q.key) for q in store.quarantine.list_open()}
        assert remaining == {("a", "v3", "k"), ("a", "v1", "other"), ("b", "v1", "k"), (None, None, "k")}
        assert store.quarantine.supersede("a", "k", seq["v3"] or 0) == 0  # already closed

    def test_quarantine_lists_by_route_and_add_is_idempotent(self, store: Store):
        store.quarantine.add("a", "v1", "k", "first")
        store.quarantine.add("a", "v1", "k", "second")
        store.quarantine.add("b", "v1", "k", "other route")
        rows = store.quarantine.list_open("a")
        assert [(q.key, q.error, q.status) for q in rows] == [("k", "second", "open")]
        assert len(store.quarantine.list_open()) == 2

    def test_values_survive_pickling(self, store: Store):
        seal(store, "v1", ["a"], wm=datetime(2026, 1, 1, tzinfo=UTC))
        store.ledger.transition("a", "v1", "running", actor="t")
        run = store.runs.get_run(store.runs.create_run("f", "manual", "ann", {"x": [1]}))
        task = store.runs.list_tasks(store.runs.get_run(run.run_id).run_id)
        values = [
            run,
            task,
            store.versions.get("v1"),
            store.ledger.get("a", "v1"),
            store.ledger.attempts("a", "v1"),
            StateRow("k", "p", 1, {"x": 1}, "v1"),
        ]
        assert pickle.loads(pickle.dumps(values)) == values

    # the spool

    def test_chunk_round_trip_keeps_types(self, spool: Spool):
        recs = [Record("k", "upsert", {"d": Decimal("1.10"), "t": T0, "n": [1, 2], "o": {"x": None}}, 7)]
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert table_to_records(spool.get_chunk("v1", 0))[0] == recs[0]

    def test_all_record_fields_round_trip(self, spool: Spool):
        recs = [
            Record("a", "upsert", {"x": 1, "s": "t", "n": None}, 2**64 - 1, moved_from="p0"),
            Record("b", "delete", {}, 0),
        ]
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert table_to_records(spool.get_chunk("v1", 0)) == recs

    def test_mixed_type_column_round_trips(self, spool: Spool):  # Review Focus 4
        recs = [Record("a", "upsert", {"x": 1}, 1), Record("b", "upsert", {"x": "two"}, 2)]
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert [r["x"] for r in table_to_records(spool.get_chunk("v1", 0))] == [1, "two"]

    def test_awkward_columns_round_trip_exactly(self, spool: Spool):
        big = 2**70
        payloads: list[dict[str, Any]] = [
            {"amount": Decimal("1.5"), "when": T0, "blob": b"\x00\x01", "nested": {"a": 1}, "huge": big},
            {"amount": 2, "when": "never", "blob": b"", "nested": {"b": 2}, "huge": 1},
            {"amount": Decimal(3), "when": None, "blob": None, "nested": None, "huge": 2},
        ]
        recs = upserts(*((f"k{i}", p) for i, p in enumerate(payloads)))
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        back = table_to_records(spool.get_chunk("v1", 0))
        assert [r.payload for r in back] == payloads
        assert [type(r["amount"]) for r in back] == [Decimal, int, Decimal]

    def test_dates_and_bytes_in_a_mixed_column_round_trip(self, spool: Spool):
        payloads: list[dict[str, Any]] = [{"v": date(2026, 1, 2)}, {"v": bytes([0, 255])}, {"v": "text"}]
        spool.put_chunk(
            "v1", 0, "data", records_to_table(upserts(*((f"k{i}", p) for i, p in enumerate(payloads))))
        )
        assert [r.payload for r in table_to_records(spool.get_chunk("v1", 0))] == payloads

    def test_decimals_of_different_scale_keep_their_scale(self, spool: Spool):
        recs = upserts(
            ("a", {"d": Decimal("1.10")}), ("b", {"d": Decimal("2.5")}), ("c", {"d": Decimal(100)})
        )
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        back = table_to_records(spool.get_chunk("v1", 0))
        assert [str(r["d"]) for r in back] == ["1.10", "2.5", "100"]

    def test_decimals_of_one_scale_stay_arrow_decimals(self):
        recs = upserts(("a", {"d": Decimal("1.10")}), ("b", {"d": Decimal("22.50")}))
        table = records_to_table(recs)
        assert pa.types.is_decimal(table.column("d").type)

    def test_records_with_nested_values_that_differ_in_shape_round_trip(self, spool: Spool):
        recs = upserts(("a", {"o": {"x": 1}}), ("b", {"o": {"y": 2}}), ("c", {"o": {"x": [1, None]}}))
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert [r["o"] for r in table_to_records(spool.get_chunk("v1", 0))] == [
            {"x": 1},
            {"y": 2},
            {"x": [1, None]},
        ]

    def test_empty_objects_round_trip(self, spool: Spool):
        recs = upserts(("a", {"o": {}}), ("b", {"o": {}, "p": [{}, {"x": 1}]}), ("c", {"o": None}))
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert [r.payload for r in table_to_records(spool.get_chunk("v1", 0))] == [
            {"o": {}, "p": None},
            {"o": {}, "p": [{}, {"x": 1}]},
            {"o": None, "p": None},
        ]

    def test_a_key_one_record_lacks_reads_back_as_none(self, spool: Spool):
        # Chunks are tabular: a column that any record has exists for every record.
        recs = upserts(("a", {"x": 1, "y": 2}), ("b", {"x": 3}))
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert [r.payload for r in table_to_records(spool.get_chunk("v1", 0))] == [
            {"x": 1, "y": 2},
            {"x": 3, "y": None},
        ]

    def test_chunk_columns_are_the_payload_plus_the_fc_columns(self):
        table = records_to_table(upserts(("a", {"x": 1})))
        assert table.column_names == ["x", "_fc_key", "_fc_op", "_fc_row_hash", "_fc_moved_from"]
        assert table.column("_fc_row_hash").type == pa.uint64()
        assert records_to_table([]).num_rows == 0

    def test_a_payload_column_cannot_use_the_reserved_prefix(self):
        with pytest.raises(ValueError):
            records_to_table(upserts(("a", {"_fc_key": 1})))

    def test_snapshot_table_holds_keys_and_hashes(self, spool: Spool):
        spool.put_chunk("v1", 0, "snapshot", snapshot_table([("a", 1), ("b", 2**64 - 1)]))
        table = spool.get_chunk("v1", 0, "snapshot")
        assert table.column("_fc_key").to_pylist() == ["a", "b"]
        assert table.column("_fc_row_hash").to_pylist() == [1, 2**64 - 1]

    def test_putting_a_chunk_twice_overwrites(self, spool: Spool):
        spool.put_chunk("v1", 0, "data", records_to_table(upserts(("a", {"x": 1}))))
        ref = spool.put_chunk("v1", 0, "data", records_to_table(upserts(("b", {"x": 2}), ("c", {"x": 3}))))
        assert [r.key for r in table_to_records(spool.get_chunk("v1", 0))] == ["b", "c"]
        assert spool.list_chunks("v1") == [ref] and ref.rows == 2

    def test_chunks_of_different_kinds_and_versions_are_separate(self, spool: Spool):
        spool.put_chunk("v1", 0, "data", records_to_table(upserts(("a", {"x": 1}))))
        spool.put_chunk("v1", 0, "full", records_to_table(upserts(("f", {"x": 1}))))
        spool.put_chunk("v2", 0, "data", records_to_table(upserts(("z", {"x": 1}))))
        assert [r.key for r in table_to_records(spool.get_chunk("v1", 0, "full"))] == ["f"]
        assert [r.key for r in table_to_records(spool.get_chunk("v1", 0))] == ["a"]
        assert [c.kind for c in spool.list_chunks("v1", "full")] == ["full"]

    def test_list_chunks_is_in_chunk_order(self, spool: Spool):
        for n in (2, 0, 1):
            spool.put_chunk("v1", n, "data", records_to_table(upserts((f"k{n}", {"x": n}))))
        assert [(c.version_id, c.chunk_no, c.kind) for c in spool.list_chunks("v1")] == [
            ("v1", 0, "data"),
            ("v1", 1, "data"),
            ("v1", 2, "data"),
        ]
        assert spool.list_chunks("nobody") == []

    def test_a_missing_chunk_is_a_key_error(self, spool: Spool):
        with pytest.raises(KeyError):
            spool.get_chunk("v1", 0)

    def test_delete_chunks_from_a_number_drops_the_tail(self, spool: Spool):
        for n in range(4):
            spool.put_chunk("v1", n, "data", records_to_table(upserts((f"k{n}", {"x": n}))))
        spool.put_chunk("v1", 2, "full", records_to_table(upserts(("f", {"x": 1}))))
        spool.delete_chunks("v1", "data", from_chunk=2)
        assert [c.chunk_no for c in spool.list_chunks("v1")] == [0, 1]
        assert [c.chunk_no for c in spool.list_chunks("v1", "full")] == [2]
        spool.delete_chunks("v1", "data")
        assert spool.list_chunks("v1") == []

    def test_delete_version_drops_every_kind(self, spool: Spool):
        spool.put_chunk("v1", 0, "data", records_to_table(upserts(("a", {"x": 1}))))
        spool.put_chunk("v1", 0, "snapshot", snapshot_table([("a", 1)]))
        spool.put_chunk("v2", 0, "data", records_to_table(upserts(("b", {"x": 1}))))
        spool.delete_version("v1")
        assert spool.list_chunks("v1") == [] and spool.list_chunks("v1", "snapshot") == []
        assert len(spool.list_chunks("v2")) == 1
        spool.delete_version("v1")  # nothing left to delete is not an error

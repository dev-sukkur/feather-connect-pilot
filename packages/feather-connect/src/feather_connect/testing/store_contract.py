"""The contract every store and spool backend must pass.

A backend's test module subclasses `StoreContract` and defines the fixtures `store`, `spool` and `clock`.
`clock` is the `FrozenClock` that `store` and `spool` were built with, so tests can check the times they
record. A backend with only a spool subclasses `SpoolContract`, which needs `spool` and `clock`.
"""

import pickle
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from feather_connect.clock import FrozenClock
from feather_connect.core.records import Record, row_hash
from feather_connect.ids import stable_id
from feather_connect.storage.port import (
    CLOSED,
    TRANSITIONS,
    BudgetSpec,
    DeliveryStatus,
    DuplicateUser,
    InvalidTransition,
    SliceFinished,
    StaleWriter,
    StateRow,
    Store,
    TokenRow,
    VersionDraft,
    VersionSealed,
    delivery_cursor,
    failure_cursor,
    parse_limits,
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


class SpoolContract:
    """The chunk tests, for a backend that has a spool and no store."""

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

    def test_chunks_live_on_the_day_of_their_versions_first_write(self, spool: Spool, clock: FrozenClock):
        spool.put_chunk("v1", 0, "data", records_to_table(upserts(("a", {"x": 1}))))
        spool.put_chunk("v1", 0, "snapshot", snapshot_table([("a", 1)]))
        clock.advance(timedelta(days=2))
        spool.put_chunk(
            "v1", 1, "data", records_to_table(upserts(("b", {"x": 1})))
        )  # a rewrite stays on day 1
        spool.put_chunk("v2", 0, "data", records_to_table(upserts(("c", {"x": 1}))))
        assert spool.days() == [T0.date(), clock.now().date()]
        assert spool.versions_on(T0.date()) == ["v1"] and spool.versions_on(clock.now().date()) == ["v2"]
        assert spool.snapshot_versions() == ["v1"]

    def test_dropping_a_day_keeps_snapshots_and_other_days(self, spool: Spool, clock: FrozenClock):
        spool.put_chunk("v1", 0, "data", records_to_table(upserts(("a", {"x": 1}))))
        spool.put_chunk("v1", 0, "snapshot", snapshot_table([("a", 1)]))
        clock.advance(timedelta(days=1))
        spool.put_chunk("v2", 0, "data", records_to_table(upserts(("b", {"x": 1}))))
        spool.drop_day(T0.date())
        assert spool.list_chunks("v1") == [] and len(spool.list_chunks("v1", "snapshot")) == 1
        assert len(spool.list_chunks("v2")) == 1 and spool.days() == [clock.now().date()]
        spool.drop_day(T0.date())  # a day with nothing on it is not an error
        spool.ensure_partitions(2)
        spool.put_chunk("v3", 0, "data", records_to_table(upserts(("c", {"x": 1}))))
        assert len(spool.list_chunks("v3")) == 1


class StoreContract(SpoolContract):
    def _table(self) -> pa.Table:
        return records_to_table(upserts(("k", {"x": 1})))

    def test_a_sealed_version_refuses_every_write_and_delete(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        spool.put_chunk("v1", 0, "data", self._table())
        spool.put_chunk("v1", 0, "snapshot", snapshot_table([("k", 1)]))
        seal(store, "v1", ["a"])
        for kind in ("data", "snapshot", "full", "moves", "settled"):
            with pytest.raises(VersionSealed):
                spool.put_chunk("v1", 0, kind, self._table())
        for kind in ("data", "snapshot", "full", "moves"):
            with pytest.raises(VersionSealed):
                spool.delete_chunks("v1", kind)
        with pytest.raises(VersionSealed):
            spool.delete_version("v1")
        assert [c.chunk_no for c in spool.list_chunks("v1")] == [0]
        assert [c.chunk_no for c in spool.list_chunks("v1", "snapshot")] == [0]

    def test_what_follows_sealing_may_still_clear_the_working_chunks(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        spool.put_chunk("v1", 0, "candidates", snapshot_table([("k", 1)]))
        seal(store, "v1", ["a"])
        spool.delete_chunks("v1", "candidates")
        assert spool.list_chunks("v1", "candidates") == []

    def test_retention_can_still_take_a_sealed_versions_chunks(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        spool.put_chunk("v1", 0, "data", self._table())
        spool.put_chunk("v1", 0, "snapshot", snapshot_table([("k", 1)]))
        seal(store, "v1", ["a"])
        spool.expire_chunks("v1", "snapshot")
        assert spool.list_chunks("v1", "snapshot") == []
        spool.expire_version("v1")
        assert spool.list_chunks("v1") == []

    def test_a_later_claim_shuts_out_the_earlier_writer(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        store.versions.claim("v1", "first", "r1", "d", "p")
        spool.put_chunk("v1", 0, "data", self._table(), "first")
        store.versions.stage(draft("v1"), "first")
        store.versions.claim("v1", "second", "r1", "d", "p")

        with pytest.raises(KeyError):  # the claim took the staged version with it
            store.versions.get("v1")
        with pytest.raises(StaleWriter):
            spool.put_chunk("v1", 1, "data", self._table(), "first")
        with pytest.raises(StaleWriter):
            spool.delete_chunks("v1", "data", writer="first")
        with pytest.raises(StaleWriter):
            spool.delete_version("v1", "first")
        with pytest.raises(StaleWriter):
            store.versions.stage(draft("v1"), "first")
        with pytest.raises(StaleWriter):
            store.versions.discard_staged("v1", "first")
        store.versions.stage(draft("v1"), "second")
        with pytest.raises(StaleWriter):  # not the later writer's version to seal, though it is staged
            store.versions.seal(["v1"], ["a"], "first")
        assert store.versions.get("v1").status == "staged"
        assert store.versions.seal(["v1"], ["a"], "second")[0].status == "sealed"
        assert store.versions.seal(["v1"], ["a"], "first")[0].status == "sealed"  # a repeat changes nothing

    def test_a_writer_that_clears_its_version_keeps_its_claim(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        store.versions.claim("v1", "w", "r1", "d", "p")
        spool.put_chunk("v1", 0, "data", self._table(), "w")
        spool.delete_version("v1", "w")
        assert spool.list_chunks("v1") == []
        spool.put_chunk("v1", 0, "settled", self._table(), "w")

    def test_a_sealed_version_cannot_be_claimed_or_taken_over(self, store: Store, spool: Spool):
        seal(store, "v1", ["a"])
        with pytest.raises(VersionSealed):
            store.versions.claim("v1", "w", "r1", "d", "p")
        with pytest.raises(VersionSealed):
            store.versions.take_over(["v1"], "w")

    def test_taking_over_shuts_out_the_earlier_writer_and_keeps_the_version(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        store.versions.claim("v1", "first", "r1", "d", "p")
        store.versions.stage(draft("v1"), "first")
        store.versions.take_over(["v1"], "seal")
        assert store.versions.get("v1").status == "staged"
        with pytest.raises(StaleWriter):
            spool.put_chunk("v1", 0, "data", self._table(), "first")
        assert store.versions.seal(["v1"], ["a"], "seal")[0].status == "sealed"

    def test_a_finished_slice_cannot_be_claimed_and_a_stale_writer_cannot_finish_one(self, store: Store):
        store.versions.register_slices("r1", "d", ["p", "q"])
        vid = stable_id("r1", "d", "p")
        store.versions.claim(vid, "first", "r1", "d", "p")
        store.versions.claim(vid, "second", "r1", "d", "p")
        with pytest.raises(StaleWriter):
            store.versions.finish_slice("r1", "d", "p", "first")
        assert store.versions.finish_slice("r1", "d", "p", "second") is False
        assert store.versions.finish_slice("r1", "d", "p", "first") is False  # done already: it's a repeat
        with pytest.raises(SliceFinished):
            store.versions.claim(vid, "third", "r1", "d", "p")

    # budgets

    def test_budget_never_exceeds_any_window(self, store: Store, clock: FrozenClock):
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute", "12/day"])))
        assert sum(store.budgets.acquire("z", 1).granted for _ in range(20)) == 10
        clock.advance(timedelta(minutes=1))
        assert sum(store.budgets.acquire("z", 1).granted for _ in range(20)) == 2  # the day window bites

    def test_low_priority_leaves_the_reserve_for_high(self, store: Store):
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute"]), reserve={"high": 0.6}))
        assert sum(store.budgets.acquire("z", 1, "low").granted for _ in range(10)) == 4
        assert sum(store.budgets.acquire("z", 1, "high").granted for _ in range(10)) == 6

    def test_normal_priority_may_use_the_whole_window(self, store: Store):
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute"]), reserve={"high": 0.6}))
        assert sum(store.budgets.acquire("z", 1, "normal").granted for _ in range(20)) == 10

    def test_refused_grant_says_how_long_to_wait(self, store: Store, clock: FrozenClock):
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute"])))
        assert store.budgets.acquire("z", 10).granted
        refused = store.budgets.acquire("z", 1)
        assert not refused.granted and refused.wait == timedelta(seconds=6)
        clock.advance(refused.wait)
        assert store.budgets.acquire("z", 1).granted

    def test_the_wait_is_for_the_slowest_window(self, store: Store):
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute", "2/day"])))
        assert store.budgets.acquire("z", 2).granted
        assert store.budgets.acquire("z", 1).wait > timedelta(hours=11)

    def test_acquire_is_all_or_nothing_across_windows(self, store: Store):
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute", "3/day"])))
        assert store.budgets.acquire("z", 2).granted
        assert not store.budgets.acquire(
            "z", 2
        ).granted  # the day window has one left, so the minute window is spared
        assert [u.used for u in store.budgets.usage("z")] == [2, 2]
        assert store.budgets.acquire("z", 1).granted

    def test_usage_reports_units_in_use_after_refill(self, store: Store, clock: FrozenClock):
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute"])))
        store.budgets.acquire("z", 6)
        assert [u.used for u in store.budgets.usage("z")] == [6]
        clock.advance(timedelta(seconds=30))
        assert [u.used for u in store.budgets.usage("z")] == [1]

    def test_defining_a_budget_again_keeps_its_use_unless_the_spec_changed(self, store: Store):
        spec = BudgetSpec("z", parse_limits(["10/minute"]))
        store.budgets.define(spec)
        store.budgets.acquire("z", 7)
        store.budgets.define(spec)
        assert [u.used for u in store.budgets.usage("z")] == [7]
        store.budgets.define(BudgetSpec("z", parse_limits(["20/minute"])))
        assert [u.used for u in store.budgets.usage("z")] == [0]

    def test_an_unknown_budget_or_an_impossible_request_is_an_error(self, store: Store):
        with pytest.raises(KeyError):
            store.budgets.acquire("nobody", 1)
        store.budgets.define(BudgetSpec("z", parse_limits(["10/minute"]), reserve={"high": 0.6}))
        with pytest.raises(ValueError):
            store.budgets.acquire("z", 11)
        with pytest.raises(ValueError):
            store.budgets.acquire("z", 5, "low")  # low may take 4 of a window at most

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

    def test_slices_for_run_lists_what_was_registered_finished_or_not(self, store: Store):
        store.versions.register_slices("r1", "d", ["p1", "p2"])
        store.versions.register_slices("r1", "e", ["p3"])
        store.versions.finish_slice("r1", "d", "p1")
        store.versions.register_slices("r1", "d", ["p2"])  # a replay
        assert sorted(store.versions.slices_for_run("r1", "d")) == ["p1", "p2"]
        assert store.versions.slices_for_run("r1", "e") == ["p3"]
        assert store.versions.slices_for_run("r2", "d") == []

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

    def test_starting_a_task_again_keeps_its_row(self, store: Store):
        rid = store.runs.create_run("f", "manual", "ann", {})
        other = store.runs.create_run("f", "manual", "ann", {})
        first = store.runs.start_task(rid, "apply", "ref")
        store.runs.finish_task(first, "failed", "bad")
        assert store.runs.start_task(rid, "apply", "ref") == first
        assert [(t.task_id, t.status) for t in store.runs.list_tasks(rid)] == [(first, "failed")]
        assert store.runs.start_task(other, "apply", "ref") != first

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

    def test_a_retry_is_noted_on_the_task_and_outlives_its_finish(self, store: Store):
        rid = store.runs.create_run("f", "manual", "ann", {})
        task = store.runs.start_task(rid, "extract", "ref")
        assert store.runs.list_tasks(rid)[0].attempt == 1
        store.runs.record_attempt(task, 2, "timeout")
        store.runs.record_attempt(task, 3, "reset by peer")
        row = store.runs.list_tasks(rid)[0]
        assert (row.attempt, row.error, row.status) == (3, "reset by peer", "running")
        store.runs.finish_task(task, "succeeded")  # no error to give: the last retry's stays
        row = store.runs.list_tasks(rid)[0]
        assert (row.attempt, row.error, row.status) == (3, "reset by peer", "succeeded")
        store.runs.finish_task(task, "failed", "gave up")
        assert store.runs.list_tasks(rid)[0].error == "gave up"
        with pytest.raises(KeyError):
            store.runs.record_attempt("nothing", 2, "x")

    def test_retried_tasks_counts_the_tasks_that_took_more_than_one_attempt(self, store: Store):
        first = store.runs.create_run("f", "manual", "ann", {})
        second = store.runs.create_run("f", "manual", "ann", {})
        third = store.runs.create_run("f", "manual", "ann", {})
        a, b = store.runs.start_task(first, "extract", "a"), store.runs.start_task(first, "extract", "b")
        store.runs.start_task(first, "extract", "c")
        store.runs.start_task(second, "extract", "a")
        store.runs.record_attempt(a, 2, "x")
        store.runs.record_attempt(b, 4, "y")
        assert store.reads.retried_tasks([first, second, third]) == {first: 2}
        assert store.reads.retried_tasks([]) == {}

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

    def test_extraction_entries_are_found_and_closed_by_dataset(self, store: Store):
        store.quarantine.add(None, "v1", "k", "bad", dataset="d", partition="p")
        store.quarantine.add(None, None, "m", "no partition", dataset="d")
        store.quarantine.add(None, "v2", "k", "bad", dataset="other")
        store.quarantine.add("a", "v1", "r", "a sink's")
        assert store.quarantine.open_extraction_keys("d", ["k", "m", "r", "x"]) == {"k", "m"}
        assert store.quarantine.open_extraction_keys("d", ["k", "m"], outside="p") == {"m"}
        assert store.quarantine.open_extraction_keys("d", ["k"], outside="q") == {"k"}
        assert store.quarantine.close_extraction("d", ["k", "r"]) == 1
        assert store.quarantine.close_extraction("d", ["k"]) == 0  # already closed
        assert store.quarantine.open_extraction_keys("d", ["k", "m"]) == {"m"}
        assert store.quarantine.open_extraction_keys("other", ["k"]) == {"k"}
        assert len(store.quarantine.list_open("a")) == 1
        store.quarantine.add(None, "v1", "k", "bad again", dataset="d", partition="p")  # reopened
        assert store.quarantine.open_extraction_keys("d", ["k"]) == {"k"}

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

    # catalogue

    def test_catalog_keeps_specs_per_sha(self, store: Store, clock: FrozenClock):
        store.catalog.sync("acme", "sha1", {"f": flow_spec("f", ["a", "b"])})
        clock.advance(timedelta(minutes=1))
        store.catalog.sync("acme", "sha2", {"f": flow_spec("f", ["a", "c"], owner="ann")})
        assert store.catalog.flow("f").code_sha == "sha2" and store.catalog.flow("f").spec["owner"] == "ann"
        old = store.catalog.flow("f", "sha1")
        assert (old.code_sha, old.project, old.spec["owner"]) == ("sha1", "acme", None)
        assert [r.name for r in store.catalog.routes()] == ["a", "c"]  # b is not in the latest code
        assert [(r.name, r.flow, r.code_sha) for r in store.catalog.routes()] == [
            ("a", "f", "sha2"),
            ("c", "f", "sha2"),
        ]
        with pytest.raises(KeyError):
            store.catalog.flow("f", "nope")
        with pytest.raises(KeyError):
            store.catalog.flow("nope")

    def test_catalog_sync_again_makes_that_sha_the_latest(self, store: Store, clock: FrozenClock):
        store.catalog.sync("acme", "sha1", {"f": flow_spec("f", ["a"])})
        clock.advance(timedelta(minutes=1))
        store.catalog.sync("acme", "sha2", {"f": flow_spec("f", ["a"])})
        clock.advance(timedelta(minutes=1))
        store.catalog.sync("acme", "sha1", {"f": flow_spec("f", ["a"], owner="bob")})  # a rollback
        assert store.catalog.flow("f").code_sha == "sha1" and store.catalog.flow("f").spec["owner"] == "bob"
        assert [f.code_sha for f in store.catalog.flows()] == ["sha1"]

    def test_catalog_retires_what_a_project_no_longer_has(self, store: Store, clock: FrozenClock):
        store.catalog.sync("acme", "sha1", {"f": flow_spec("f", ["a"]), "g": flow_spec("g", ["b"])})
        store.catalog.sync("other", "sha9", {"h": flow_spec("h", ["c"])})
        clock.advance(timedelta(minutes=1))
        store.catalog.sync("acme", "sha2", {"f": flow_spec("f", ["a"])})
        assert [f.name for f in store.catalog.flows()] == ["f", "h"]
        assert [r.name for r in store.catalog.routes()] == ["a", "c"]
        assert store.catalog.flow("g").code_sha == "sha1"  # still there to look at
        clock.advance(timedelta(minutes=1))
        store.catalog.sync("acme", "sha3", {"f": flow_spec("f", ["a"]), "g": flow_spec("g", ["b"])})
        assert [f.name for f in store.catalog.flows()] == ["f", "g", "h"]
        assert [r.name for r in store.catalog.routes()] == ["a", "b", "c"]

    # workers

    def test_workers_heartbeat_and_list(self, store: Store, clock: FrozenClock):
        store.workers.heartbeat("w2", {"pool": "x"}, "sha2", 4)
        store.workers.heartbeat("w1", {}, "sha1", 8)
        clock.advance(timedelta(seconds=5))
        store.workers.heartbeat("w1", {}, "sha1", 8)
        rows = store.workers.list_workers()
        assert [(w.worker_id, w.labels, w.code_sha, w.slots, w.state) for w in rows] == [
            ("w1", {}, "sha1", 8, "alive"),
            ("w2", {"pool": "x"}, "sha2", 4, "alive"),
        ]  # workers on different SHAs are both there
        assert rows[0].last_seen == clock.now() and rows[1].last_seen == T0

    def test_stale_lists_workers_past_the_timeout(self, store: Store, clock: FrozenClock):
        store.workers.heartbeat("old", {}, "s", 1)
        store.workers.heartbeat("gone", {}, "s", 1)
        store.workers.mark("gone", "dead")
        clock.advance(timedelta(seconds=40))
        store.workers.heartbeat("new", {}, "s", 1)
        assert [w.worker_id for w in store.workers.stale(timedelta(seconds=30))] == ["old"]
        assert store.workers.stale(timedelta(seconds=41)) == []

    def test_a_draining_worker_stays_draining_and_a_dead_one_comes_back(self, store: Store):
        store.workers.heartbeat("w", {}, "s", 1)
        store.workers.mark("w", "draining")
        store.workers.heartbeat("w", {}, "s", 1)
        assert store.workers.list_workers()[0].state == "draining"
        store.workers.mark("w", "dead")
        store.workers.heartbeat("w", {}, "s", 1)
        assert store.workers.list_workers()[0].state == "alive"
        with pytest.raises(KeyError):
            store.workers.mark("nobody", "dead")

    def test_last_tick_is_kept_per_flow_and_trigger(self, store: Store, clock: FrozenClock):
        assert store.schedules.last_tick("f", "every:60s") is None
        store.schedules.set_last_tick("f", "every:60s", T0)
        store.schedules.set_last_tick("f", "cron:x", T0 + timedelta(hours=1))
        assert store.schedules.last_tick("f", "every:60s") == T0
        store.schedules.set_last_tick("f", "every:60s", T0 + timedelta(minutes=1))
        assert store.schedules.last_tick("f", "every:60s") == T0 + timedelta(minutes=1)
        assert store.schedules.last_tick("f", "cron:x") == T0 + timedelta(hours=1)
        assert store.schedules.last_tick("g", "every:60s") is None

    def test_overrides_are_set_read_listed_and_cleared(self, store: Store, clock: FrozenClock):
        assert store.overrides.get("flow:f", "paused") is None and store.overrides.list_all() == []
        store.overrides.set("flow:f", "paused", True, actor="ann", reason="maintenance")
        store.overrides.set("route:a", "paused", {"until": "later"}, actor="bob")
        assert store.overrides.get("flow:f", "paused") is True
        assert store.overrides.get("route:a", "paused") == {"until": "later"}
        found = store.overrides.list_all()
        assert [(o.target, o.key, o.reason, o.created_by, o.created_at, o.expires_at) for o in found] == [
            ("flow:f", "paused", "maintenance", "ann", T0, None),
            ("route:a", "paused", None, "bob", T0, None),
        ]
        store.overrides.set("flow:f", "paused", False, actor="cy")  # setting again replaces
        assert store.overrides.get("flow:f", "paused") is False
        assert (
            store.overrides.list_all()[0].created_by == "cy" and store.overrides.list_all()[0].reason is None
        )
        store.overrides.clear("flow:f", "paused", actor="ann", reason="done")
        store.overrides.clear("flow:f", "paused", actor="ann")  # clearing what is not there is fine
        assert store.overrides.get("flow:f", "paused") is None
        assert [o.target for o in store.overrides.list_all()] == ["route:a"]

    def test_an_expired_override_is_gone(self, store: Store, clock: FrozenClock):
        store.overrides.set("route:a", "paused", True, actor="ann", expires_at=T0 + timedelta(hours=1))
        clock.advance(timedelta(minutes=59))
        assert store.overrides.get("route:a", "paused") is True and len(store.overrides.list_all()) == 1
        clock.advance(timedelta(minutes=1))
        assert store.overrides.get("route:a", "paused") is None and store.overrides.list_all() == []

    # users, tokens and the audit log

    def test_users_are_created_and_found_by_name(self, store: Store, clock: FrozenClock):
        assert store.users.get_user("op") is None
        user_id = store.users.create_user("op", "hash-1")
        found = store.users.get_user("op")
        assert found is not None and (found.user_id, found.username, found.password_hash) == (
            user_id,
            "op",
            "hash-1",
        )
        assert found.created_at == T0
        assert store.users.get_user("OP") is None and store.users.get_user("other") is None
        with pytest.raises(DuplicateUser):
            store.users.create_user("op", "hash-2")

    def test_a_users_password_hash_can_be_replaced(self, store: Store):
        user_id = store.users.create_user("op", "hash-1")
        store.users.set_password_hash("op", "hash-2")
        found = store.users.get_user("op")
        assert found is not None and (found.user_id, found.password_hash, found.created_at) == (
            user_id,
            "hash-2",
            T0,
        )
        with pytest.raises(KeyError):
            store.users.set_password_hash("nobody", "hash-3")

    def test_tokens_are_found_by_hash_and_revoked_once(self, store: Store, clock: FrozenClock):
        assert store.users.find_token("h1") is None
        expiry = T0 + timedelta(days=1)
        token_id = store.users.create_token("ci", "h1", expiry)
        other = store.users.create_token("forever", "h2", None)
        found = store.users.find_token("h1")
        assert found == TokenRow(token_id, "ci", "h1", T0, expiry, None)
        clock.advance(timedelta(hours=1))
        store.users.revoke_token(token_id)
        clock.advance(timedelta(hours=1))
        store.users.revoke_token(token_id)  # the first revocation stands
        store.users.revoke_token("no-such-token")
        revoked = store.users.find_token("h1")
        assert revoked is not None and revoked.revoked_at == T0 + timedelta(hours=1)
        untouched = store.users.find_token("h2")
        assert untouched is not None and untouched.token_id == other and untouched.revoked_at is None

    def test_audit_rows_list_newest_first_with_a_cursor(self, store: Store, clock: FrozenClock):
        ids: list[str] = []
        for n in range(5):
            ids.append(
                store.audit.record("human", "ann", f"act{n}", f"flow:f{n}", {"reason": "r", "n": n}, "ok")
            )
            clock.advance(timedelta(seconds=1))
        rows = store.audit.list_audit()
        assert [r.audit_id for r in rows] == ids[::-1]
        first = rows[0]
        assert (first.actor_type, first.actor, first.action, first.target, first.result) == (
            "human",
            "ann",
            "act4",
            "flow:f4",
            "ok",
        )
        assert first.request == {"reason": "r", "n": 4} and first.at == T0 + timedelta(seconds=4)
        page = store.audit.list_audit(limit=2)
        assert [r.audit_id for r in page] == ids[:2:-1]
        rest = store.audit.list_audit(limit=10, cursor=page[-1].audit_id)
        assert [r.audit_id for r in rest] == ids[2::-1]
        assert store.audit.list_audit(cursor=ids[0]) == []
        store.audit.record("system", "scheduler", "tick", None, {}, "failed: x")
        assert store.audit.list_audit(limit=1)[0].target is None

    def test_an_override_can_be_claimed_once(self, store: Store, clock: FrozenClock):
        soon = T0 + timedelta(hours=1)
        assert store.overrides.claim("preview", "j1", {"x": 1}, actor="ann", expires_at=soon) is True
        assert store.overrides.claim("preview", "j1", {"x": 2}, actor="bob", expires_at=soon) is False
        assert store.overrides.get("preview", "j1") == {"x": 1}
        assert store.overrides.claim("preview", "j2", True, actor="ann", expires_at=soon) is True
        store.overrides.set("flow:f", "paused", True, actor="ann")
        assert store.overrides.claim("flow:f", "paused", False, actor="bob") is False  # a set one is taken
        assert store.overrides.get("flow:f", "paused") is True
        clock.advance(timedelta(hours=1))  # an expired one is free again
        assert store.overrides.claim("preview", "j1", {"x": 3}, actor="cy", expires_at=None) is True
        assert store.overrides.get("preview", "j1") == {"x": 3}

    # reads

    def test_partition_grid_shows_the_latest_sealed_version_and_each_route_status(
        self, store: Store, clock: FrozenClock
    ):
        seal(store, "v1", ["a", "b"], part="p1", up=3)
        clock.advance(timedelta(hours=1))
        seal(store, "v2", ["a", "b"], part="p2", up=5)
        seal(store, "v3", ["a", "b"], part="p1", up=4, run="r2")
        store.ledger.transition("a", "v3", "running", actor="t")
        store.ledger.transition("a", "v3", "applied", actor="t", rows_applied=4)
        store.versions.stage(draft("v4", part="p3"))  # staged: not part of the grid
        seal(store, "v5", ["a"], ds="e", part="p9")  # another dataset
        grid = store.reads.partition_grid("d")
        assert [r.partition for r in grid] == ["p2", "p1"]
        p1 = grid[1]
        assert (p1.version_id, p1.dataset, p1.upserts, p1.deletes, p1.sealed_at) == (
            "v3",
            "d",
            4,
            0,
            T0 + timedelta(hours=1),
        )
        assert dict(p1.cells) == {"a": "applied", "b": "pending"}
        assert dict(grid[0].cells) == {"a": "pending", "b": "pending"}
        assert grid[0].seal_seq > 0 and grid[1].seal_seq > grid[0].seal_seq
        assert store.reads.partition_grid("nothing") == []

    def test_partition_grid_filters_by_time_and_pages_by_partition(self, store: Store, clock: FrozenClock):
        seal(store, "v1", ["a"], part="p1")
        clock.advance(timedelta(hours=2))
        seal(store, "v2", ["a"], part="p2")
        seal(store, "v3", ["a"], part="p3")
        assert [r.partition for r in store.reads.partition_grid("d", since=T0 + timedelta(hours=1))] == [
            "p3",
            "p2",
        ]
        assert store.reads.partition_grid("d", since=T0 + timedelta(hours=3)) == []
        first = store.reads.partition_grid("d", limit=2)
        assert [r.partition for r in first] == ["p3", "p2"]
        rest = store.reads.partition_grid("d", cursor=first[-1].partition)
        assert [r.partition for r in rest] == ["p1"]

    def _fail(self, store: Store, route: str, vid: str, error: str):
        store.ledger.transition(route, vid, "running", actor="t")
        store.ledger.transition(route, vid, "failed", actor="t", error=error)

    def test_failures_group_by_route_and_message(self, store: Store):
        for vid, part in (("v1", "p1"), ("v2", "p2"), ("v3", "p3")):
            seal(store, vid, ["a", "b"], part=part)
        self._fail(store, "a", "v1", "disk full")
        self._fail(store, "a", "v2", "disk full")
        self._fail(store, "a", "v3", "TimeoutError: slow sink")
        self._fail(store, "b", "v1", "disk full")
        store.ledger.transition("b", "v2", "running", actor="t")  # running is not a failure
        groups = store.reads.failure_groups()
        assert [(g.route, g.error_class, g.message, g.count) for g in groups] == [
            ("a", "PermanentError", "disk full", 2),
            ("a", "TimeoutError", "slow sink", 1),
            ("b", "PermanentError", "disk full", 1),
        ]
        assert groups[0].versions == ("v1", "v2") and groups[1].versions == ("v3",)
        assert [g.message for g in store.reads.failure_groups(route="b")] == ["disk full"]
        page = store.reads.failure_groups(limit=2)
        rest = store.reads.failure_groups(cursor=failure_cursor(page[-1]))
        assert page + rest == groups and len(rest) == 1
        store.ledger.transition("a", "v1", "pending", actor="op")  # retried: out of the group
        assert store.reads.failure_groups()[0].count == 1

    def test_failure_samples_are_capped(self, store: Store):
        for n in range(8):
            seal(store, f"v{n}", ["a"], part=f"p{n}")
            self._fail(store, "a", f"v{n}", "same")
        (group,) = store.reads.failure_groups()
        assert group.count == 8 and len(group.versions) == 5

    def test_delivery_counts_by_status(self, store: Store):
        assert store.reads.delivery_counts() == {}
        seal(store, "v1", ["a", "b"], part="p1")
        seal(store, "v2", ["a"], part="p2")
        self._fail(store, "a", "v1", "x")
        assert store.reads.delivery_counts() == {"pending": 2, "failed": 1}

    # retention queries

    def test_pending_older_than_lists_only_pending_deliveries_past_the_age(
        self, store: Store, clock: FrozenClock
    ):
        seal(store, "v1", ["a"])
        clock.advance(timedelta(minutes=2))
        seal(store, "v2", ["a"], part="q")
        seal(store, "v3", ["a"], part="r")
        store.ledger.transition("a", "v3", "running", actor="t")
        clock.advance(timedelta(seconds=30))
        assert [d.version_id for d in store.ledger.pending_older_than(timedelta(minutes=1))] == ["v1"]
        assert [d.version_id for d in store.ledger.pending_older_than(timedelta(seconds=10))] == ["v1", "v2"]
        later = clock.now() + timedelta(minutes=5)
        assert len(store.ledger.pending_older_than(timedelta(minutes=1), now=later)) == 2

    def test_open_versions_are_those_with_a_delivery_that_is_not_closed(self, store: Store):
        for vid, part in (("v1", "p"), ("v2", "q"), ("v3", "r")):
            seal(store, vid, ["a", "b"], part=part)
        for route in ("a", "b"):
            store.ledger.transition(route, "v1", "running", actor="t")
            store.ledger.transition(route, "v1", "applied", actor="t")
        store.ledger.transition("a", "v2", "running", actor="t")
        store.ledger.transition("a", "v2", "applied", actor="t")
        assert store.ledger.open_versions(["v1", "v2", "v3", "nope"]) == {"v2", "v3"}

    def test_closed_deliveries_are_deleted_with_their_attempts_once_old_enough(
        self, store: Store, clock: FrozenClock
    ):
        seal(store, "v1", ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "applied", actor="t")
        clock.advance(timedelta(days=10))
        seal(store, "v2", ["a"], part="q")
        store.ledger.transition("a", "v2", "running", actor="t")
        store.ledger.transition("a", "v2", "applied", actor="t")
        seal(store, "v3", ["a"], part="r")  # pending, so not deleted whatever its age
        assert store.ledger.delete_closed_before(T0 + timedelta(days=5)) == 1
        with pytest.raises(KeyError):
            store.ledger.get("a", "v1")
        assert store.ledger.get("a", "v2").status == "applied"
        assert store.ledger.delete_closed_before(clock.now() + timedelta(days=30)) == 1
        assert store.ledger.get("a", "v3").status == "pending"

    def test_runs_ready_to_seal_have_every_slice_done_and_a_staged_version(self, store: Store):
        store.versions.register_slices("r1", "d", ["p", "q"])
        store.versions.stage(draft("v1", run="r1", part="p"))
        store.versions.finish_slice("r1", "d", "p")
        assert store.versions.runs_ready_to_seal() == []  # q is not done
        store.versions.finish_slice("r1", "d", "q")
        assert store.versions.runs_ready_to_seal() == [("r1", "d")]
        store.versions.stage(draft("v9", run="r2", part="p"))  # a run with no slices is not one
        assert store.versions.runs_ready_to_seal() == [("r1", "d")]
        store.versions.seal(["v1"], ["a"])
        assert store.versions.runs_ready_to_seal() == []

    def test_get_many_leaves_out_unknown_versions(self, store: Store):
        store.versions.stage(draft("v1"))
        store.versions.stage(draft("v2", part="q"))
        got = store.versions.get_many(["v2", "nope", "v1"])
        assert sorted(v.version_id for v in got) == ["v1", "v2"]
        assert store.versions.get_many([]) == []

    def test_old_manifests_are_listed_unless_newest_of_their_partition_or_delivered(
        self, store: Store, clock: FrozenClock
    ):
        seal(store, "v1", [])
        seal(store, "v2", [])  # the newest of partition p
        seal(store, "v3", ["a"], part="q")  # delivered
        seal(store, "v4", [], part="r")  # the only one of its partition
        clock.advance(timedelta(days=1))
        assert store.versions.sealed_before(T0) == []  # sealed at T0, not before
        assert [v.version_id for v in store.versions.sealed_before(clock.now())] == ["v1"]
        assert store.versions.delete_many(["v1", "nope"]) == 1
        assert store.versions.sealed_before(clock.now()) == []
        with pytest.raises(KeyError):
            store.versions.get("v1")

    def test_old_runs_are_deleted_with_their_tasks_and_logs_unless_going_or_open(
        self, store: Store, clock: FrozenClock
    ):
        done = store.runs.create_run("f", "manual", "t", {})
        store.runs.set_run_status(done, "succeeded")
        task = store.runs.start_task(done, "plan", "plan:1")
        store.runs.log(task, "info", "hello")
        going = store.runs.create_run("f", "manual", "t", {})
        store.runs.set_run_status(going, "running")
        held = store.runs.create_run("f", "manual", "t", {})
        store.runs.set_run_status(held, "failed")
        seal(store, "v1", ["a"], run=held)
        recent_at = clock.now() + timedelta(days=100)
        clock.advance(timedelta(days=200))
        recent = store.runs.create_run("f", "manual", "t", {})
        assert store.runs.delete_runs_before(recent_at) == 1
        assert {r.run_id for r in store.runs.list_runs()} == {going, held, recent}
        assert store.runs.list_tasks(done) == []

    def test_old_log_lines_are_deleted(self, store: Store, clock: FrozenClock):
        run = store.runs.create_run("f", "manual", "t", {})
        task = store.runs.start_task(run, "plan", "plan:1")
        store.runs.log(task, "info", "old")
        clock.advance(timedelta(days=40))
        store.runs.log(task, "info", "new")
        assert store.runs.delete_logs_before(T0 + timedelta(days=30)) == 1
        assert store.runs.task_logs(task) == ["info: new"]


def flow_spec(name: str, routes: list[str], owner: str | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "owner": owner,
        "datasets": [{"name": f"{name}_ds"}],
        "routes": [{"name": route, "connection": route} for route in routes],
        "triggers": [],
    }

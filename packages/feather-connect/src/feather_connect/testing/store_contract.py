"""The contract every store and spool backend must pass.

A backend's test module subclasses `StoreContract` and defines the fixtures `store`, `spool` and `clock`.
`clock` is the `FrozenClock` that `store` and `spool` were built with, so tests can check the times they
record. A backend with only a spool subclasses `SpoolContract`, which needs `spool` and `clock`.
"""

import pickle
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest

from feather_connect.clock import FrozenClock
from feather_connect.core.connector import InboundRequest
from feather_connect.core.records import Record, row_hash
from feather_connect.ids import stable_id
from feather_connect.storage.port import (
    CLOSED,
    TRANSITIONS,
    BudgetSpec,
    DeliveryStatus,
    DuplicateFile,
    DuplicateRegistry,
    DuplicateUser,
    DuplicateVersion,
    InboxDay,
    InUse,
    InvalidTransition,
    KeyMismatch,
    PackageVersion,
    RegistryFile,
    ResponseRow,
    Role,
    SliceFinished,
    StaleWriter,
    StateRow,
    Store,
    TokenRow,
    VersionDraft,
    VersionNotReady,
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


def inbound(body: bytes = b"{}", headers: dict[str, str] | None = None, at: datetime = T0) -> InboundRequest:
    return InboundRequest("POST", "/ingress/default/d", headers or {}, body, at)


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


class ResponseStoreContract:
    """The answers of destinations per record (ruling 5). A part of `StoreContract`, which needs only `store`."""

    def test_responses_are_kept_per_record_and_listed_by_chunk_then_key(self, store: Store):
        store.responses.record(
            [
                ResponseRow("a", "v1", 1, "k2", 201, {"erp_id": "e-3"}),
                ResponseRow("a", "v1", 0, "k9", 201, {"erp_id": "e-1", "at": T0, "n": Decimal("1.5")}),
                ResponseRow("a", "v1", 0, "k10", 422, {}, sent={"customer": None, "lines": [1, 2]}),
                ResponseRow("b", "v1", 0, "k1", 201, {"erp_id": "other route"}),
            ]
        )
        rows = store.responses.of_delivery("a", "v1")
        assert [(r.chunk_no, r.key) for r in rows] == [(0, "k10"), (0, "k9"), (1, "k2")]
        assert rows[1].fields == {"erp_id": "e-1", "at": T0, "n": Decimal("1.5")}  # values keep their types
        assert rows[0].sent == {"customer": None, "lines": [1, 2]} and rows[1].sent is None
        assert rows[0].status == 422 and rows[0].at == T0
        assert store.responses.of_delivery("a", "v1", limit=1) == rows[:1]
        assert store.responses.of_delivery("a", "nope") == []

    def test_a_chunk_sent_again_replaces_its_answers(self, store: Store, clock: FrozenClock):
        store.responses.record([ResponseRow("a", "v1", 0, "k", 500, {"try": 1})])
        clock.advance(timedelta(minutes=1))
        store.responses.record([ResponseRow("a", "v1", 0, "k", 201, {"try": 2})])
        (row,) = store.responses.of_delivery("a", "v1")
        assert (row.status, row.fields, row.at) == (201, {"try": 2}, T0 + timedelta(minutes=1))

    def test_an_answer_with_no_fields_keeps_the_fields_before(self, store: Store):  # I3
        store.responses.record([ResponseRow("a", "v1", 0, "k", 201, {"erp_id": "e-1"})])
        store.responses.record(
            [ResponseRow("a", "v1", 0, "k", 409, {})]
        )  # a resend, answered "applied already"
        (row,) = store.responses.of_delivery("a", "v1")
        assert (row.status, row.fields) == (409, {"erp_id": "e-1"})
        store.responses.replace_chunk("a", "v1", 0, [ResponseRow("a", "v1", 0, "k", 409, {})])
        assert store.responses.of_delivery("a", "v1")[0].fields == {"erp_id": "e-1"}

    def test_replacing_a_chunk_drops_the_answers_of_records_not_sent_again(self, store: Store):  # M8
        store.responses.record(
            [
                ResponseRow("a", "v1", 0, "k1", 201, {"erp_id": "e-1"}),
                ResponseRow("a", "v1", 0, "k2", 201, {"erp_id": "e-2"}),
                ResponseRow("a", "v1", 1, "k3", 201, {"erp_id": "e-3"}),
            ]
        )
        store.responses.replace_chunk(
            "a", "v1", 0, [ResponseRow("a", "v1", 0, "k2", 200, {"erp_id": "e-2b"})]
        )
        rows = store.responses.of_delivery("a", "v1")
        assert [(r.chunk_no, r.key, r.fields["erp_id"]) for r in rows] == [
            (0, "k2", "e-2b"),
            (1, "k3", "e-3"),
        ]
        store.responses.replace_chunk("a", "v1", 1, [])  # a chunk that got no answers keeps none
        assert [r.key for r in store.responses.of_delivery("a", "v1")] == ["k2"]

    def test_find_gives_the_latest_answer_for_a_record(self, store: Store):
        store.responses.record(
            [ResponseRow("a", "v1", 0, "k", 422, {}, sent={"x": 1}), ResponseRow("a", "v1", 3, "k", 201, {})]
        )
        found = store.responses.find("a", "v1", "k")
        assert found is not None and found.chunk_no == 3
        assert store.responses.find("a", "v1", "other") is None
        store.responses.record([])  # nothing to keep is fine

    def test_answers_go_when_their_delivery_goes(self, store: Store, clock: FrozenClock):
        seal(store, "v1", ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "applied", actor="t")
        seal(store, "v2", ["a"], part="q")
        store.responses.record(
            [ResponseRow("a", "v1", 0, "k", 201, {}), ResponseRow("a", "v2", 0, "k", 201, {})]
        )
        assert store.ledger.delete_closed_before(clock.now() + timedelta(days=1)) == 1
        assert store.responses.of_delivery("a", "v1") == []
        assert len(store.responses.of_delivery("a", "v2")) == 1  # pending, so kept


class KeyIndexContract:
    """The key index: each key of a version, written when the version seals and in the same transaction, so that the
    versions that held a record are found without reading the spool (ruling 11)."""

    def _put(self, spool: Spool, vid: str, chunk_no: int, records: list[Record]) -> None:
        spool.put_chunk(vid, chunk_no, "data", records_to_table(records))

    def test_sealing_indexes_each_key_of_the_version(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        self._put(spool, "v1", 0, upserts(("k1", {"x": 1}), ("k2", {"x": 2})))
        self._put(spool, "v1", 1, [Record("k3", "delete", {}, 0)])
        seal(store, "v1", ["a"])
        version = store.versions.get("v1")
        [seen] = store.versions.key_history("d", "k1")
        assert (seen.version_id, seen.dataset, seen.partition, seen.key, seen.op, seen.chunk_no) == (
            "v1",
            "d",
            "p",
            "k1",
            "upsert",
            0,
        )
        assert seen.row_hash == row_hash({"x": 1}) and seen.seal_seq == version.seal_seq
        assert seen.sealed_at == version.sealed_at and seen.run_id == "r1"
        [gone] = store.versions.key_history("d", "k3")
        assert (gone.op, gone.chunk_no) == ("delete", 1)
        assert store.versions.key_history("d", "nope") == []
        assert store.versions.key_history("other", "k1") == []

    def test_the_history_is_oldest_first_and_holds_sealed_versions_only(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        self._put(spool, "v1", 0, upserts(("k", {"x": 1})))
        seal(store, "v1", ["a"])
        self._put(spool, "v2", 0, upserts(("k", {"x": 2})))
        store.versions.stage(draft("v2", part="q", run="r2"))
        assert [s.version_id for s in store.versions.key_history("d", "k")] == ["v1"]
        store.versions.seal(["v2"], ["a"])
        store.versions.seal(["v2"], ["a"])  # sealing again indexes nothing more
        found = store.versions.key_history("d", "k")
        assert [(s.version_id, s.partition, s.run_id) for s in found] == [
            ("v1", "p", "r1"),
            ("v2", "q", "r2"),
        ]
        assert [s.version_id for s in store.versions.key_history("d", "k", limit=1)] == [
            "v2"
        ]  # the newest kept

    def test_a_push_batch_is_indexed_as_it_seals(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        store.inbox.put("d", inbound())
        seqs = [e.event_seq for e in store.inbox.take("d", 1)]
        self._put(spool, "v1", 0, upserts(("INV-1", {"total": 5})))
        store.inbox.seal_batch(seqs, [draft("v1", complete=False)], ["a"])
        assert [s.version_id for s in store.versions.key_history("d", "INV-1")] == ["v1"]

    def test_a_refused_seal_indexes_nothing(self, store: Store, spool: Spool):
        spool.bind(store.versions)
        store.versions.claim("v1", "first", "r1", "d", "p")
        self._put(spool, "v1", 0, upserts(("k", {"x": 1})))
        store.versions.stage(draft("v1"), "first")
        store.versions.claim("v1", "second", "r1", "d", "p")
        with pytest.raises(StaleWriter):
            store.versions.seal(["v1"], ["a"], "first")
        assert store.versions.key_history("d", "k") == []


class TraceContract:
    """What a trace of one record reads beside the key index: its quarantine entries and the audit rows of a route."""

    def test_the_quarantine_entries_of_a_key(self, store: Store):
        store.quarantine.add("a", "v1", "k", "bad", payload={"x": 1})
        store.quarantine.add("a", "v2", "k", "worse")
        store.quarantine.add("b", "v9", "k", "of a version not asked for")
        store.quarantine.add("a", "v1", "j", "of another key")
        store.quarantine.add(None, None, "k", "unreadable", "d", "p", raw="{oops")
        store.quarantine.add(None, None, "k", "of another dataset", "e", "p")
        store.quarantine.close_extraction("d", ["k"])
        found = store.quarantine.of_key("d", "k", ["v1", "v2"])
        assert [(q.route, q.version_id, q.error, q.status) for q in found] == [
            ("a", "v1", "bad", "open"),
            ("a", "v2", "worse", "open"),
            (None, None, "unreadable", "superseded"),
        ]
        assert found[0].payload == {"x": 1} and found[2].raw == "{oops"
        assert store.quarantine.of_key("d", "nope", ["v1"]) == []

    def test_the_audit_rows_of_a_target(self, store: Store, clock: FrozenClock):
        store.audit.record("human", "ann", "retry_deliveries", "route:a", {"reason": "first"}, "ok")
        store.audit.record("human", "ann", "skip_deliveries", "route:b", {"reason": "other"}, "ok")
        clock.advance(timedelta(minutes=1))
        store.audit.record("human", "bob", "replay_deliveries", "route:a", {"reason": "second"}, "ok")
        found = store.audit.for_target("route:a")
        assert [(r.actor, r.action, r.request["reason"]) for r in found] == [
            ("ann", "retry_deliveries", "first"),
            ("bob", "replay_deliveries", "second"),
        ]
        assert [r.actor for r in store.audit.for_target("route:a", since=T0 + timedelta(seconds=30))] == [
            "bob"
        ]
        assert store.audit.for_target("route:z") == []


class TaskRecordsContract:
    """The reads behind a task's Records panel: a page of a version's keys at a time, never the whole version."""

    def _sealed(self, store: Store, spool: Spool, vid: str, keys: list[str], part: str = "p") -> None:
        spool.bind(store.versions)
        spool.put_chunk(vid, 0, "data", records_to_table(upserts(*((k, {"k": k}) for k in keys))))
        seal(store, vid, ["a"], part=part)

    def test_a_task_is_found_by_its_id(self, store: Store):
        run_id = store.runs.create_run("f", "manual", "ann", {})
        task_id = store.runs.start_task(run_id, "extract", "extract:x", "d", "p")
        found = store.runs.get_task(task_id)
        assert (found.task_id, found.run_id, found.kind, found.dataset) == (task_id, run_id, "extract", "d")
        with pytest.raises(KeyError):
            store.runs.get_task("no-such-task")

    def test_keys_come_a_page_at_a_time_in_key_order(self, store: Store, spool: Spool):
        self._sealed(store, spool, "v1", ["b", "a", "d", "c", "\u00e9"])
        self._sealed(store, spool, "v2", ["a", "z"], part="q")
        first = store.versions.keys_page("d", ["v1", "v2"], limit=3)
        assert [(s.key, s.version_id) for s in first] == [("a", "v1"), ("a", "v2"), ("b", "v1")]
        assert first[0].seal_seq is not None and first[0].run_id == "r1" and first[1].partition == "q"
        after = (first[-1].key, first[-1].version_id)
        rest = store.versions.keys_page("d", ["v1", "v2"], after=after, limit=10)
        assert [s.key for s in rest] == ["c", "d", "z", "\u00e9"]  # code point order
        assert [s.key for s in store.versions.keys_page("d", ["v1"], limit=10)] == [
            "a",
            "b",
            "c",
            "d",
            "\u00e9",
        ]
        assert store.versions.keys_page("d", [], limit=10) == []
        assert store.versions.keys_page("other", ["v1"], limit=10) == []

    def test_keys_can_be_searched_by_their_start(self, store: Store, spool: Spool):
        self._sealed(store, spool, "v1", ["INV-1", "INV-10", "INV-2", "ORD-1", "inv-3", "INV%", "INV_1"])
        found = store.versions.keys_page("d", ["v1"], prefix="INV-1", limit=10)
        assert [s.key for s in found] == ["INV-1", "INV-10"]
        assert [s.key for s in store.versions.keys_page("d", ["v1"], prefix="INV%", limit=10)] == ["INV%"]
        assert [s.key for s in store.versions.keys_page("d", ["v1"], prefix="INV_", limit=10)] == ["INV_1"]
        page = store.versions.keys_page("d", ["v1"], prefix="INV", after=("INV-10", "v1"), limit=10)
        assert [s.key for s in page] == ["INV-2", "INV_1"]

    def test_the_earlier_sightings_of_keys(self, store: Store, spool: Spool):
        self._sealed(store, spool, "v1", ["a", "b"])
        spool.put_chunk("v2", 0, "data", records_to_table([Record("b", "delete", {}, 0)]))
        seal(store, "v2", ["a"])
        self._sealed(store, spool, "v3", ["a", "b", "c"])
        v3 = store.versions.get("v3")
        assert v3.seal_seq is not None
        earlier = store.versions.earlier_sightings("d", ["a", "b", "c"], v3.seal_seq)
        assert {k: (s.version_id, s.op) for k, s in earlier.items()} == {
            "a": ("v1", "upsert"),
            "b": ("v2", "delete"),
        }
        assert store.versions.earlier_sightings("d", [], v3.seal_seq) == {}

    def test_the_answers_of_some_records_of_a_delivery(self, store: Store):
        store.responses.record(
            [
                ResponseRow("a", "v1", 0, "k1", 201, {"erp_id": "e-1"}),
                ResponseRow("a", "v1", 1, "k1", 200, {"erp_id": "e-1b"}),
                ResponseRow("a", "v1", 0, "k2", 422, {}, sent={"x": 1}),
                ResponseRow("a", "v1", 0, "k3", 201, {}),
                ResponseRow("b", "v1", 0, "k1", 500, {}),
            ]
        )
        found = store.responses.for_keys("a", "v1", ["k1", "k2", "nope"])
        assert {k: (r.status, dict(r.fields)) for k, r in found.items()} == {
            "k1": (200, {"erp_id": "e-1b"}),  # the highest chunk's answer
            "k2": (422, {}),
        }
        assert found["k2"].sent == {"x": 1}
        assert store.responses.for_keys("a", "v1", []) == {}

    def test_the_quarantine_entries_of_versions(self, store: Store):
        store.quarantine.add(None, "v1", "k1", "unreadable", "d", "p", raw="{oops")
        store.quarantine.add(None, "v2", "k2", "no key", "d", "q")
        store.quarantine.add("a", "v1", "k1", "rejected by a")
        store.quarantine.add("b", "v1", "k3", "rejected by b")
        store.quarantine.add(None, "v9", "k9", "of another version", "d", "p")
        read = store.quarantine.of_versions(["v1", "v2"], None)
        assert [(q.version_id, q.key, q.error) for q in read] == [
            ("v1", "k1", "unreadable"),
            ("v2", "k2", "no key"),
        ]
        assert [q.error for q in store.quarantine.of_versions(["v1"], "a")] == ["rejected by a"]
        assert [q.key for q in store.quarantine.of_versions(["v1"], "b", keys=["k3", "k1"])] == ["k3"]
        assert store.quarantine.of_versions(["v1"], "b", keys=[]) == []
        assert len(store.quarantine.of_versions(["v1", "v2"], None, limit=1)) == 1


class StoreContract(
    SpoolContract, ResponseStoreContract, KeyIndexContract, TraceContract, TaskRecordsContract
):
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

    def test_budgets_of_one_vendor_account_share_their_windows(self, store: Store):  # ADR-0013 rule 8
        limits = parse_limits(["10/minute"])
        store.budgets.define(BudgetSpec("zoho-a", limits, account="zakya:60001"))
        store.budgets.define(BudgetSpec("zoho-b", limits, account="zakya:60001"))
        store.budgets.define(BudgetSpec("other", limits, account="zakya:70002"))
        store.budgets.acquire("zoho-a", 6)
        assert [u.used for u in store.budgets.usage("zoho-b")] == [6]
        assert not store.budgets.acquire("zoho-b", 5).granted
        assert [u.used for u in store.budgets.usage("other")] == [0]
        store.budgets.define(BudgetSpec("zoho-a", limits, account="zakya:60001"))  # again: the use stays
        assert [u.used for u in store.budgets.usage("zoho-a")] == [6]

    def test_a_budget_that_disagrees_with_its_account_is_refused_and_resets_nothing(self, store: Store):
        limits = parse_limits(["10/minute"])
        store.budgets.define(BudgetSpec("zoho-a", limits, account="zakya:60001"))
        store.budgets.define(BudgetSpec("zoho-b", limits, account="zakya:60001"))
        store.budgets.acquire("zoho-a", 6)
        with pytest.raises(ValueError, match="zoho-a"):
            store.budgets.define(BudgetSpec("zoho-b", parse_limits(["20/minute"]), account="zakya:60001"))
        with pytest.raises(ValueError, match="zoho-"):
            store.budgets.define(BudgetSpec("zoho-c", parse_limits(["20/minute"]), account="zakya:60001"))
        assert [u.used for u in store.budgets.usage("zoho-b")] == [6]
        assert [w.window.units for w in store.budgets.usage("zoho-b")] == [10]
        store.budgets.define(BudgetSpec("alone", limits, account="zakya:70002"))
        store.budgets.acquire("alone", 4)
        store.budgets.define(BudgetSpec("alone", parse_limits(["20/minute"]), account="zakya:70002"))
        assert [(u.window.units, u.used) for u in store.budgets.usage("alone")] == [
            (20, 0)
        ]  # its only budget

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

    def test_since_watermark_stops_at_the_first_version_below_it(self, store: Store):  # S6
        seal(store, "v1", ["a"], wm=50)  # older than the lookback, whatever its mark
        seal(store, "v2", ["a"], wm=10)
        seal(store, "v3", ["a"], wm=30)
        assert [v.version_id for v in store.versions.since_watermark("d", "p", 20)] == ["v3"]

    def test_latest_watermark_follows_new_seals(self, store: Store):
        seal(store, "v1", ["a"], wm=10)
        assert store.versions.latest_watermark("d") == 10
        seal(store, "v2", ["a"], wm=40)
        seal(store, "v3", ["a"], wm=20)
        assert store.versions.latest_watermark("d") == 40

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

    def test_runs_are_counted_by_status_and_day_and_found_by_id(self, store: Store, clock: FrozenClock):
        first = store.runs.create_run("f", "manual", "ann", {})
        clock.advance(timedelta(days=1))
        second = store.runs.create_run("f", "manual", "ann", {})
        third = store.runs.create_run("g", "manual", "ann", {})
        store.runs.set_run_status(third, "failed", "boom")
        assert store.runs.count_runs() == 3 and store.runs.count_runs("queued") == 2
        assert store.runs.count_runs("failed", since=clock.now()) == 1
        assert store.runs.count_runs(since=clock.now() + timedelta(seconds=1)) == 0
        assert store.runs.runs_by_day(T0) == {T0.date(): (1, 0), (T0 + timedelta(days=1)).date(): (2, 1)}
        assert store.runs.runs_by_day(clock.now()) == {clock.now().date(): (2, 1)}
        assert [r.run_id for r in store.runs.find_runs(second.lower())] == [second]
        assert {r.run_id for r in store.runs.find_runs("")} == {first, second, third}
        assert len(store.runs.find_runs("", limit=2)) == 2 and store.runs.find_runs("%") == []

    def test_sealed_times_and_delivery_counts_come_in_one_read(self, store: Store, clock: FrozenClock):
        seal(store, "v1", ["a", "b"], ds="d1")
        clock.advance(timedelta(minutes=5))
        seal(store, "v2", ["a"], ds="d2")
        assert store.versions.last_sealed_many(["d1", "d2", "nothing"]) == {
            "d1": store.versions.last_sealed_at("d1"),
            "d2": T0 + timedelta(minutes=5),
        }
        assert store.versions.last_sealed_many([]) == {}
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "failed", actor="t", error="no")
        assert store.ledger.count_by_status(["a", "b"], ["failed", "pending"]) == {
            ("a", "failed"): 1,
            ("a", "pending"): 1,
            ("b", "pending"): 1,
        }
        assert (
            store.ledger.count_by_status(["b"], ["failed"]) == {}
            and store.ledger.count_by_status([], ["failed"]) == {}
        )

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

    def test_listing_tasks_narrowed_and_paged(self, store: Store):
        rid = store.runs.create_run("f", "manual", "ann", {})
        other = store.runs.create_run("f", "manual", "ann", {})
        first = store.runs.start_task(rid, "plan", "a")
        second = store.runs.start_task(rid, "apply", "b")
        third = store.runs.start_task(rid, "apply", "c")
        store.runs.start_task(other, "apply", "c")
        store.runs.finish_task(first, "succeeded")
        store.runs.finish_task(second, "failed", "bad")

        def ids(**kw: Any) -> list[str]:
            return [t.task_id for t in store.runs.list_tasks(rid, **kw)]

        assert ids() == [first, second, third]
        assert ids(status="running") == [third] and ids(status="failed") == [second]
        assert ids(status="succeeded") == [first]
        assert ids(kinds=("apply",)) == [second, third] and ids(kinds=("apply", "plan"), limit=1) == [first]
        assert ids(newest_first=True) == [third, second, first]
        assert ids(limit=2, newest_first=True) == [third, second]
        assert ids(after_task=first) == [second, third]
        assert ids(after_task=second, newest_first=True) == [first]

    def test_finding_tasks_by_ref(self, store: Store):
        rid = store.runs.create_run("f", "manual", "ann", {})
        other = store.runs.create_run("f", "manual", "ann", {})
        one = store.runs.start_task(rid, "apply", "apply:r:v:0:0")
        two = store.runs.start_task(rid, "apply", "apply:r:v:0:0#1")
        store.runs.start_task(rid, "apply", "apply:r:w:0:0")
        store.runs.start_task(other, "apply", "apply:r:v:0:0")
        row = store.runs.task_by_ref(rid, "apply:r:v:0:0")
        assert row is not None and row.task_id == one
        assert store.runs.task_by_ref(rid, "nothing") is None
        assert [t.task_id for t in store.runs.tasks_by_ref(rid, "apply:r:v:")] == [one, two]
        assert store.runs.tasks_by_ref(rid, "apply:%") == []

    def test_counting_tasks(self, store: Store):
        rid = store.runs.create_run("f", "manual", "ann", {})
        store.runs.finish_task(store.runs.start_task(rid, "plan", "a"), "succeeded")
        store.runs.finish_task(store.runs.start_task(rid, "plan", "b"), "succeeded")
        store.runs.finish_task(store.runs.start_task(rid, "skip", "c"), "skipped", "paused")
        assert dict(store.runs.count_tasks(rid)) == {("plan", "succeeded"): 2, ("skip", "skipped"): 1}

    def test_one_stream_run_per_flow_and_day(self, store: Store):
        first = store.runs.stream_run("f", "2026-10-18")
        run = store.runs.get_run(first)
        assert (run.trigger, run.status, run.params) == ("stream", "succeeded", {"day": "2026-10-18"})
        assert store.runs.stream_run("f", "2026-10-18") == first
        assert store.runs.stream_run("g", "2026-10-18") != first
        assert store.runs.stream_run("f", "2026-10-19") != first

    def test_a_version_keeps_its_tick_and_is_found_by_it(self, store: Store):
        rid = store.runs.create_run("f", "stream", "system", {})
        for vid, tick in (("v1", "t1"), ("v2", "t2")):
            store.versions.stage(replace(draft(vid, run=rid), tick=tick))
        assert [v.version_id for v in store.versions.staged_for_run(rid, "d", "t1")] == ["v1"]
        assert len(store.versions.staged_for_run(rid, "d")) == 2
        store.versions.seal(["v1", "v2"], [])
        assert [v.version_id for v in store.versions.sealed_for_run(rid, "d", "t2")] == ["v2"]
        assert store.versions.get("v2").tick == "t2"
        assert [v.version_id for v in store.versions.newest_sealed_for_run(rid, "d", 1)] == ["v2"]

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

    def test_a_quarantined_record_keeps_its_payload_or_raw_text(self, store: Store):  # ruling 11, B5
        store.quarantine.add("a", "v1", "k", "HTTP 422", payload={"total": Decimal("9.50"), "lines": [1]})
        store.quarantine.add(None, "v1", "<missing>", "short row", dataset="d", partition="p", raw="a;b")
        by_key = {q.key: q for q in store.quarantine.list_open()}
        assert by_key["k"].payload == {"total": Decimal("9.50"), "lines": [1]} and by_key["k"].raw is None
        assert by_key["<missing>"].raw == "a;b" and by_key["<missing>"].payload is None

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

    def test_quarantine_pages_and_counts_the_open_entries(self, store: Store):  # Stage 2 M7
        for n in range(5):
            store.quarantine.add("a" if n % 2 else "b", "v1", f"k{n}", "bad")
        assert store.quarantine.count_open() == 5 and store.quarantine.count_open("a") == 2
        first = [q.key for q in store.quarantine.list_open(limit=2)]
        rest = [q.key for q in store.quarantine.list_open(limit=10, offset=2)]
        assert first == ["k0", "k1"] and rest == ["k2", "k3", "k4"]
        assert [q.key for q in store.quarantine.list_open("b", limit=1, offset=1)] == ["k2"]

    def test_the_last_sealed_time_of_a_dataset_ignores_a_staged_version(self, store: Store):  # Stage 2 M8
        assert store.versions.last_sealed_at("d") is None
        seal(store, "v1", ["a"], part="p")
        sealed = store.versions.get("v1").sealed_at
        store.versions.stage(draft("v2", part="q"))  # staged later, so the newest by stage order
        assert sealed is not None and store.versions.last_sealed_at("d") == sealed

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

    def test_advance_tick_lets_one_of_two_leaders_fire(self, store: Store):  # Stage 2 M9
        t1, t2 = T0 + timedelta(minutes=1), T0 + timedelta(minutes=2)
        assert store.schedules.advance_tick("f", "t", None, T0) is True  # first seen
        assert store.schedules.advance_tick("f", "t", None, T0) is False
        assert store.schedules.advance_tick("f", "t", T0, t1) is True
        assert store.schedules.advance_tick("f", "t", T0, t2) is False  # the other leader read T0 too
        assert store.schedules.last_tick("f", "t") == t1

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

    def test_forget_stopped_workers_removes_dead_and_draining_ones_seen_long_ago(
        self, store: Store, clock: FrozenClock
    ):  # Stage 2 M6
        for worker in ("alive", "dead", "draining", "recent"):
            store.workers.heartbeat(worker, {}, "sha", 4)
        store.workers.mark("dead", "dead")
        store.workers.mark("draining", "draining")
        clock.advance(timedelta(days=2))
        store.workers.heartbeat("recent", {}, "sha", 4)
        store.workers.mark("recent", "dead")
        assert store.workers.forget_stopped(clock.now() - timedelta(days=1)) == 2
        assert [w.worker_id for w in store.workers.list_workers()] == ["alive", "recent"]

    def test_delete_expired_overrides(self, store: Store, clock: FrozenClock):  # Stage 2 M6
        store.overrides.set("t", "old", 1, actor="a", expires_at=clock.now() + timedelta(minutes=1))
        store.overrides.set("t", "later", 2, actor="a", expires_at=clock.now() + timedelta(days=1))
        store.overrides.set("t", "for-ever", 3, actor="a")
        clock.advance(timedelta(hours=1))
        assert store.overrides.delete_expired() == 1
        assert store.overrides.delete_expired() == 0
        assert (store.overrides.get("t", "later"), store.overrides.get("t", "for-ever")) == (2, 3)

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
        user_id = store.users.create_user("op", "hash-1", "operator")
        found = store.users.get_user("op")
        assert found is not None and (found.user_id, found.username, found.password_hash, found.role) == (
            user_id,
            "op",
            "hash-1",
            "operator",
        )
        assert found.created_at == T0
        assert store.users.get_user("OP") is None and store.users.get_user("other") is None
        with pytest.raises(DuplicateUser):
            store.users.create_user("op", "hash-2", "admin")

    def test_a_users_password_hash_can_be_replaced(self, store: Store):
        user_id = store.users.create_user("op", "hash-1", "admin")
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
        token_id = store.users.create_token("ci", "h1", expiry, "operator", "ann")
        other = store.users.create_token("forever", "h2", None, "admin", None)
        found = store.users.find_token("h1")
        assert found == TokenRow(token_id, "ci", "h1", T0, expiry, None, "operator", "ann")
        clock.advance(timedelta(hours=1))
        store.users.revoke_token(token_id)
        clock.advance(timedelta(hours=1))
        store.users.revoke_token(token_id)  # the first revocation stands
        store.users.revoke_token("no-such-token")
        revoked = store.users.find_token("h1")
        assert revoked is not None and revoked.revoked_at == T0 + timedelta(hours=1)
        untouched = store.users.find_token("h2")
        assert untouched is not None and untouched.token_id == other and untouched.revoked_at is None

    def test_a_users_role_can_be_changed(self, store: Store):
        store.users.create_user("ann", "hash-1", "admin")
        store.users.set_role("ann", "operator")
        found = store.users.get_user("ann")
        assert found is not None and (found.role, found.password_hash) == ("operator", "hash-1")
        store.users.set_role("ann", "admin")
        assert store.users.get_user("ann") == replace(found, role="admin")
        with pytest.raises(KeyError):
            store.users.set_role("nobody", "admin")

    def test_users_are_listed_by_name_with_their_roles(self, store: Store):
        assert store.users.list_users() == []
        roles: list[tuple[str, Role]] = [("cy", "operator"), ("Bob", "operator"), ("ann", "admin")]
        for name, role in roles:
            store.users.create_user(name, f"hash-{name}", role)
        assert [(u.username, u.role) for u in store.users.list_users()] == [
            ("Bob", "operator"),
            ("ann", "admin"),
            ("cy", "operator"),
        ]

    def test_a_token_keeps_its_scope(self, store: Store):
        store.users.create_token("ci", "h1", None, "operator", None, "publish")
        store.users.create_token("ops", "h2", None, "admin", None)
        assert [store.users.find_token(h).scope for h in ("h1", "h2")] == ["publish", "all"]  # type: ignore[union-attr]

    # the built-in registry

    @staticmethod
    def _wheel(project: str, version: str, at: datetime = T0) -> RegistryFile:
        module = project.replace("-", "_")
        return RegistryFile(
            project,
            version,
            f"{module}-{version}-py3-none-any.whl",
            f"sha-{version}",
            3,
            {"v": version},
            "ci",
            at,
        )

    def test_registry_files_are_kept_once_and_read_back(self, store: Store, clock: FrozenClock):
        first = self._wheel("acme-invoices", "1.0.0")
        store.registry.put_file(first, b"one")
        store.registry.put_file(self._wheel("acme-invoices", "1.1.0", T0 + timedelta(minutes=1)), b"two")
        store.registry.put_file(self._wheel("Zed", "0.1"), b"zed")
        assert store.registry.projects() == ["Zed", "acme-invoices"]
        assert [f.version for f in store.registry.files("acme-invoices")] == ["1.0.0", "1.1.0"]
        assert store.registry.file(first.filename) == first and store.registry.file("nope.whl") is None
        assert store.registry.content(first.filename) == b"one" and store.registry.content("nope.whl") is None
        with pytest.raises(DuplicateFile):  # the same version, another file: a version is never replaced
            store.registry.put_file(replace(first, filename="acme_invoices-1.0.0-py3-none-other.whl"), b"x")
        with pytest.raises(DuplicateFile):
            store.registry.put_file(first, b"changed")
        with pytest.raises(DuplicateFile):  # 1.0 is 1.0.0 to every installer
            store.registry.put_file(
                replace(first, version="1.0", filename="acme_invoices-1.0-py3-none-any.whl"), b"x"
            )
        assert store.registry.content(first.filename) == b"one"

    def test_a_version_is_yanked_once(self, store: Store, clock: FrozenClock):
        store.registry.put_file(self._wheel("acme-invoices", "1.0.0"), b"one")
        clock.advance(timedelta(hours=1))
        assert store.registry.yank("acme-invoices", "1.0.0", "broken totals") is True
        clock.advance(timedelta(hours=1))
        assert store.registry.yank("acme-invoices", "1.0.0", "again") is True
        assert store.registry.yank("acme-invoices", "9.9.9", None) is False
        (found,) = store.registry.files("acme-invoices")
        assert (found.yanked_at, found.yanked_reason) == (T0 + timedelta(hours=1), "broken totals")
        assert store.registry.content(found.filename) == b"one"  # yanked, not deleted

    # installed packages

    @staticmethod
    def _installed(name: str, version: str, content: bytes = b"wheel", at: datetime = T0) -> PackageVersion:
        manifest = {"name": name, "version": version, "flows": [{"name": "invoices"}]}
        return PackageVersion(
            name,
            version,
            _sha(content),
            "feather",
            f"{name.replace('-', '_')}-{version}-py3-none-any.whl",
            len(content),
            manifest,
            ">=0.1,<1",
            "b" * 64,
            "ready",  # prepared: only a ready version's flows can be activated
            {"flows": ["invoices"], "seconds": 1.5},
            "root",
            at,
        )

    def test_an_installed_version_reads_back_with_its_wheel_and_inactive_flows(self, store: Store):
        first = self._installed("acme-invoices", "1.0.0")
        store.packages.install(first, b"wheel", ["acme-invoices/invoices", "acme-invoices/orders"], "go")
        later = self._installed("acme-invoices", "1.1.0", b"newer", T0 + timedelta(minutes=1))
        store.packages.install(later, b"newer", ["acme-invoices/invoices"])
        store.packages.install(self._installed("zed", "0.1", at=T0 + timedelta(minutes=2)), b"wheel", [])
        assert store.packages.packages() == ["acme-invoices", "zed"]
        assert store.packages.version("acme-invoices", "1.0.0") == first
        assert store.packages.version("acme-invoices", "1.0") == first  # by version_key
        assert store.packages.version("acme-invoices", "2.0.0") is None
        assert [v.version for v in store.packages.versions("acme-invoices")] == ["1.0.0", "1.1.0"]
        assert [v.name for v in store.packages.versions()] == ["acme-invoices", "acme-invoices", "zed"]
        assert (
            store.packages.wheel(_sha(b"wheel")) == b"wheel"
        )  # once by sha256, though zed has the same bytes
        assert store.packages.wheel(_sha(b"newer")) == b"newer" and store.packages.wheel("d" * 64) is None
        flows = store.packages.activations("acme-invoices", "1.0.0")
        assert [(a.flow, a.state, a.version, a.reason) for a in flows] == [
            ("acme-invoices/invoices", "inactive", "1.0.0", "go"),
            ("acme-invoices/orders", "inactive", "1.0.0", "go"),
        ]
        assert [(a.flow, a.version) for a in store.packages.activations("acme-invoices")] == [
            ("acme-invoices/invoices", "1.0.0"),
            ("acme-invoices/invoices", "1.1.0"),
            ("acme-invoices/orders", "1.0.0"),
        ]
        assert flows[0].changed_by == "root" and flows[0].changed_at == T0

    def test_a_version_installed_before_is_refused_and_changes_nothing(self, store: Store):
        first = self._installed("acme-invoices", "1.0.0")
        store.packages.install(first, b"wheel", ["acme-invoices/invoices"])
        for again, content in (
            (first, b"wheel"),
            (replace(first, version="1.0", sha256=_sha(b"other")), b"other"),
        ):  # 1.0 is 1.0.0 to every installer
            with pytest.raises(DuplicateVersion):
                store.packages.install(again, content, ["acme-invoices/other"])
        assert store.packages.version("acme-invoices", "1.0.0") == first
        assert store.packages.wheel(_sha(b"other")) is None
        assert [a.flow for a in store.packages.activations("acme-invoices")] == ["acme-invoices/invoices"]

    def test_a_wheel_that_isnt_the_versions_bytes_is_refused(self, store: Store):
        with pytest.raises(ValueError, match="sha256"):
            store.packages.install(self._installed("acme-invoices", "1.0.0"), b"tampered", [])
        assert store.packages.packages() == [] and store.packages.wheel(_sha(b"tampered")) is None

    def test_an_uninstalled_version_keeps_its_rows_and_loses_its_wheel_unless_shared(self, store: Store):
        one, two = self._installed("acme", "1.0.0"), self._installed("acme", "1.1.0", b"newer")
        same = self._installed("zed", "0.1")  # the same bytes as acme 1.0.0
        store.packages.install(one, b"wheel", ["acme/invoices"])
        store.packages.install(two, b"newer", ["acme/invoices"])
        store.packages.install(same, b"wheel", [])
        store.packages.activate("acme/invoices", "acme", "1.1.0", "ops", T0)
        with pytest.raises(InUse, match="activating"):
            store.packages.uninstall("acme", "1.1.0", "root", T0)
        store.packages.promote("acme", "1.1.0", T0)
        with pytest.raises(InUse, match="active"):
            store.packages.uninstall("acme", "1.1", "root", T0)
        assert store.packages.version("acme", "1.1.0") == two  # refused: nothing changed
        assert store.packages.uninstall("acme", "1.0", "root", T0, "old") is False  # zed has its bytes
        assert store.packages.wheel(_sha(b"wheel")) == b"wheel"
        gone = store.packages.version("acme", "1.0.0")
        assert gone is not None and gone.state == "uninstalled" and gone.sha256 == one.sha256
        assert [a.state for a in store.packages.activations("acme", "1.0.0")] == ["inactive"]
        with pytest.raises(KeyError):
            store.packages.uninstall("acme", "1.0.0", "root", T0)  # once
        with pytest.raises(KeyError):
            store.packages.uninstall("acme", "9.0", "root", T0)
        store.packages.deactivate("acme/invoices", "ops", T0)
        assert store.packages.uninstall("acme", "1.1.0", "root", T0) is True
        assert store.packages.wheel(_sha(b"newer")) is None
        assert store.packages.uninstall("zed", "0.1", "root", T0) is True
        assert store.packages.wheel(_sha(b"wheel")) is None

    def test_a_version_that_isnt_ready_or_is_uninstalled_cant_be_activated(self, store: Store):
        store.packages.install(
            replace(self._installed("acme", "1.0.0"), state="preparing"), b"wheel", ["acme/invoices"]
        )
        with pytest.raises(VersionNotReady) as early:
            store.packages.activate("acme/invoices", "acme", "1.0.0", "ops", T0)
        assert early.value.state == "preparing"
        store.packages.prepared("acme", "1.0.0", "ready", {"ok": True})
        store.packages.uninstall("acme", "1.0.0", "root", T0)
        with pytest.raises(
            VersionNotReady
        ) as late:  # the race the P5 review found: activate after an uninstall
            store.packages.activate("acme/invoices", "acme", "1.0.0", "ops", T0)
        assert late.value.state == "uninstalled" and store.packages.activating() == []

    def test_an_uninstall_is_refused_while_a_run_of_the_version_is_queued_or_running(self, store: Store):
        store.packages.install(self._installed("acme", "1.0.0"), b"wheel", ["acme/invoices"])
        run = store.runs.create_run(
            "acme/invoices", "cron", "scheduler", {"package": "acme", "version": "1.0.0"}
        )
        with pytest.raises(InUse, match="queued runs"):
            store.packages.uninstall("acme", "1.0.0", "root", T0)
        store.runs.set_run_status(run, "running")
        with pytest.raises(InUse, match="running runs"):
            store.packages.uninstall("acme", "1.0.0", "root", T0)
        store.runs.set_run_status(run, "succeeded")
        assert store.packages.uninstall("acme", "1.0.0", "root", T0) is True

    def test_the_lock_hashes_still_needed_are_all_there_past_500_versions(self, store: Store):
        for n in range(505):
            row = replace(self._installed("acme", f"1.0.{n}", f"w{n}".encode()), lock_hash=f"{n:064x}")
            store.packages.install(row, f"w{n}".encode(), [])
        store.packages.uninstall("acme", "1.0.504", "root", T0)
        needed = store.packages.needed_lock_hashes()
        assert len(needed) == 504 and f"{503:064x}" in needed and f"{504:064x}" not in needed

    def test_a_flow_remembers_when_each_version_last_served(self, store: Store):
        one, two = self._installed("acme", "1.0.0"), self._installed("acme", "1.1.0", b"newer")
        store.packages.install(one, b"wheel", ["acme/invoices"])
        store.packages.install(two, b"newer", ["acme/invoices"])
        store.packages.activate("acme/invoices", "acme", "1.0.0", "ops", T0)
        store.packages.promote("acme", "1.0.0", T0 + timedelta(seconds=1))
        store.packages.activate("acme/invoices", "acme", "1.1.0", "ops", T0 + timedelta(seconds=2))
        store.packages.promote("acme", "1.1.0", T0 + timedelta(seconds=3))
        served = {a.version: a.last_active_at for a in store.packages.activations("acme")}
        assert served == {"1.0.0": T0 + timedelta(seconds=3), "1.1.0": T0 + timedelta(seconds=3)}
        store.packages.deactivate("acme/invoices", "ops", T0 + timedelta(seconds=9))
        served = {a.version: a.last_active_at for a in store.packages.activations("acme")}
        assert served["1.1.0"] == T0 + timedelta(seconds=9) and served["1.0.0"] == T0 + timedelta(seconds=3)

    def test_a_flow_is_activated_in_one_version_at_a_time_once_its_host_is_up(self, store: Store):
        import threading

        heard = threading.Event()
        stop = store.packages.listen(heard.set)
        try:
            one, two = self._installed("acme", "1.0.0"), self._installed("acme", "1.1.0", b"newer")
            store.packages.install(one, b"wheel", ["acme/invoices", "acme/orders"])
            store.packages.install(two, b"newer", ["acme/invoices"])
            heard.wait(5)
            heard.clear()  # the listener's first call, on starting, says to read everything
            assert store.packages.active() == []
            asked = store.packages.activate("acme/invoices", "acme", "1.0", "ops", T0, "go live")
            assert heard.wait(10)  # a supervisor hears it, and starts a host
            assert (asked.state, store.packages.active()) == ("activating", [])
            assert [(a.flow, a.version) for a in store.packages.activating()] == [("acme/invoices", "1.0.0")]
            heard.clear()
            [first] = store.packages.promote("acme", "1.0", T0 + timedelta(seconds=1))  # its host is up
            assert heard.wait(10)
            assert (first.flow, first.version, first.state, first.reason) == (
                "acme/invoices",
                "1.0.0",
                "active",
                "go live",
            )
            assert store.packages.active() == [first] and store.packages.activating() == []
            assert store.packages.promote("acme", "1.0.0", T0) == []  # nothing activating: nothing changes
            again = store.packages.activate("acme/invoices", "acme", "1.0.0", "ops", T0)
            assert again.state == "active"  # already active: it stays so

            # A switch: the old version fires until the new one's host is up.
            store.packages.activate("acme/invoices", "acme", "1.1.0", "ops", T0 + timedelta(seconds=2))
            assert [(a.flow, a.version) for a in store.packages.active()] == [("acme/invoices", "1.0.0")]
            [failed] = store.packages.activation_failed("acme", "1.1.0", "it doesn't load", T0)
            assert (failed.state, failed.error) == ("failed", "it doesn't load")
            assert [(a.flow, a.version) for a in store.packages.active()] == [("acme/invoices", "1.0.0")]
            store.packages.activate("acme/invoices", "acme", "1.1.0", "ops", T0 + timedelta(seconds=3))
            store.packages.promote("acme", "1.1.0", T0 + timedelta(seconds=4))
            assert [(a.flow, a.version) for a in store.packages.active()] == [("acme/invoices", "1.1.0")]
            assert {
                (a.version, a.state) for a in store.packages.activations("acme") if a.flow == "acme/invoices"
            } == {
                ("1.0.0", "inactive"),
                ("1.1.0", "active"),
            }
            with pytest.raises(KeyError):
                store.packages.activate("acme/orders", "acme", "1.1.0", "ops", T0)

            # Deactivating takes the active version and one activating out of service.
            store.packages.activate("acme/invoices", "acme", "1.0.0", "ops", T0 + timedelta(seconds=5))
            off = store.packages.deactivate(
                "acme/invoices", "ops", T0 + timedelta(seconds=6), "paused partner"
            )
            assert (off.version, off.state) == ("1.1.0", "inactive")
            assert store.packages.active() == [] and store.packages.activating() == []
            with pytest.raises(KeyError):
                store.packages.deactivate("acme/invoices", "ops", T0)
        finally:
            stop()

    def test_hosts_are_recorded_per_worker_and_version(self, store: Store):
        store.packages.record_host("w-1", "acme", "1.0.0", "starting", 0, T0)
        store.packages.record_host(
            "w-1", "acme", "1.0.0", "crash_loop", 5, T0 + timedelta(seconds=1), "exit 1"
        )
        store.packages.record_host("w-0", "acme", "1.1.0", "ready", 0, T0)
        assert [(h.worker_id, h.version, h.state, h.restarts, h.error) for h in store.packages.hosts()] == [
            ("w-0", "1.1.0", "ready", 0, None),
            ("w-1", "1.0.0", "crash_loop", 5, "exit 1"),
        ]
        assert [h.version for h in store.packages.hosts("w-1")] == ["1.0.0"]
        assert store.packages.hosts("w-1")[0].changed_at == T0 + timedelta(seconds=1)

    def test_tagged_runs_are_found_however_many_newer_runs_there_are(self, store: Store):
        old = store.runs.create_run(
            "acme/feed", "every:60s", "scheduler", {"package": "acme", "version": "1.0.0"}
        )
        running = store.runs.create_run(
            "acme/feed", "every:60s", "scheduler", {"package": "acme", "version": "0.9"}
        )
        store.runs.set_run_status(running, "running")
        for _ in range(520):  # more than any page a supervisor or host would read
            store.runs.create_run(
                "acme/feed", "every:60s", "scheduler", {"package": "acme", "version": "1.1.0"}
            )
        store.runs.create_run("other", "manual", "ops", {})
        done = store.runs.create_run(
            "acme/feed", "every:60s", "scheduler", {"package": "acme", "version": "0.1"}
        )
        store.runs.set_run_status(done, "succeeded")
        assert store.runs.tagged_versions() == {("acme", "1.0.0"), ("acme", "0.9"), ("acme", "1.1.0")}
        assert store.runs.tagged_versions(("running",)) == {("acme", "0.9")}
        assert [r.run_id for r in store.runs.queued_of("acme", "1.0.0")] == [old]
        newer = store.runs.queued_of("acme", "1.1.0", limit=3)
        assert len(newer) == 3 and [r.run_id for r in newer] == sorted(
            r.run_id for r in newer
        )  # oldest first
        assert store.runs.queued_of("acme", "0.9") == []

    def test_a_version_is_prepared_once_and_environments_are_kept_per_worker(self, store: Store):
        import threading

        store.packages.install(
            replace(self._installed("acme", "1.0.0"), state="installed"), b"wheel", ["acme/invoices"]
        )
        heard = threading.Event()
        stop = store.packages.listen(heard.set)
        try:
            heard.wait(5)
            heard.clear()
            check = {"ok": True, "flows": ["invoices"], "specs": {"acme/invoices": {"name": "acme/invoices"}}}
            assert store.packages.prepared("acme", "1.0", "ready", check)
            assert heard.wait(10)  # a supervisor waiting for it hears that it is ready
            row = store.packages.version("acme", "1.0.0")
            assert row is not None and row.state == "ready" and row.load_check == check
            assert not store.packages.prepared(
                "acme", "1.0.0", "failed", {"ok": False}
            )  # another worker, later
            assert store.packages.version("acme", "1.0.0") == row
            assert not store.packages.prepared("zed", "1.0.0", "ready", {})
        finally:
            stop()
        store.packages.record_env("w-2", "b" * 64, "building", T0)
        store.packages.record_env("w-1", "b" * 64, "building", T0)
        store.packages.record_env("w-1", "b" * 64, "ready", T0 + timedelta(seconds=5))
        store.packages.record_env("w-1", "a" * 64, "failed", T0, "uv: no such version")
        assert [(e.worker_id, e.lock_hash[0], e.state, e.error) for e in store.packages.envs()] == [
            ("w-1", "a", "failed", "uv: no such version"),
            ("w-1", "b", "ready", None),
            ("w-2", "b", "building", None),
        ]
        assert [e.lock_hash[0] for e in store.packages.envs("w-2")] == ["b"]
        assert store.packages.envs("w-1")[1].built_at == T0 + timedelta(seconds=5)

    def test_environment_entries_are_kept_per_environment_and_return_what_they_replace(self, store: Store):
        from feather_connect.storage.port import EnvEntry

        zakya = EnvEntry(
            "prod", "connection", "zakya", {"connector": "zakya", "organization_id": "1"}, "root", T0
        )
        assert store.env_config.put(zakya) is None
        store.env_config.put(EnvEntry("prod", "budget", "zoho-api", {"limits": ["90/minute"]}, "root", T0))
        store.env_config.put(EnvEntry("qa", "connection", "zakya", {"connector": "zakya"}, "root", T0))
        changed = replace(zakya, value={"connector": "zakya", "organization_id": "2"}, changed_by="ops")
        assert store.env_config.put(changed) == zakya
        assert [(e.kind, e.name) for e in store.env_config.entries("prod")] == [
            ("connection", "zakya"),
            ("budget", "zoho-api"),
        ]
        assert store.env_config.entries("prod", "budget")[0].value == {"limits": ["90/minute"]}
        assert store.env_config.entry("prod", "connection", "zakya") == changed
        assert store.env_config.remove("prod", "connection", "zakya") == changed
        assert store.env_config.entry("prod", "connection", "zakya") is None
        assert store.env_config.remove("prod", "connection", "zakya") is None

    def test_secrets_are_kept_as_ciphertext_by_name(self, store: Store):
        from feather_connect.storage.port import SealedSecret

        sealed = SealedSecret("prod", "zakya-secret", b"\x01cipher", b"n" * 12, "k1", "root", T0)
        assert store.secrets.put(sealed) is False
        assert store.secrets.put(replace(sealed, ciphertext=b"\x02cipher", updated_by="ops")) is True
        store.secrets.put(replace(sealed, env="qa"))
        assert [(s.name, s.ciphertext, s.updated_by) for s in store.secrets.names("prod")] == [
            ("zakya-secret", b"\x02cipher", "ops")
        ]
        assert store.secrets.active_key("prod") == "k1" and store.secrets.active_key("dev") is None
        with pytest.raises(KeyMismatch):  # sealed with another key than the active one
            store.secrets.put(replace(sealed, key_id="k2"))
        count = store.secrets.rotate(
            "prod", "k1", "k2", lambda s: replace(s, ciphertext=b"rotated", key_id="k2")
        )
        got = store.secrets.get("prod", "zakya-secret")
        assert count == 1 and got is not None and (got.ciphertext, got.key_id) == (b"rotated", "k2")
        assert store.secrets.active_key("prod") == "k2" and store.secrets.active_key("qa") == "k1"
        with pytest.raises(KeyMismatch):  # rotated meanwhile: compare and swap
            store.secrets.rotate("prod", "k1", "k3", lambda s: s)
        with pytest.raises(KeyMismatch):
            store.secrets.put(sealed)  # k1 is not active in prod any more
        assert store.secrets.remove("prod", "zakya-secret") and not store.secrets.remove(
            "prod", "zakya-secret"
        )
        assert (
            store.secrets.get("prod", "zakya-secret") is None
            and store.secrets.get("qa", "zakya-secret") is not None
        )

    def test_a_rotation_seals_every_secret_however_many_or_none(self, store: Store):
        from feather_connect.storage.port import MAX_ENV_ROWS, SealedSecret

        for i in range(MAX_ENV_ROWS + 5):
            store.secrets.put(SealedSecret("prod", f"s{i:05}", b"c", b"n" * 12, "k1", "root", T0))

        def broken(secret: SealedSecret) -> SealedSecret:
            if secret.name == "s00500":
                raise ValueError("doesn't open")
            return replace(secret, key_id="k2")

        with pytest.raises(ValueError, match="doesn't open"):
            store.secrets.rotate("prod", "k1", "k2", broken)
        assert store.secrets.active_key("prod") == "k1"
        first = store.secrets.get("prod", "s00000")
        assert first is not None and first.key_id == "k1"  # nothing changed
        assert store.secrets.rotate("prod", "k1", "k2", lambda s: replace(s, key_id="k2")) == MAX_ENV_ROWS + 5
        last = store.secrets.get("prod", f"s{MAX_ENV_ROWS + 4:05}")
        assert last is not None and last.key_id == "k2"

    def test_a_webhook_event_is_kept_once_by_its_key(self, store: Store):
        from feather_connect.storage.port import WebhookEvent

        event = WebhookEvent("acme", "orders", "o-1", "2026-11-09", b'{"id": "o-1"}', {"x-sig": "ab"}, T0)
        assert store.webhooks.add(event) and not store.webhooks.add(replace(event, body=b"again"))
        store.webhooks.add(replace(event, key="o-2", received_at=T0 + timedelta(seconds=1)))
        assert [(e.key, e.body) for e in store.webhooks.events("acme", "orders")] == [
            ("o-1", b'{"id": "o-1"}'),
            ("o-2", b'{"id": "o-1"}'),
        ]
        assert store.webhooks.events("acme", "other") == []
        # Retention deletes only what a worker has processed, received before the cut.
        assert store.webhooks.processed("acme", "orders", ["o-1", "missing"], T0) == 1
        assert store.webhooks.purge(T0 + timedelta(days=1)) == 1
        assert [e.key for e in store.webhooks.events("acme", "orders")] == ["o-2"]
        store.webhooks.processed("acme", "orders", ["o-2"], T0)
        assert store.webhooks.purge(T0) == 0  # received after the cut

    def test_webhook_events_are_kept_many_at_once_and_only_the_new_reach_the_inbox(self, store: Store):
        from feather_connect.storage.port import WebhookEvent

        def event(key: str, body: bytes = b"{}") -> tuple[WebhookEvent, str, InboundRequest]:
            return WebhookEvent("acme", "orders", key, None, body, {}, T0), "acme/orders", inbound(body)

        store.webhooks.add(event("old")[0])
        kept = store.webhooks.keep_many(
            [event("o-1", b"1"), event("old"), event("o-2", b"2"), event("o-1", b"x")]
        )
        assert kept == [
            True,
            False,
            True,
            False,
        ]  # a repeat, of one kept before or earlier in these, is not new
        assert [e.request.body for e in store.inbox.take("acme/orders", 10)] == [b"1", b"2"]
        assert {e.key: e.processed_at for e in store.webhooks.events("acme", "orders")} == {
            "old": None,
            "o-1": T0,
            "o-2": T0,
        }
        assert store.webhooks.keep_many([]) == []

    def test_a_partition_is_closed_once_with_who_and_why(self, store: Store):
        first = store.readiness.close("acme/invoices", "store_id=1509", "ops", "store-day closed", T0)
        again = store.readiness.close(
            "acme/invoices", "store_id=1509", "other", "again", T0 + timedelta(hours=1)
        )
        assert again == first and (first.closed_by, first.reason, first.closed_at) == (
            "ops",
            "store-day closed",
            T0,
        )
        assert set(store.readiness.closes("acme/invoices", ["store_id=1509", "store_id=1"])) == {
            "store_id=1509"
        }
        assert store.readiness.closes("acme/other", ["store_id=1509"]) == {}

    def test_a_firing_happens_once_and_queues_its_run_with_it(self, store: Store):
        from feather_connect.storage.port import Firing

        firing = Firing(
            "acme/post",
            "store_day",
            "2026-11-08",
            "store_id=1509",
            0,
            "ready",
            {"a": {"p": "v"}},
            T0,
            "ready",
        )
        params = {"package": "acme", "version": "1.0.0", "ready": {"period": "2026-11-08"}}
        run_id = store.readiness.fire(firing, params)
        assert run_id is not None and store.readiness.fire(firing, params) is None
        run = store.runs.get_run(run_id)
        assert (run.flow, run.trigger, run.status, run.params["ready"]) == (
            "acme/post",
            "ready",
            "queued",
            params["ready"],
        )
        rerun = store.readiness.fire(
            replace(firing, seq=1, reason="late data", fired_at=T0 + timedelta(hours=1)), params
        )
        assert rerun is not None and rerun != run_id
        listed = store.readiness.firings("acme/post")
        assert [(f.seq, f.run_id, f.versions) for f in listed] == [
            (1, rerun, {"a": {"p": "v"}}),
            (0, run_id, {"a": {"p": "v"}}),
        ]
        assert (
            store.readiness.firings("acme/post", "other") == [] and store.readiness.firings("acme/else") == []
        )

    def test_registries_are_added_listed_and_removed(self, store: Store):
        store.registry.add_registry("zeta", "https://z.example.com/simple/", None, "root")
        added = store.registry.add_registry(
            "acme", "https://pkgs.example.com/simple/", "${secret:env:T}", "root"
        )
        assert (added.name, added.token_ref, added.added_by, added.added_at) == (
            "acme",
            "${secret:env:T}",
            "root",
            T0,
        )
        with pytest.raises(DuplicateRegistry):
            store.registry.add_registry("acme", "https://other.example.com/simple/", None, "root")
        assert [r.name for r in store.registry.registries()] == ["acme", "zeta"]
        assert store.registry.get_registry("acme") == added and store.registry.get_registry("nope") is None
        assert (
            store.registry.remove_registry("acme") is True and store.registry.remove_registry("acme") is False
        )
        assert [r.name for r in store.registry.registries()] == ["zeta"]

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

    def test_pending_older_than_lists_only_the_next_delivery_of_each_stream(
        self, store: Store, clock: FrozenClock
    ):
        for version_id in ("v1", "v2", "v3"):  # in one stream, on route a
            seal(store, version_id, ["a"])
        seal(store, "other", ["a"], ds="e")  # another dataset, so another stream
        seal(store, "blocked", ["b"], part="q")
        seal(store, "behind", ["b"], part="q")  # waits behind a failure, so nothing can be done for it
        store.ledger.transition("b", "blocked", "running", actor="t")
        store.ledger.transition("b", "blocked", "failed", actor="t")
        clock.advance(timedelta(minutes=2))
        found = store.ledger.pending_older_than(timedelta(minutes=1))
        assert [(d.route, d.version_id) for d in found] == [("a", "v1"), ("a", "other")]

        store.ledger.transition("a", "v1", "running", actor="t")  # a running one is next, and isn't pending
        assert [d.version_id for d in store.ledger.pending_older_than(timedelta(minutes=1))] == ["other"]
        assert store.ledger.pending_older_than(timedelta(minutes=1), limit=0) == []

    def test_pending_older_than_stops_at_its_limit(self, store: Store, clock: FrozenClock):
        for n in range(3):
            seal(store, f"v{n}", ["a"], part=f"p{n}")
        clock.advance(timedelta(minutes=2))
        found = store.ledger.pending_older_than(timedelta(minutes=1), limit=2)
        assert [d.version_id for d in found] == ["v0", "v1"]

    def test_a_run_that_ended_badly_has_no_seal_to_wait_for_and_its_staged_versions_are_abandoned(
        self, store: Store
    ):
        run = store.runs.create_run("f", "manual", "ann", {})
        store.versions.stage(draft("v1", run=run))
        store.versions.register_slices(run, "d", ["p"])
        store.versions.finish_slice(run, "d", "p")
        assert store.versions.runs_ready_to_seal() == [(run, "d")]
        assert store.versions.abandoned() == []
        for status in ("failed", "cancelled"):
            store.runs.set_run_status(run, status)
            assert store.versions.runs_ready_to_seal() == []
            assert [v.version_id for v in store.versions.abandoned()] == ["v1"]
        store.versions.seal(["v1"], ["a"])
        assert store.versions.abandoned() == []

    def test_stream_leftovers_are_staged_tick_versions_of_stream_runs(self, store: Store):  # S12
        stream = store.runs.stream_run("f", "2026-10-18")
        manual = store.runs.create_run("f", "manual", "ann", {})
        store.versions.stage(replace(draft("v1", run=stream), tick="t1"))
        store.versions.stage(draft("v2", run=stream))  # a push version: the sealer's, with no tick
        store.versions.stage(replace(draft("v3", run=manual), tick="t1"))
        store.versions.stage(replace(draft("v4", run=stream), tick="t2"))
        assert [v.version_id for v in store.versions.stream_leftovers()] == ["v1", "v4"]
        assert [v.version_id for v in store.versions.stream_leftovers(limit=1)] == ["v1"]
        store.versions.seal(["v1"], ["a"])
        assert [v.version_id for v in store.versions.stream_leftovers()] == ["v4"]

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

    # The inbox.

    def test_inbox_take_is_in_arrival_order(self, store: Store):
        for n in range(3):
            assert store.inbox.put("d", inbound(str(n).encode())) is True
        store.inbox.put("other", inbound(b"x"))
        events = store.inbox.take("d", 10)
        assert [e.request.body for e in events] == [b"0", b"1", b"2"]
        assert events[0].event_seq < events[1].event_seq < events[2].event_seq
        assert [e.request.body for e in store.inbox.take("d", 2)] == [b"0", b"1"]

    def test_inbox_take_stops_at_a_size(self, store: Store):  # S20
        for body in (b"a" * 40, b"b" * 40, b"c" * 40, b"d"):
            store.inbox.put("d", inbound(body))
        assert [len(e.request.body) for e in store.inbox.take("d", 10, max_bytes=50)] == [40, 40]
        assert [len(e.request.body) for e in store.inbox.take("d", 10, max_bytes=10)] == [
            40
        ]  # the first always
        assert len(store.inbox.take("d", 10, max_bytes=None)) == 4

    def test_inbox_keeps_a_request_exactly(self, store: Store):
        body = b'\x00\xff{"not": "text"}'
        store.inbox.put("d", inbound(body, {"x-signature": "abc"}), "e1")
        [event] = store.inbox.take("d", 1)
        assert (event.dataset, event.event_id, event.received_at) == ("d", "e1", T0)
        assert event.request == inbound(body, {"x-signature": "abc"})

    def test_inbox_put_many_stores_in_order_and_says_what_put_would(self, store: Store):
        store.inbox.put("d", inbound(b"old"), "seen")
        request = inbound(b"\x00raw", {"x-signature": "abc"})
        stored = store.inbox.put_many(
            [
                ("d", request, "e1"),
                ("d", inbound(b"seen again"), "seen"),
                ("d", inbound(b"no id"), None),
                ("d", inbound(b"repeat within"), "e1"),
                ("other", inbound(b"other dataset"), "e1"),
                ("d", inbound(b"last"), "e2"),
            ]
        )
        assert stored == [True, False, True, False, True, True]
        events = store.inbox.take("d", 10)
        assert [e.request.body for e in events] == [b"old", b"\x00raw", b"no id", b"last"]
        assert events[1].event_id == "e1" and events[1].request.headers == request.headers
        assert [e.request.body for e in store.inbox.take("other", 10)] == [b"other dataset"]

    def test_inbox_put_many_of_nothing_stores_nothing(self, store: Store):
        assert store.inbox.put_many([]) == []
        assert store.inbox.take("d", 10) == []

    def test_inbox_stores_an_event_id_once_per_dataset(self, store: Store):
        assert store.inbox.put("d", inbound(b"1"), "e1") is True
        assert store.inbox.put("d", inbound(b"2"), "e1") is False
        assert store.inbox.put("other", inbound(b"3"), "e1") is True
        assert store.inbox.put("d", inbound(b"4")) is True  # no id: nothing to compare
        assert store.inbox.put("d", inbound(b"5")) is True
        assert [e.request.body for e in store.inbox.take("d", 10)] == [b"1", b"4", b"5"]

    def test_an_event_id_stays_used_once_sealed(self, store: Store):
        store.inbox.put("d", inbound(), "e1")
        store.inbox.seal_batch([e.event_seq for e in store.inbox.take("d", 1)], [], [])
        assert store.inbox.put("d", inbound(), "e1") is False

    def test_inbox_oldest_age(self, store: Store, clock: FrozenClock):
        assert store.inbox.oldest_age("d", clock.now()) is None
        store.inbox.put("d", inbound())
        store.inbox.put("d", inbound(at=T0 + timedelta(seconds=5)))
        assert store.inbox.oldest_age("d", T0 + timedelta(seconds=7)) == timedelta(seconds=7)
        store.inbox.seal_batch([e.event_seq for e in store.inbox.take("d", 1)], [], [])
        assert store.inbox.oldest_age("d", T0 + timedelta(seconds=7)) == timedelta(seconds=2)

    def test_seal_batch_seals_events_and_versions_together(self, store: Store):
        store.inbox.put("d", inbound())
        seqs = [e.event_seq for e in store.inbox.take("d", 1)]
        [version] = store.inbox.seal_batch(seqs, [draft("v1", complete=False)], ["a"])
        assert version.status == "sealed" and version.seal_seq is not None
        assert store.ledger.get("a", "v1").status == "pending"
        assert store.inbox.take("d", 1) == []

    def test_seal_batch_is_one_transaction(self, store: Store):
        """Any refusal leaves the events open, the versions unknown and the ledger empty."""
        store.inbox.put("d", inbound(b"1"))
        store.inbox.put("d", inbound(b"2"))
        first, second = (e.event_seq for e in store.inbox.take("d", 2))
        store.inbox.seal_batch([first], [], [])
        with pytest.raises(StaleWriter):  # an event already sealed: another sealer got there
            store.inbox.seal_batch([first, second], [draft("v1", complete=False)], ["a"])
        with pytest.raises(StaleWriter):  # no such event
            store.inbox.seal_batch([second, second + 1000], [draft("v1", complete=False)], ["a"])
        store.versions.claim("v2", "w1", "r1", "d", "p")
        store.versions.claim("v3", "w1", "r1", "d", "p")
        store.versions.claim("v3", "later", "r1", "d", "p")
        drafts = [draft("v2", complete=False), draft("v3", complete=False)]
        with pytest.raises(StaleWriter):  # the first draft holds its claim, and the second has lost its own
            store.inbox.seal_batch([second], drafts, ["a"], writer="w1")
        assert [e.event_seq for e in store.inbox.take("d", 10)] == [second]
        for version_id in ("v1", "v2", "v3"):
            with pytest.raises(KeyError):
                store.versions.get(version_id)
        assert store.ledger.list_deliveries() == []

    def test_seal_batch_refuses_a_version_already_sealed(self, store: Store):
        seal(store, "v1", ["a"], complete=False)
        store.inbox.put("d", inbound())
        [event] = store.inbox.take("d", 1)
        with pytest.raises(VersionSealed):
            store.inbox.seal_batch([event.event_seq], [draft("v1", complete=False)], ["a"])
        assert store.inbox.take("d", 1) == [event]

    def test_a_sealed_event_cannot_seal_again_with_a_later_first_event(self, store: Store):
        """Two sealers can see different first events when a commit comes late: an event still seals only once."""
        store.inbox.put("d", inbound(b"1"))
        store.inbox.put("d", inbound(b"2"))
        first, second = (e.event_seq for e in store.inbox.take("d", 2))
        store.inbox.seal_batch([first, second], [draft("from-first", complete=False)], ["a"])
        with pytest.raises(StaleWriter):
            store.inbox.seal_batch([second], [draft("from-second", complete=False)], ["a"])
        with pytest.raises(KeyError):
            store.versions.get("from-second")

    def test_inbox_days_say_what_is_on_them(self, store: Store):
        store.inbox.put("d", inbound())
        store.inbox.put("d", inbound())
        [first, _] = store.inbox.take("d", 2)
        day = T0.date()
        assert day in store.inbox.days()
        assert store.inbox.day_status(day) == InboxDay(2, first.event_seq, None)
        store.inbox.seal_batch([first.event_seq], [], [])
        status = store.inbox.day_status(day)
        assert (status.unsealed, status.last_sealed) == (1, T0)

    def test_an_inbox_day_with_an_unsealed_event_is_not_dropped(self, store: Store):
        store.inbox.put("d", inbound())
        assert store.inbox.drop_day(T0.date()) is False
        assert len(store.inbox.take("d", 1)) == 1

    def test_dropping_an_inbox_day_frees_its_event_ids(self, store: Store):
        store.inbox.put("d", inbound(), "e1")
        store.inbox.seal_batch([e.event_seq for e in store.inbox.take("d", 1)], [], [])
        assert store.inbox.drop_day(T0.date()) is True
        assert T0.date() not in store.inbox.days()
        assert store.inbox.put("d", inbound(), "e1") is True

    def test_inbox_defer_counts_a_try_and_says_when(self, store: Store):
        store.inbox.put("d", inbound())
        [event] = store.inbox.take("d", 1)
        store.inbox.defer(event.event_seq, T0 + timedelta(seconds=10))
        store.inbox.defer(event.event_seq, T0 + timedelta(seconds=30))
        [again] = store.inbox.take("d", 1)
        assert (again.tries, again.retry_at) == (2, T0 + timedelta(seconds=30))

    def test_a_held_inbox_event_waits_for_reprocess_or_release(self, store: Store):
        for body in (b"1", b"2", b"3"):
            store.inbox.put("d", inbound(body))
        first, second, third = store.inbox.take("d", 3)
        store.inbox.defer(first.event_seq, T0)
        store.inbox.hold(first.event_seq)
        store.inbox.hold(second.event_seq)
        with pytest.raises(StaleWriter):
            store.inbox.hold(second.event_seq)
        with pytest.raises(StaleWriter):
            store.inbox.seal_batch([second.event_seq], [], [])
        assert store.inbox.take("d", 3) == [third]
        assert store.inbox.day_status(T0.date()).unsealed == 3
        assert store.inbox.reprocess("other", first.event_seq) is False
        assert store.inbox.reprocess("d", first.event_seq) is True
        [back, _] = store.inbox.take("d", 3)
        assert (back.event_seq, back.tries, back.retry_at) == (first.event_seq, 0, None)
        assert store.inbox.release("d", second.event_seq) is True
        assert store.inbox.release("d", second.event_seq) is False
        assert store.inbox.reprocess("d", second.event_seq) is False
        assert store.inbox.day_status(T0.date()).unsealed == 2

    def test_an_inbox_day_with_a_held_event_is_not_dropped(self, store: Store):
        store.inbox.put("d", inbound())
        [event] = store.inbox.take("d", 1)
        store.inbox.hold(event.event_seq)
        assert store.inbox.drop_day(T0.date()) is False
        store.inbox.release("d", event.event_seq)
        assert store.inbox.drop_day(T0.date()) is True

    def test_an_inbox_day_with_nothing_in_it_can_be_dropped(self, store: Store):
        store.inbox.ensure_partitions()
        assert store.inbox.drop_day(T0.date()) is True
        assert store.inbox.drop_day(T0.date()) is True


def flow_spec(name: str, routes: list[str], owner: str | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "owner": owner,
        "datasets": [{"name": f"{name}_ds"}],
        "routes": [{"name": route, "connection": route} for route in routes],
        "triggers": [],
    }


def _sha(content: bytes) -> str:
    import hashlib

    return hashlib.sha256(content).hexdigest()

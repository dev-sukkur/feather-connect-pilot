import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pyarrow as pa
import pytest
from pydantic import BaseModel

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.ids import stable_id
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import ExtractFailed, finalize_versions, run_extract
from feather_connect.storage import spool
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import StateRow, VersionDraft
from feather_connect.storage.spool import MemorySpool, table_to_records

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
T1 = T0 + timedelta(hours=1)
ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]

DEPS: RuntimeDeps


def make_deps() -> RuntimeDeps:
    clock = FrozenClock(T0)

    def make_context(connection: str) -> Context:
        return Context({}, logging.getLogger(connection), clock.now(), "UTC")

    return RuntimeDeps(MemoryStore(clock), MemorySpool(), clock, "sha", make_context)


@pytest.fixture(autouse=True)
def fresh_deps():
    global DEPS
    DEPS = make_deps()


def dataset(strategy, *, name="d", key="id", source=None, **kw) -> Any:
    return fc.Dataset(name, source=source or Source(), key=key, strategy=strategy, **kw)


def route_specs(names, mode: Any = "upsert"):
    return [fc.to(Sink(name), mode=mode) for name in names]


def extract(ds, rows, run="r1", partition="_", routes=("a",), **kw):
    ds.source.load(rows)
    return run_extract(ds, run, partition, routes=route_specs(routes), stage_only=False, deps=DEPS, **kw)


def latest_run():
    latest = DEPS.store.versions.latest_sealed("d", "_")
    return None if latest is None else latest.run_id


def recs(v):
    return table_to_records(DEPS.spool.get_chunk(v.version_id, 0))


def all_recs(v):
    chunks = DEPS.spool.list_chunks(v.version_id)
    return [r for c in chunks for r in table_to_records(DEPS.spool.get_chunk(v.version_id, c.chunk_no))]


def test_snapshot_diff_detects_insert_update_and_hard_delete():  # H2
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}])
    v = extract(ds, [{"id": 1, "v": "a"}, {"id": 2, "v": "B"}, {"id": 4, "v": "d"}], run="r2")[0]
    assert v.complete and (v.upserts, v.deletes) == (2, 1)
    assert {(r.key, r.op) for r in recs(v)} == {("2", "upsert"), ("4", "upsert"), ("3", "delete")}


def test_a_version_is_sealed_with_a_delivery_per_route_and_a_stable_id():
    v = extract(dataset(fc.strategy.replace()), ROWS, routes=("a", "b"))[0]
    assert v.status == "sealed" and v.code_sha == "sha" and v.content_hash and v.schema_hash
    assert DEPS.store.ledger.get("a", v.version_id).status == "pending"
    assert DEPS.store.ledger.get("b", v.version_id).status == "pending"
    assert v.version_id == stable_id("r1", "d", "_")


def test_unchanged_slice_seals_no_version():
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, ROWS)
    assert extract(ds, ROWS, run="r2") == []
    assert latest_run() == "r1"
    assert DEPS.spool.list_chunks(stable_id("r2", "d", "_"), "full") == []


def test_replace_delivers_the_whole_slice_and_implies_deletes():
    ds = dataset(fc.strategy.replace())
    extract(ds, [{"id": 1}, {"id": 2}])
    v = extract(ds, [{"id": 1}], run="r2")[0]
    assert {(r.key, r.op) for r in recs(v)} == {("1", "upsert"), ("2", "delete")}


def test_by_stamp_lookback_reread_is_suppressed():  # H4
    ds = dataset(fc.strategy.by_stamp("u", lookback="2h"))
    extract(ds, [{"id": 1, "u": T0}, {"id": 2, "u": T0}, {"id": 3, "u": T0}], partition="*")
    v = extract(
        ds, [{"id": 1, "u": T0}, {"id": 2, "u": T0}, {"id": 3, "u": T1, "x": 1}], run="r2", partition="*"
    )[0]
    assert [r.key for r in recs(v)] == ["3"] and v.complete is False


class RecordingSource(Source):
    def __init__(self):
        super().__init__()
        self.requests: list[ReadRequest] = []

    def read(self, ctx, req):
        self.requests.append(req)
        return super().read(ctx, req)


def test_by_stamp_reads_from_the_watermark_minus_lookback():
    source = RecordingSource()
    ds = dataset(fc.strategy.by_stamp("u", lookback="2h"), source=source)
    first = extract(ds, [{"id": 1, "u": T0}], partition="*")[0]
    second = extract(ds, [{"id": 1, "u": T1}], run="r2", partition="*")[0]
    assert source.requests[0].since is None
    assert source.requests[1].since == T0 - timedelta(hours=2)
    assert (source.requests[1].partition, source.requests[1].stamp_field) == ("*", "u")
    assert (first.watermark, second.watermark) == (T0, T1)


def test_by_stamp_without_lookback_reads_from_a_watermark_of_any_type():
    source = RecordingSource()
    ds = dataset(fc.strategy.by_stamp("u"), source=source)
    extract(ds, [{"id": 1, "u": 5}], partition="*")
    assert extract(ds, [{"id": 1, "u": 5}], run="r2", partition="*") == []
    assert source.requests[1].since == 5


def test_by_stamp_lookback_that_cannot_apply_to_the_watermark_fails():
    ds = dataset(fc.strategy.by_stamp("u", lookback="2h"))
    extract(ds, [{"id": 1, "u": 5}], partition="*")
    with pytest.raises(ExtractFailed, match="lookback"):
        extract(ds, [{"id": 1, "u": 5}], run="r2", partition="*")


def test_by_stamp_later_version_wins_per_key():
    ds = dataset(fc.strategy.by_stamp("u", lookback="2h"))
    extract(ds, [{"id": 1, "u": T0, "v": "a"}], partition="*")
    extract(ds, [{"id": 1, "u": T1, "v": "b"}], run="r2", partition="*")
    row = {"id": 1, "u": T0, "v": "a"}  # the same row as the first version's, so only the later one may count
    v = extract(ds, [row], run="r3", partition="*")[0]
    assert [r["v"] for r in recs(v)] == ["a"]
    assert extract(ds, [row], run="r4", partition="*") == []


def test_by_stamp_splits_rows_into_one_version_per_partition():
    ds = dataset(fc.strategy.by_stamp("u"), partition=fc.partition.by_day("date", tz="UTC"))
    vs = extract(
        ds,
        [{"id": 1, "date": "2026-10-18", "u": T0}, {"id": 2, "date": "2026-10-19", "u": T0}],
        partition="*",
    )
    assert sorted(v.partition for v in vs) == ["date=2026-10-18", "date=2026-10-19"]
    assert all(v.version_id == stable_id("r1", "d", v.partition) for v in vs)


def test_by_stamp_key_read_in_two_partitions_lands_only_in_the_last():
    ds = dataset(fc.strategy.by_stamp("u"), partition=fc.partition.by_day("date", tz="UTC"))
    vs = extract(
        ds,
        [{"id": 1, "date": "2026-10-18", "u": T0}, {"id": 1, "date": "2026-10-19", "u": T1}],
        partition="*",
    )
    assert [v.partition for v in vs] == ["date=2026-10-19"]


def test_by_stamp_deleted_field_emits_a_delete():
    ds = dataset(fc.strategy.by_stamp("u", deleted_field="gone"))
    v = extract(ds, [{"id": 1, "u": T0, "gone": True}], partition="*")[0]
    assert (recs(v)[0].key, recs(v)[0].op) == ("1", "delete")
    assert (v.upserts, v.deletes) == (0, 1)


def test_key_rewrite_keeps_one_current_row():  # H3
    ds = dataset(fc.strategy.snapshot_diff(), key="sku")
    extract(ds, [{"row_id": 10, "sku": "A", "p": 1}])
    v = extract(ds, [{"row_id": 99, "sku": "A", "p": 1}], run="r2")[0]
    assert [(r.key, r.op) for r in recs(v)] == [("A", "upsert")]


def test_duplicate_keys_in_one_read_keep_the_last():  # Review Focus 1
    v = extract(dataset(fc.strategy.replace()), [{"id": 1, "v": "old"}, {"id": 1, "v": "new"}])[0]
    assert [r["v"] for r in recs(v)] == ["new"]


def test_duplicate_keys_across_chunk_windows_keep_the_last(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 2)
    rows = [{"id": 1, "v": "old"}, {"id": 2, "v": "x"}, {"id": 1, "v": "new"}]
    v = extract(dataset(fc.strategy.replace()), rows)[0]
    assert sorted((r.key, r["v"]) for r in all_recs(v)) == [("1", "new"), ("2", "x")]
    assert v.upserts == 2


def test_missing_key_is_quarantined_and_the_rest_proceed():  # Review Focus 2
    v = extract(dataset(fc.strategy.replace()), [{"id": None}, {"id": 2}])[0]
    assert [r.key for r in recs(v)] == ["2"]
    row = DEPS.store.quarantine.list_open()[0]
    assert "id" in row.error and row.key == "<missing>" and row.route is None
    assert row.version_id == v.version_id


def test_record_with_no_partition_is_quarantined():
    ds = dataset(fc.strategy.by_stamp("u"), partition=fc.partition.by_day("date", tz="UTC"))
    v = extract(ds, [{"id": 1, "u": T0}, {"id": 2, "date": "2026-10-18", "u": T0}], partition="*")[0]
    assert [r.key for r in recs(v)] == ["2"]
    row = DEPS.store.quarantine.list_open()[0]
    assert (row.key, row.version_id) == ("1", None) and "date" in row.error


def test_record_whose_stamp_cannot_be_compared_is_quarantined():
    ds = dataset(fc.strategy.by_stamp("u"))
    v = extract(ds, [{"id": 1, "u": T0}, {"id": 2, "u": "soon"}], partition="*")[0]
    assert [r.key for r in recs(v)] == ["1"] and v.watermark == T0
    assert DEPS.store.quarantine.list_open()[0].key == "2"


class UnfilteredSource(Source):
    """Hands back everything it holds, as a source that filters on its own side would."""

    def read(self, ctx, req):
        yield self._rows


def test_a_bad_stamp_first_in_a_read_is_quarantined_not_sealed():
    ds = dataset(fc.strategy.by_stamp("u"))
    v = extract(ds, [{"id": 2, "u": "soon"}, {"id": 1, "u": T0}], partition="*")[0]
    assert [r.key for r in recs(v)] == ["1"] and v.watermark == T0
    assert [q.key for q in DEPS.store.quarantine.list_open()] == ["2"]


def test_a_bad_stamp_alone_in_a_later_run_cannot_block_the_dataset():
    ds = dataset(fc.strategy.by_stamp("u"), source=UnfilteredSource())
    extract(ds, [{"id": 1, "u": T0}], partition="*")
    assert extract(ds, [{"id": 2, "u": "soon"}], run="r2", partition="*") == []
    assert [q.key for q in DEPS.store.quarantine.list_open()] == ["2"]
    v = extract(ds, [{"id": 3, "u": T1}], run="r3", partition="*")[0]
    assert v.watermark == T1


def test_the_stored_watermark_sets_the_stamp_kind():
    ds = dataset(fc.strategy.by_stamp("u"), source=UnfilteredSource())
    extract(ds, [{"id": 1, "u": T0}], partition="*")
    for stamp in (5, date(2026, 10, 18), datetime(2026, 10, 18, 11), True, float("nan")):
        assert extract(ds, [{"id": 2, "u": stamp}], run=f"r-{stamp}", partition="*") == []
    assert DEPS.store.versions.latest_watermark("d") == T0


def test_the_first_acceptable_stamp_sets_the_kind_across_slices():
    ds = dataset(fc.strategy.by_stamp("u"), partition=fc.partition.by_fields("s"))
    rows = [{"id": 2, "s": "a", "u": 5}, {"id": 3, "s": "b", "u": T0}]
    v = extract(ds, rows, partition="*")
    assert [x.partition for x in v] == ["s=a"] and v[0].watermark == 5
    assert [q.key for q in DEPS.store.quarantine.list_open()] == ["3"]


def test_iso_text_stamps_with_a_z_work_from_one_read_to_the_next():
    source = RecordingSource()
    ds = dataset(fc.strategy.by_stamp("u", lookback="2h"), source=source)
    row = {"id": 1, "u": "2026-10-18T10:00:00Z"}
    v = extract(ds, [row], partition="*")[0]
    assert v.watermark == T0  # the parsed datetime
    assert recs(v)[0]["u"] == "2026-10-18T10:00:00Z"  # the payload stays as read
    assert extract(ds, [row], run="r2", partition="*") == []
    assert source.requests[1].since == T0 - timedelta(hours=2)
    assert DEPS.store.quarantine.list_open() == []


def test_iso_text_without_an_offset_is_read_as_utc():
    v = extract(dataset(fc.strategy.by_stamp("u")), [{"id": 1, "u": "2026-10-18T10:00:00"}], partition="*")[0]
    assert v.watermark == T0


@pytest.mark.parametrize(
    "rows",
    [
        [{"id": 1, "u": T0}, {"id": 2, "u": "2026-10-18T11:00:00+00:00"}],
        [{"id": 1, "u": "2026-10-18T10:00:00Z"}, {"id": 2, "u": T1}],
    ],
)
def test_datetimes_and_iso_text_mix_in_one_read(rows):
    v = extract(dataset(fc.strategy.by_stamp("u")), rows, partition="*")[0]
    assert v.watermark == T1 and v.upserts == 2
    assert DEPS.store.quarantine.list_open() == []


def test_a_non_date_string_is_quarantined_when_text_stamps_are_fine():
    ds = dataset(fc.strategy.by_stamp("u"))
    rows = [{"id": 1, "u": "not a date"}, {"id": 2, "u": "2026-10-18T10:00:00Z"}]
    v = extract(ds, rows, partition="*")[0]
    assert [r.key for r in recs(v)] == ["2"] and v.watermark == T0
    assert [q.key for q in DEPS.store.quarantine.list_open()] == ["1"]
    assert extract(ds, [{"id": 3, "u": "not a date"}], run="r2", partition="*") == []
    assert sorted(q.key for q in DEPS.store.quarantine.list_open()) == ["1", "3"]


def test_a_record_turned_down_for_other_reasons_does_not_set_the_stamp_kind():
    ds = dataset(fc.strategy.by_stamp("u"))
    v = extract(ds, [{"id": None, "u": 5}, {"id": 1, "u": T0}], partition="*")[0]
    assert v.watermark == T0


def test_numbers_of_different_types_are_one_stamp_kind():
    ds = dataset(fc.strategy.by_stamp("u"))
    assert extract(ds, [{"id": 1, "u": 5}, {"id": 2, "u": 6.5}], partition="*")[0].watermark == 6.5


def test_a_reserved_column_is_quarantined_and_the_rest_proceed():
    v = extract(dataset(fc.strategy.replace()), [{"id": 1, "_fc_x": 1}, {"id": 2}])[0]
    assert [r.key for r in recs(v)] == ["2"]
    row = DEPS.store.quarantine.list_open()[0]
    assert row.key == "1" and "_fc_x" in row.error


def test_an_infrastructure_error_is_not_taken_for_a_bad_record():
    class BrokenSpool(MemorySpool):
        def delete_version(self, version_id):
            raise ValueError("the spool is down")

    deps = RuntimeDeps(DEPS.store, BrokenSpool(), DEPS.clock, "sha", DEPS.make_context)
    ds = dataset(fc.strategy.by_stamp("u"))
    ds.source.load([{"id": 1, "u": T0}])
    with pytest.raises(ValueError, match="the spool is down"):
        run_extract(ds, "r1", "*", routes=route_specs(("a",)), stage_only=False, deps=deps)
    assert DEPS.store.quarantine.list_open() == []


def test_record_that_cannot_be_hashed_is_quarantined():
    v = extract(dataset(fc.strategy.replace()), [{"id": 1, "v": object()}, {"id": 2}])[0]
    assert [r.key for r in recs(v)] == ["2"]
    assert DEPS.store.quarantine.list_open()[0].key == "1"


def test_empty_read_cannot_wipe_a_slice_unless_allowed():  # Review Focus 3
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, [{"id": 1}])
    with pytest.raises(ExtractFailed, match="allow_empty"):
        extract(ds, [], run="r2")
    assert latest_run() == "r1"
    ds2 = dataset(fc.strategy.snapshot_diff(), name="d2", allow_empty=True)
    extract(ds2, [{"id": 1}])
    assert extract(ds2, [], run="r2")[0].deletes == 1


def test_a_read_whose_rows_are_all_rejected_counts_as_empty():
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, [{"id": 1}])
    with pytest.raises(ExtractFailed, match="allow_empty"):
        extract(ds, [{"id": None}], run="r2")


def test_the_empty_read_guard_waits_for_finalize():
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, [{"id": 1}])
    ds.source.load([])
    routes = route_specs(("a",))
    staged = run_extract(ds, "r2", "_", routes=routes, stage_only=True, deps=DEPS)
    assert staged[0].status == "staged"
    assert DEPS.spool.list_chunks(staged[0].version_id, "candidates")[0].rows == 1
    with pytest.raises(ExtractFailed, match="allow_empty"):
        finalize_versions(ds, staged, routes, DEPS)


def test_first_empty_read_is_fine():
    assert extract(dataset(fc.strategy.snapshot_diff()), []) == []
    assert DEPS.store.versions.list_versions("d") == []


def test_validation_contract_quarantines_invalid_records():  # FR-C3
    class M(BaseModel):
        id: int
        price: float

    v = extract(dataset(fc.strategy.replace(), schema=M), [{"id": 1, "price": "x"}, {"id": 2, "price": 3.0}])[
        0
    ]
    assert [r.key for r in recs(v)] == ["2"]
    assert DEPS.store.quarantine.list_open()[0].key == "1"


def test_arrow_schema_is_a_validation_contract_too():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("price", pa.float64())])
    rows = [{"id": 1, "price": "x"}, {"id": 2, "price": 3.0}]
    v = extract(dataset(fc.strategy.replace(), schema=schema), rows)[0]
    assert [r.key for r in recs(v)] == ["2"]
    bad = extract(
        dataset(fc.strategy.replace(), name="d2", key="k", schema=schema), [{"k": 1}, {"k": 2, "id": 1}]
    )
    assert [r.key for r in recs(bad[0])] == ["2"]


def test_a_rejected_record_is_not_taken_for_a_delete():
    class M(BaseModel):
        id: int
        price: float

    ds = dataset(fc.strategy.snapshot_diff(), schema=M)
    extract(ds, [{"id": 1, "price": 1.0}, {"id": 2, "price": 2.0}])
    v = extract(ds, [{"id": 1, "price": "x"}, {"id": 2, "price": 9.0}], run="r2")[0]
    assert [(r.key, r.op) for r in recs(v)] == [("2", "upsert")] and v.deletes == 0
    # The key is still tracked, so it goes when the source really drops it.
    last = extract(ds, [{"id": 2, "price": 9.0}], run="r3")[0]
    assert [(r.key, r.op) for r in recs(last)] == [("1", "delete")]


def test_on_record_error_fail_fails_the_version():
    class M(BaseModel):
        id: int

    ds = dataset(fc.strategy.replace(), schema=M, on_record_error="fail")
    with pytest.raises(ExtractFailed, match="id"):
        extract(ds, [{"id": "x"}, {"id": 2}])
    assert DEPS.store.versions.latest_sealed("d", "_") is None


def test_restart_rewrites_chunks_and_drops_leftovers(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 1)
    ds = dataset(fc.strategy.replace())
    routes = route_specs(("a",))
    ds.source.load([{"id": 1}, {"id": 2}, {"id": 3}])
    vid = run_extract(ds, "r1", "_", routes=routes, stage_only=True, deps=DEPS)[0].version_id
    assert len(DEPS.spool.list_chunks(vid)) == 3
    ds.source.load([{"id": 1}])
    version = run_extract(ds, "r1", "_", routes=routes, stage_only=True, deps=DEPS)[0]
    assert version.version_id == vid and version.upserts == 1
    assert len(DEPS.spool.list_chunks(vid)) == 1


def test_restart_after_sealing_leaves_the_version_alone():  # I1
    ds = dataset(fc.strategy.replace())
    v = extract(ds, ROWS)[0]
    again = extract(ds, [{"id": 9}])[0]
    assert (again.version_id, again.seal_seq) == (v.version_id, v.seal_seq)
    assert [r.key for r in recs(v)] == ["1", "2"]


def test_restart_after_many_later_versions_leaves_the_version_alone():  # I1
    ds = dataset(fc.strategy.replace())
    first = extract(ds, [{"id": 1}], run="r0")[0]
    for i in range(1, 102):
        extract(ds, [{"id": 1}], run=f"r{i}")
    again = extract(ds, [{"id": 9}], run="r0")[0]
    assert (again.version_id, again.seal_seq) == (first.version_id, first.seal_seq)
    assert [r.key for r in recs(first)] == ["1"]


def test_restart_of_a_by_stamp_run_returns_all_it_sealed_however_many():
    ds = dataset(fc.strategy.by_stamp("u"), partition=fc.partition.by_fields("s"))
    rows = [{"id": i, "s": str(i), "u": T0} for i in range(101)]
    sealed = extract(ds, rows, partition="*")
    assert len(sealed) == 101
    assert extract(ds, rows, partition="*") == sealed


def test_a_no_op_restart_leaves_no_staged_version():
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, ROWS)
    routes = route_specs(("a",))
    ds.source.load(ROWS[:1])
    staged = run_extract(ds, "r2", "_", routes=routes, stage_only=True, deps=DEPS)[0]
    ds.source.load(ROWS)
    assert run_extract(ds, "r2", "_", routes=routes, stage_only=True, deps=DEPS) == []
    assert DEPS.store.versions.staged_for_run("r2", "d") == []
    assert DEPS.spool.list_chunks(staged.version_id, "candidates") == []


def test_a_by_stamp_restart_drops_a_partition_it_no_longer_touches():
    ds = dataset(fc.strategy.by_stamp("u"), partition=fc.partition.by_fields("s"))
    routes = route_specs(("a",))
    ds.source.load([{"id": 1, "s": "a", "u": T0}, {"id": 2, "s": "b", "u": T0}])
    run_extract(ds, "r1", "*", routes=routes, stage_only=True, deps=DEPS)
    ds.source.load([{"id": 1, "s": "a", "u": T0}])
    run_extract(ds, "r1", "*", routes=routes, stage_only=True, deps=DEPS)
    assert [v.partition for v in DEPS.store.versions.staged_for_run("r1", "d")] == ["s=a"]


def test_a_sealed_version_without_a_snapshot_does_not_hide_an_older_one():
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, ROWS)
    draft = VersionDraft("x", "d", "_", "rx", True, 0, 0, "", None, None, "sha")
    DEPS.store.versions.stage(draft)
    DEPS.store.versions.seal(["x"], ["a"])
    v = extract(ds, ROWS[:1], run="r3")[0]
    assert (v.upserts, v.deletes) == (0, 1)
    assert [(r.key, r.op) for r in recs(v)] == [("2", "delete")]


def test_restart_of_a_by_stamp_run_after_sealing_returns_what_it_sealed():
    ds = dataset(fc.strategy.by_stamp("u"))
    v = extract(ds, [{"id": 1, "u": T0}], partition="*")[0]
    assert extract(ds, [{"id": 1, "u": T0}, {"id": 2, "u": T1}], partition="*") == [v]


def test_finalize_is_safe_to_repeat():
    ds = dataset(fc.strategy.snapshot_diff(), keep_state=True)
    extract(ds, ROWS)
    ds.source.load([{"id": 1, "v": "a"}])
    routes = route_specs(("a",))
    staged = run_extract(ds, "r2", "_", routes=routes, stage_only=True, deps=DEPS)
    sealed = finalize_versions(ds, staged, routes, DEPS)
    assert finalize_versions(ds, staged, routes, DEPS) == sealed
    assert [(r.key, r.op) for r in all_recs(sealed[0])] == [("2", "delete")]


def test_large_read_stays_chunked(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 2)
    v = extract(dataset(fc.strategy.replace()), [{"id": i} for i in range(5)])[0]
    chunks = DEPS.spool.list_chunks(v.version_id)
    assert [c.rows for c in chunks] == [2, 2, 1]
    assert [r.key for r in all_recs(v)] == ["0", "1", "2", "3", "4"]


def test_deletes_go_at_the_end_of_the_last_chunk(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 2)
    ds = dataset(fc.strategy.replace())
    extract(ds, [{"id": i} for i in range(4)])
    v = extract(ds, [{"id": 0}, {"id": 1}, {"id": 2}], run="r2")[0]
    assert [(r.key, r.op) for r in all_recs(v)] == [
        ("0", "upsert"),
        ("1", "upsert"),
        ("2", "upsert"),
        ("3", "delete"),
    ]
    assert not DEPS.spool.list_chunks(v.version_id, "candidates")


def test_full_chunks_are_kept_only_for_a_replace_partition_route():
    ds = dataset(fc.strategy.snapshot_diff())
    plain = extract(ds, ROWS)[0]
    assert DEPS.spool.list_chunks(plain.version_id, "full") == []
    ds.source.load([{"id": 1, "v": "a"}, {"id": 2, "v": "B"}])
    routes = route_specs(("a",), mode="replace_partition")
    kept = run_extract(ds, "r2", "_", routes=routes, stage_only=False, deps=DEPS)[0]
    assert [r.key for r in recs(kept)] == ["2"]
    full = table_to_records(DEPS.spool.get_chunk(kept.version_id, 0, "full"))
    assert [r.key for r in full] == ["1", "2"]


def test_only_complete_versions_keep_a_snapshot():
    ds = dataset(fc.strategy.by_stamp("u"))
    v = extract(ds, [{"id": 1, "u": T0}], partition="*")[0]
    assert DEPS.spool.list_chunks(v.version_id, "snapshot") == []
    ds2 = dataset(fc.strategy.replace(), name="d2")
    v2 = extract(ds2, ROWS)[0]
    assert DEPS.spool.get_chunk(v2.version_id, 0, "snapshot").num_rows == 2


def test_keep_state_updates_record_state_when_sealed():
    ds = dataset(fc.strategy.snapshot_diff(), keep_state=True)
    v = extract(ds, ROWS)[0]
    row = DEPS.store.state.get_many("d", ["1"])["1"]
    assert (row.partition, row.last_version, row.payload["v"]) == ("_", v.version_id, "a")
    extract(ds, [{"id": 2, "v": "b"}], run="r2")
    assert list(DEPS.store.state.get_many("d", ["1", "2"])) == ["2"]


def test_without_keep_state_no_record_state_is_written():
    extract(dataset(fc.strategy.replace()), ROWS)
    assert DEPS.store.state.get_many("d", ["1", "2"]) == {}


def test_by_stamp_with_keep_state_compares_with_record_state():
    ds = dataset(fc.strategy.by_stamp("u"), keep_state=True)
    extract(ds, [{"id": 1, "u": T0}], partition="*")
    assert extract(ds, [{"id": 1, "u": T0}], run="r2", partition="*") == []
    DEPS.store.state.put_many("d", [StateRow("1", "_", 0, {}, "x")])
    v = extract(ds, [{"id": 1, "u": T0}], run="r3", partition="*")[0]
    assert [r.key for r in recs(v)] == ["1"]


def test_record_batches_are_read_too():
    class ArrowSource(Source):
        def read(self, ctx, req):  # pyright: ignore[reportIncompatibleMethodOverride]
            yield pa.RecordBatch.from_pylist([{"id": 1}, {"id": 2}])

    v = extract(dataset(fc.strategy.replace(), source=ArrowSource()), [])[0]
    assert [r.key for r in recs(v)] == ["1", "2"]


def test_extract_never_calls_a_sink():  # I5
    sink = Sink("a")
    ds = dataset(fc.strategy.replace())
    ds.source.load(ROWS)
    run_extract(ds, "r1", "_", routes=[fc.to(sink)], stage_only=False, deps=DEPS)
    assert sink.calls == []


class _StrictInSlice(BaseModel):
    id: int
    n: int
    s: str


def _moved_and_turned_down(second_rows):
    ds = dataset(
        fc.strategy.snapshot_diff(),
        schema=_StrictInSlice,
        keep_state=True,
        partition=fc.partition.by_fields("s"),
    )
    routes = route_specs(("a",))
    ds.source.load([{"id": 1, "s": "a", "n": 1}])
    v1 = run_extract(ds, "r1", "s=a", routes=routes, stage_only=False, deps=DEPS)[0]
    ds.source.load(second_rows)
    staged = [
        version
        for partition in ("s=a", "s=b")
        for version in run_extract(ds, "r2", partition, routes=routes, stage_only=True, deps=DEPS)
    ]
    return ds, routes, v1, staged


def _snapshot_of(version_id) -> dict[Any, Any]:
    table = DEPS.spool.get_chunk(version_id, 0, "snapshot")
    return dict(
        zip(table.column("_fc_key").to_pylist(), table.column("_fc_row_hash").to_pylist(), strict=True)
    )


def test_a_key_that_moved_and_was_turned_down_stays_in_its_old_slice_snapshot():
    ds, routes, v1, staged = _moved_and_turned_down([{"id": 1, "s": "b", "n": "bad"}])
    sealed = {v.partition: v for v in finalize_versions(ds, staged, routes, DEPS)}
    old_hash = _snapshot_of(v1.version_id)["1"]
    assert _snapshot_of(sealed["s=a"].version_id) == {"1": old_hash}
    assert sealed["s=a"].deletes == 0
    assert _snapshot_of(sealed["s=b"].version_id) == {"1": 0}


def test_repeating_a_finalize_keeps_the_same_decision_for_a_turned_down_move():
    ds, routes, v1, staged = _moved_and_turned_down([{"id": 1, "s": "b", "n": "bad"}])
    a = next(v for v in staged if v.partition == "s=a")
    # A first attempt that got as far as writing its decision, then died before sealing.
    DEPS.spool.put_chunk(
        a.version_id, 0, "snapshot", spool.snapshot_table(_snapshot_of(v1.version_id).items())
    )
    sealed = {v.partition: v for v in finalize_versions(ds, staged, routes, DEPS)}
    assert DEPS.spool.list_chunks(sealed["s=a"].version_id, "moves") == []
    assert sealed["s=a"].deletes == 0 and list(_snapshot_of(sealed["s=a"].version_id)) == ["1"]

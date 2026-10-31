import logging
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.context import Context
from feather_connect.ids import stable_id
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import ExtractFailed, run_extract
from feather_connect.runtime.seal import run_seal
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool, table_to_records

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
DAY_18, DAY_19, DAY_20 = "date=2026-10-18", "date=2026-10-19", "date=2026-10-20"
ROUTES = [fc.to(Sink("a"))]

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


def day_dataset(**kw) -> Any:
    return fc.Dataset(
        "d",
        source=Source(),
        key="id",
        strategy=fc.strategy.snapshot_diff(),
        partition=fc.partition.by_day("date", tz="UTC"),
        **kw,
    )


def on(day: str, *ids: int, **fields) -> list[dict[str, Any]]:
    return [{"id": i, "date": day.removeprefix("date="), **fields} for i in ids]


def stage_slices(ds, run, slices):  # slices: {partition: rows}
    ds.source.load([r for rows in slices.values() for r in rows])
    DEPS.store.versions.register_slices(run, ds.name, list(slices))
    staged = []
    for p in slices:
        staged += run_extract(ds, run, p, routes=ROUTES, stage_only=True, deps=DEPS)
    return staged


def run_slices(ds, run, slices):
    stage_slices(ds, run, slices)
    return run_seal(ds, run, routes=ROUTES, deps=DEPS)


def all_records(v):
    chunks = DEPS.spool.list_chunks(v.version_id)
    return [r for c in chunks for r in table_to_records(DEPS.spool.get_chunk(v.version_id, c.chunk_no))]


def moves_of(v) -> dict[Any, Any]:
    if not DEPS.spool.list_chunks(v.version_id, "moves"):
        return {}
    table = DEPS.spool.get_chunk(v.version_id, 0, "moves")
    assert table.column_names == ["_fc_key", "_fc_moved_from"]
    return dict(
        zip(table.column("_fc_key").to_pylist(), table.column("_fc_moved_from").to_pylist(), strict=True)
    )


def version_for(vs, partition):
    return next(v for v in vs if v.partition == partition)


def test_move_between_slices_of_one_run_is_not_a_delete():  # H1b, I10
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1), DAY_19: []})
    vs = run_slices(ds, "r2", {DAY_18: [], DAY_19: on(DAY_19, 1)})
    ops = [(r.key, r.op) for v in vs for r in all_records(v)]
    assert ("1", "delete") not in ops and ("1", "upsert") in ops
    assert moves_of(version_for(vs, DAY_19)) == {"1": DAY_18}


def test_key_absent_from_every_slice_is_a_delete():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    vs = run_slices(ds, "r2", {DAY_18: on(DAY_18, 1)})
    assert ("2", "delete") in [(r.key, r.op) for r in all_records(vs[0])]
    assert moves_of(vs[0]) == {}


def test_nothing_is_sealed_until_seal_run():
    ds = day_dataset()
    ds.source.load(on(DAY_18, 1))
    DEPS.store.versions.register_slices("r1", "d", [DAY_18])
    v = run_extract(ds, "r1", DAY_18, routes=ROUTES, stage_only=True, deps=DEPS)[0]
    assert v.status == "staged" and DEPS.store.versions.latest_sealed("d", DAY_18) is None


def test_seal_run_seals_all_versions_together_and_is_idempotent():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    first = run_slices(ds, "r2", {DAY_18: on(DAY_18, 2), DAY_19: on(DAY_19, 1)})
    assert [v.status for v in first] == ["sealed", "sealed"]
    assert sorted(v.seal_seq or 0 for v in first) == [2, 3]
    assert DEPS.store.versions.staged_for_run("r2", "d") == []
    records = [all_records(v) for v in first]
    assert run_seal(ds, "r2", routes=ROUTES, deps=DEPS) == first
    assert [all_records(v) for v in first] == records
    assert len(DEPS.store.ledger.list_deliveries("a")) == 3


def test_a_run_with_nothing_staged_seals_nothing():
    assert run_seal(day_dataset(), "r1", routes=ROUTES, deps=DEPS) == []


def test_a_dropped_candidate_corrects_the_versions_counts_and_hash():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    staged = version_for(stage_slices(ds, "r2", {DAY_18: on(DAY_18, 2), DAY_19: on(DAY_19, 1)}), DAY_18)
    assert staged.deletes == 1
    sealed = version_for(run_seal(ds, "r2", routes=ROUTES, deps=DEPS), DAY_18)
    assert (sealed.upserts, sealed.deletes) == (0, 0)
    assert sealed.content_hash != staged.content_hash
    assert all_records(sealed) == []


def test_a_kept_candidate_leaves_the_counts_and_hash_as_staged():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    staged = stage_slices(ds, "r2", {DAY_18: on(DAY_18, 1, x=1)})[0]
    sealed = run_seal(ds, "r2", routes=ROUTES, deps=DEPS)[0]
    assert (sealed.upserts, sealed.deletes, sealed.content_hash) == (1, 1, staged.content_hash)
    assert sorted((r.key, r.op) for r in all_records(sealed)) == [("1", "upsert"), ("2", "delete")]


def test_a_complete_version_with_no_records_still_seals():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1)})
    vs = run_slices(ds, "r2", {DAY_18: [], DAY_19: on(DAY_19, 1)})
    emptied = version_for(vs, DAY_18)
    assert emptied.status == "sealed" and all_records(emptied) == [] and moves_of(emptied) == {}
    assert DEPS.store.ledger.get("a", emptied.version_id).status == "pending"
    # Its empty snapshot is the slice's new truth, so a later read of the old slice has nothing to delete.
    assert run_slices(ds, "r3", {DAY_18: [], DAY_19: on(DAY_19, 1)}) == []


def test_a_slice_emptied_only_by_moves_passes_the_empty_read_guard():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    vs = run_slices(ds, "r2", {DAY_18: [], DAY_19: on(DAY_19, 1), DAY_20: on(DAY_20, 2)})
    assert version_for(vs, DAY_18).deletes == 0
    assert moves_of(version_for(vs, DAY_19)) == {"1": DAY_18}
    assert moves_of(version_for(vs, DAY_20)) == {"2": DAY_18}


def test_an_empty_slice_with_a_real_delete_trips_the_guard_and_seals_nothing():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    staged = stage_slices(ds, "r2", {DAY_18: [], DAY_19: on(DAY_19, 1)})
    with pytest.raises(ExtractFailed, match="allow_empty"):
        run_seal(ds, "r2", routes=ROUTES, deps=DEPS)
    assert DEPS.store.versions.sealed_for_run("r2", "d") == []
    assert all_records(version_for(staged, DAY_18)) == []
    assert DEPS.spool.list_chunks(version_for(staged, DAY_18).version_id, "candidates")[0].rows == 2


def test_allow_empty_lets_the_real_delete_through_and_keeps_the_move():
    ds = day_dataset(allow_empty=True)
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    vs = run_slices(ds, "r2", {DAY_18: [], DAY_19: on(DAY_19, 1)})
    assert [(r.key, r.op) for r in all_records(version_for(vs, DAY_18))] == [("2", "delete")]
    assert moves_of(version_for(vs, DAY_19)) == {"1": DAY_18}


def test_a_key_in_two_old_slices_moves_once_and_leaves_no_delete():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1), DAY_19: on(DAY_19, 2)})
    run_slices(ds, "r2", {DAY_19: on(DAY_19, 1, 2)})  # a read of day 18 is yet to see that key 1 went
    vs = run_slices(ds, "r3", {DAY_18: [], DAY_19: on(DAY_19, 2), DAY_20: on(DAY_20, 1)})
    assert not [r for v in vs for r in all_records(v) if r.op == "delete"]
    assert moves_of(version_for(vs, DAY_20)) == {"1": DAY_18}


def test_a_key_held_by_an_unchanged_slice_of_the_run_is_not_a_delete():  # I10
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    run_slices(ds, "r2", {DAY_19: on(DAY_19, 1)})
    vs = run_slices(ds, "r3", {DAY_18: on(DAY_18, 2), DAY_19: on(DAY_19, 1)})
    assert [v.partition for v in vs] == [
        DAY_18
    ]  # day 19 read as unchanged, so nothing can take a move notice
    assert (all_records(vs[0]), vs[0].deletes, moves_of(vs[0])) == ([], 0, {})


def test_a_slice_emptied_by_a_key_an_unchanged_slice_holds_passes_the_guard():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1)})
    run_slices(ds, "r2", {DAY_19: on(DAY_19, 1)})
    vs = run_slices(ds, "r3", {DAY_18: [], DAY_19: on(DAY_19, 1)})
    assert [(v.partition, v.deletes) for v in vs] == [(DAY_18, 0)]


def test_a_key_unchanged_slice_does_not_hold_is_still_a_delete():
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2), DAY_19: on(DAY_19, 3)})
    vs = run_slices(ds, "r2", {DAY_18: on(DAY_18, 2), DAY_19: on(DAY_19, 3)})
    assert [(r.key, r.op) for r in all_records(vs[0])] == [("1", "delete")]


def test_a_record_back_in_an_unchanged_slice_is_not_a_delete_with_keep_state():
    ds = day_dataset(keep_state=True)
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1)})
    run_slices(ds, "r2", {DAY_19: on(DAY_19, 1, 2)})
    vs = run_slices(ds, "r3", {DAY_18: on(DAY_18, 1), DAY_19: on(DAY_19, 2)})
    assert ("1", "delete") not in [(r.key, r.op) for v in vs for r in all_records(v)]
    assert "1" in DEPS.store.state.get_many("d", ["1"])


def strict_dataset() -> Any:
    fields = [pa.field("id", pa.int64()), pa.field("date", pa.string()), pa.field("x", pa.int64())]
    return day_dataset(schema=pa.schema(fields))


def rejected_chunks(run, partition):
    return DEPS.spool.list_chunks(stable_id(run, "d", partition), "rejected")


def test_a_record_rejected_in_the_slice_it_moved_to_is_not_a_delete():
    ds = strict_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2, x=1)})
    vs = run_slices(
        ds, "r2", {DAY_18: on(DAY_18, 2, x=1), DAY_19: on(DAY_19, 1, x="bad") + on(DAY_19, 3, x=1)}
    )
    assert ("1", "delete") not in [(r.key, r.op) for v in vs for r in all_records(v)]
    assert version_for(vs, DAY_18).deletes == 0
    assert [q.key for q in DEPS.store.quarantine.list_open()] == ["1"]
    assert rejected_chunks("r2", DAY_19) == []


def test_rejected_keys_survive_the_discard_of_a_no_op_slice_and_go_once_sealed():
    ds = strict_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2, x=1)})
    staged = stage_slices(ds, "r2", {DAY_18: on(DAY_18, 2, x=1), DAY_19: on(DAY_19, 1, x="bad")})
    assert [v.partition for v in staged] == [DAY_18]
    assert rejected_chunks("r2", DAY_19)[0].rows == 1
    vs = run_seal(ds, "r2", routes=ROUTES, deps=DEPS)
    assert (all_records(vs[0]), vs[0].deletes) == ([], 0)
    assert rejected_chunks("r2", DAY_19) == []


def test_rejected_keys_outlast_a_crash_before_sealing(monkeypatch):
    ds = strict_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2, x=1)})
    stage_slices(ds, "r2", {DAY_18: on(DAY_18, 2, x=1), DAY_19: on(DAY_19, 1, x="bad")})
    with monkeypatch.context() as patch:
        patch.setattr(DEPS.store.versions, "seal", _raise)
        with pytest.raises(OSError):
            run_seal(ds, "r2", routes=ROUTES, deps=DEPS)
    assert rejected_chunks("r2", DAY_19)[0].rows == 1
    assert run_seal(ds, "r2", routes=ROUTES, deps=DEPS)[0].deletes == 0


def test_a_read_that_seals_at_once_leaves_no_rejected_keys_behind():
    ds = strict_dataset()
    ds.source.load(on(DAY_18, 1, x="bad"))
    assert run_extract(ds, "r1", DAY_18, routes=ROUTES, stage_only=False, deps=DEPS) == []
    assert rejected_chunks("r1", DAY_18) == []


def test_a_crash_before_sealing_changes_nothing_on_the_repeat(monkeypatch):
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2, 3)})
    stage_slices(ds, "r2", {DAY_18: on(DAY_18, 3), DAY_19: on(DAY_19, 1)})

    with monkeypatch.context() as patch:
        patch.setattr(DEPS.store.versions, "seal", _raise)
        with pytest.raises(OSError):
            run_seal(ds, "r2", routes=ROUTES, deps=DEPS)
        with pytest.raises(OSError):
            run_seal(ds, "r2", routes=ROUTES, deps=DEPS)
    vs = run_seal(ds, "r2", routes=ROUTES, deps=DEPS)

    old = version_for(vs, DAY_18)
    assert [(r.key, r.op) for r in all_records(old)] == [("2", "delete")]
    assert (old.upserts, old.deletes) == (0, 1)
    assert moves_of(version_for(vs, DAY_19)) == {"1": DAY_18}
    assert not DEPS.spool.list_chunks(old.version_id, "candidates")


def test_a_restarted_slice_after_a_crash_is_decided_afresh(monkeypatch):
    ds = day_dataset()
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    stage_slices(ds, "r2", {DAY_18: on(DAY_18, 2), DAY_19: []})

    with monkeypatch.context() as patch:
        patch.setattr(DEPS.store.versions, "seal", _raise)
        with pytest.raises(OSError):
            run_seal(ds, "r2", routes=ROUTES, deps=DEPS)
    # The first attempt decided on a delete. Key 1 then turns up in day 19, whose slice is read again.
    ds.source.load(on(DAY_18, 2) + on(DAY_19, 1))
    run_extract(ds, "r2", DAY_19, routes=ROUTES, stage_only=True, deps=DEPS)
    vs = run_seal(ds, "r2", routes=ROUTES, deps=DEPS)

    old = version_for(vs, DAY_18)
    assert (all_records(old), old.deletes) == ([], 0)
    assert moves_of(version_for(vs, DAY_19)) == {"1": DAY_18}


def test_a_crash_after_sealing_finishes_the_state_on_the_repeat(monkeypatch):
    ds = day_dataset(keep_state=True)
    stage_slices(ds, "r1", {DAY_18: on(DAY_18, 1)})

    with monkeypatch.context() as patch:
        patch.setattr(DEPS.store.state, "put_many", _raise)
        with pytest.raises(OSError):
            run_seal(ds, "r1", routes=ROUTES, deps=DEPS)
    assert DEPS.store.state.get_many("d", ["1"]) == {}
    run_seal(ds, "r1", routes=ROUTES, deps=DEPS)
    assert DEPS.store.state.get_many("d", ["1"])["1"].partition == DAY_18


def _raise(*args, **kw):
    raise OSError("crashed")


def test_move_across_runs_with_keep_state():  # H1a
    ds = day_dataset(keep_state=True)
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1)})
    v = run_slices(ds, "r2", {DAY_19: on(DAY_19, 1)})[0]
    assert moves_of(v) == {"1": DAY_18}
    vs = run_slices(ds, "r3", {DAY_18: []})  # the old slice is read later
    assert ("1", "delete") not in [(r.key, r.op) for v in vs for r in all_records(v)]


def test_a_read_of_the_old_slice_passes_the_guard_after_a_move_across_runs():
    ds = day_dataset(keep_state=True)
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1)})
    run_slices(ds, "r2", {DAY_19: on(DAY_19, 1)})
    v = run_slices(ds, "r3", {DAY_18: []})[0]
    assert v.status == "sealed" and v.deletes == 0
    assert DEPS.store.state.get_many("d", ["1"])["1"].partition == DAY_19


def test_keep_state_keeps_a_delete_of_a_record_that_is_still_in_this_slice():
    ds = day_dataset(keep_state=True)
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, 2)})
    v = run_slices(ds, "r2", {DAY_18: on(DAY_18, 1)})[0]
    assert ("2", "delete") in [(r.key, r.op) for r in all_records(v)]
    assert list(DEPS.store.state.get_many("d", ["1", "2"])) == ["1"]


def test_an_upsert_in_its_own_partition_is_not_a_move():
    ds = day_dataset(keep_state=True)
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1)})
    v = run_slices(ds, "r2", {DAY_18: on(DAY_18, 1, x=1)})[0]
    assert moves_of(v) == {}


def test_keep_state_moves_a_by_stamp_record_too():
    ds: Any = fc.Dataset(
        "d",
        source=Source(),
        key="id",
        strategy=fc.strategy.by_stamp("date"),
        partition=fc.partition.by_day("date", tz="UTC"),
        keep_state=True,
    )
    ds.source.load(on(DAY_18, 1))
    run_extract(ds, "r1", "*", routes=ROUTES, stage_only=False, deps=DEPS)
    ds.source.load(on(DAY_19, 1))
    v = run_extract(ds, "r2", "*", routes=ROUTES, stage_only=False, deps=DEPS)[0]
    assert moves_of(v) == {"1": DAY_18}


def test_keep_state_records_the_latest_row():
    ds = day_dataset(keep_state=True)
    run_slices(ds, "r1", {DAY_18: on(DAY_18, 1, v="old")})
    moved = run_slices(ds, "r2", {DAY_19: on(DAY_19, 1, v="new")})[0]
    run_seal(ds, "r1", routes=ROUTES, deps=DEPS)  # a late repeat must not take the state back
    row = DEPS.store.state.get_many("d", ["1"])["1"]
    assert (row.partition, row.last_version, row.payload["v"]) == (DAY_19, moved.version_id, "new")

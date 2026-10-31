import logging
from datetime import UTC, datetime

import pytest

from feather_connect.connectors import memory
from feather_connect.core.connector import ApplyBatch, ApplyRecord, CheckResult, ReadRequest, RouteMode
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.partitions import by_day, none

CTX = Context(config={}, log=logging.getLogger("test"), now=datetime(2026, 10, 18, tzinfo=UTC), tz="UTC")


def batch(
    chunk=0, mode: RouteMode = "upsert", records=(), version="v1", partition="_", first=True, last=True
):
    return ApplyBatch(
        "r", version, chunk, partition, mode, list(records), f"r:{version}:{chunk}", first, last
    )


UP = ApplyRecord("k1", "upsert", {"a": 1})


def test_source_filters_by_partition_and_since():
    s = memory.Source()
    s.load([{"id": 1, "date": "2026-10-18", "u": 5}, {"id": 2, "date": "2026-10-19", "u": 9}])
    req = ReadRequest("date=2026-10-18", by_day("date", tz="UTC"))
    assert [r["id"] for b in s.read(CTX, req) for r in b] == [1]
    req = ReadRequest("*", none(), since=6, stamp_field="u")
    assert [r["id"] for b in s.read(CTX, req) for r in b] == [2]


def test_sink_upsert_then_delete():
    k = memory.Sink()
    k.apply(CTX, batch(records=[UP]))
    assert k.rows == [{"a": 1}]
    k.apply(CTX, batch(version="v2", records=[ApplyRecord("k1", "delete", None)]))
    assert k.rows == [] and k.deleted_keys == ["k1"]


def test_sink_apply_is_idempotent_per_version_and_chunk():
    k = memory.Sink()
    k.apply(CTX, batch(records=[UP]))
    k.apply(CTX, batch(records=[UP]))
    assert k.rows == [{"a": 1}]


def test_sink_append_keeps_every_record():
    k = memory.Sink()
    k.apply(CTX, batch(mode="append", records=[UP]))
    k.apply(CTX, batch(mode="append", version="v2", records=[UP]))
    assert k.rows == [{"a": 1}, {"a": 1}]


def test_sink_replace_partition_swaps_the_slice():
    k = memory.Sink()
    k.apply(CTX, batch(mode="replace_partition", partition="p1", records=[UP]))
    k.apply(
        CTX,
        batch(
            mode="replace_partition",
            partition="p1",
            version="v2",
            records=[ApplyRecord("k2", "upsert", {"a": 2})],
        ),
    )
    assert k.rows == [{"a": 2}]


def test_sink_failure_and_rejection_hooks():
    k = memory.Sink()
    k.fail_next(TransientError("boom"))
    with pytest.raises(TransientError):
        k.apply(CTX, batch(records=[UP]))
    k.reject("k1", "bad sku")
    r = k.apply(CTX, batch(records=[UP]))
    assert r.applied == 0 and r.rejected[0].message == "bad sku"


# Beyond the brief: the source


def test_source_declares_all_four_strategies():
    assert memory.Source.spec.strategies == {"by_stamp", "snapshot_diff", "replace", "push"}


def test_source_load_replaces_the_rows():
    s = memory.Source(rows=[{"id": 1}])
    req = ReadRequest("_", none())
    s.load([{"id": 2}])
    assert [r["id"] for b in s.read(CTX, req) for r in b] == [2]
    s.load([])
    assert list(s.read(CTX, req)) == []  # no rows, no batches


def test_source_since_includes_the_boundary_and_skips_rows_without_a_stamp():
    s = memory.Source(rows=[{"id": 1, "u": 5}, {"id": 2, "u": 6}, {"id": 3, "u": None}, {"id": 4}])
    req = ReadRequest("*", none(), since=6, stamp_field="u")
    assert [r["id"] for b in s.read(CTX, req) for r in b] == [2]


def test_source_compares_iso_text_stamps_as_datetimes():
    source = memory.Source(
        rows=[{"id": 1, "u": "2026-10-18T09:00:00Z"}, {"id": 2, "u": "2026-10-18T15:30:00+05:30"}]
    )
    since = datetime(2026, 10, 18, 9, 30, tzinfo=UTC)
    req = ReadRequest("*", none(), since=since, stamp_field="u")
    assert [r["id"] for b in source.read(CTX, req) for r in b] == [2]


def test_source_lets_through_a_stamp_it_cannot_compare():
    source = memory.Source(rows=[{"id": 1, "u": "soon"}])
    since = datetime(2026, 10, 18, tzinfo=UTC)
    req = ReadRequest("*", none(), since=since, stamp_field="u")
    assert [r["id"] for b in source.read(CTX, req) for r in b] == [1]


def test_source_discover_lists_one_stream_with_the_fields_of_its_rows():
    s = memory.Source(rows=[{"id": 1, "name": None}, {"id": 2, "name": "x", "price": 1.5}])
    [stream] = s.discover(CTX)
    assert stream.fields == {"id": "int", "name": "str", "price": "float"}


# Beyond the brief: the sink


def test_sink_declares_upsert_append_and_replace_partition():
    assert memory.Sink.spec.modes == {"upsert", "append", "replace_partition"}


def test_connections_are_named_and_checks_pass():
    assert memory.Source().connection == "memory" and memory.Sink(connection="zakya").connection == "zakya"
    assert memory.Source().check(CTX) == CheckResult(True) == memory.Sink().check(CTX)


def test_sink_keys_can_be_tuples_and_deletes_of_unknown_keys_are_logged():
    k = memory.Sink()
    key = ("M2", "RET")
    k.apply(CTX, batch(records=[ApplyRecord(key, "upsert", {"a": 1})]))
    k.apply(
        CTX,
        batch(version="v2", records=[ApplyRecord(key, "delete", None), ApplyRecord("zz", "delete", None)]),
    )
    assert k.rows == [] and k.deleted_keys == [key, "zz"]


def test_sink_repeat_returns_the_first_result_and_counts_as_a_call():
    k = memory.Sink()
    first = k.apply(CTX, batch(records=[UP]))
    assert k.apply(CTX, batch(records=[UP])) is first
    assert [c.chunk_no for c in k.calls] == [0, 0]


def test_sink_fail_next_queues_and_a_failed_call_is_not_recorded():
    k = memory.Sink()
    k.fail_next(TransientError("one"))
    k.fail_next(PermanentError("two"))
    with pytest.raises(TransientError, match="one"):
        k.apply(CTX, batch(records=[UP]))
    with pytest.raises(PermanentError, match="two"):
        k.apply(CTX, batch(records=[UP]))
    assert k.calls == [] and k.rows == []
    assert k.apply(CTX, batch(records=[UP])).applied == 1 and len(k.calls) == 1


def test_sink_reject_spares_the_other_records_and_lasts_for_one_arrival():
    k = memory.Sink()
    k.reject("k1", "bad sku")
    r = k.apply(CTX, batch(records=[UP, ApplyRecord("k2", "upsert", {"a": 2})]))
    assert r.applied == 1 and [(x.key, x.error_class) for x in r.rejected] == [("k1", "RecordRejected")]
    assert k.rows == [{"a": 2}]
    r = k.apply(CTX, batch(version="v2", records=[UP]))  # the key arrives again: now it is accepted
    assert r.applied == 1 and r.rejected == () and k.rows == [{"a": 2}, {"a": 1}]


def test_sink_replace_partition_in_chunks_clears_only_its_partition_and_only_at_the_start():
    k = memory.Sink()

    def put(key, partition, version="v1", **kw):
        records = [ApplyRecord(key, "upsert", {"k": key})]  # each partition's version has an id of its own
        version = f"{partition}:{version}"
        k.apply(
            CTX, batch(mode="replace_partition", partition=partition, version=version, records=records, **kw)
        )

    put("z", "p2")
    put("a", "p1", chunk=0, first=True, last=False)
    put("b", "p1", chunk=1, first=False, last=True)
    assert k.rows == [{"k": "z"}, {"k": "a"}, {"k": "b"}]
    put("c", "p1", version="v2")
    assert k.rows == [{"k": "z"}, {"k": "c"}]


def test_sink_replace_partition_keeps_a_record_that_moved_to_another_partition():
    k = memory.Sink()
    k.apply(CTX, batch(mode="replace_partition", partition="p1", records=[UP]))
    moved = ApplyRecord("k1", "upsert", {"a": 2}, moved_from="p1")
    k.apply(CTX, batch(mode="replace_partition", partition="p2", version="v2", records=[moved]))
    k.apply(CTX, batch(mode="replace_partition", partition="p1", version="v3", records=[]))
    assert k.rows == [{"a": 2}]


def test_sink_refuses_a_mode_it_does_not_declare():
    k = memory.Sink()
    with pytest.raises(PermanentError, match="side_effect"):
        k.apply(CTX, batch(mode="side_effect", records=[UP]))
    assert k.calls == []


def test_sink_upsert_needs_a_payload():
    with pytest.raises(PermanentError, match="k1"):
        memory.Sink().apply(CTX, batch(records=[ApplyRecord("k1", "upsert", None)]))


def test_sink_has_nothing_to_compensate():
    with pytest.raises(PermanentError, match="compensate"):
        memory.Sink().compensate(CTX, batch())

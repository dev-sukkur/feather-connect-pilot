import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory
from feather_connect.connectors import simulation as sim
from feather_connect.core.connector import ApplyBatch, ApplyRecord, ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.partitions import by_day, none
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.spool import MemorySpool

NOW = datetime(2026, 10, 18, tzinfo=UTC)
CTX = Context(config={}, log=logging.getLogger("test"), now=NOW, tz="UTC")
GEN = {
    "id": sim.seq(),
    "store": sim.choice([1, 2]),
    "day": sim.date_in("2026-10-01", "2026-10-07"),
    "amount": sim.amount(10, 100),
    "customer": sim.name(),
}


def read_all(s: sim.Source, partition: str = "*") -> list[dict[str, Any]]:
    scheme = none() if partition == "*" else by_day("day", tz="UTC")
    return [r for b in s.read(CTX, ReadRequest(partition, scheme)) for r in b]


def source(faults: sim.Faults | None = None, **kw: Any) -> sim.Source:
    return sim.Source(generator=GEN, rows_per_partition=20, faults=faults or sim.Faults(), **kw)


def test_same_seed_gives_same_rows() -> None:
    assert read_all(sim.Source(generator=GEN, seed=1)) == read_all(sim.Source(generator=GEN, seed=1))
    assert read_all(sim.Source(generator=GEN, seed=1)) != read_all(sim.Source(generator=GEN, seed=2))


def test_generated_fields_follow_their_specs() -> None:
    rows = read_all(sim.Source(generator=GEN, rows_per_partition=30))
    assert [r["id"] for r in rows] == list(range(1, 31))
    assert {r["store"] for r in rows} <= {1, 2}
    assert all("2026-10-01" <= r["day"] <= "2026-10-07" for r in rows)
    assert all(10 <= r["amount"] <= 100 for r in rows)
    assert all(" " in r["customer"] for r in rows)


def test_a_source_needs_exactly_one_of_fixture_and_generator() -> None:
    with pytest.raises(ValueError):
        sim.Source()
    with pytest.raises(ValueError):
        sim.Source(fixture="x.json", generator=GEN)


def test_reads_honour_partition_and_since() -> None:
    s = sim.Source(generator={"id": sim.seq(), "day": sim.date_in("2026-10-01", "2026-10-03")})
    rows = read_all(s, "day=2026-10-02")
    assert rows and {r["day"] for r in rows} == {"2026-10-02"}
    req = ReadRequest("*", none(), since=50, stamp_field="id")
    assert [r["id"] for b in s.read(CTX, req) for r in b] == list(range(50, 101))


def test_fixture_file_is_read(tmp_path: Path) -> None:
    rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    as_json = tmp_path / "rows.json"
    as_json.write_text(json.dumps(rows), encoding="utf-8")
    as_jsonl = tmp_path / "rows.jsonl"
    as_jsonl.write_text("\n".join(json.dumps(r) for r in rows) + "\n\n", encoding="utf-8")
    assert read_all(sim.Source(fixture=as_json)) == rows
    assert read_all(sim.Source(fixture=str(as_jsonl))) == rows
    assert sim.Source(fixture=as_json).discover(CTX)[0].fields == {"id": "int", "v": "str"}
    not_a_list = tmp_path / "one.json"
    not_a_list.write_text(json.dumps(rows[0]), encoding="utf-8")
    with pytest.raises(PermanentError):
        sim.Source(fixture=not_a_list)


def test_duplicates_fault_repeats_rows() -> None:
    rows = read_all(source(sim.Faults(duplicate=1.0)))
    assert len(rows) == 40
    assert len({r["id"] for r in rows}) == 20


def test_hard_delete_fault_drops_rows_on_a_later_read() -> None:
    s = source(sim.Faults(hard_delete=0.5))
    assert len(read_all(s)) == 20
    later = read_all(s)
    assert 0 < len(later) < 20
    assert {r["id"] for r in later} <= set(range(1, 21))
    assert len(read_all(s)) <= len(later)  # a deleted row stays deleted


def test_key_rewrite_fault_changes_the_key_field_in_place() -> None:
    s = source(sim.Faults(key_rewrite=1.0))
    assert [r["id"] for r in read_all(s)] == list(range(1, 21))
    rewritten = read_all(s)
    assert [r["id"] for r in rewritten] == [n + 1_000_000 for n in range(1, 21)]
    assert [r["customer"] for r in rewritten] == [r["customer"] for r in read_all(source())]
    named = sim.Source(
        generator={"code": choice_of("a")}, rows_per_partition=1, faults=sim.Faults(key_rewrite=1.0)
    )
    read_all(named)
    assert read_all(named) == [{"code": "a-rewritten"}]


def choice_of(value: str) -> sim.FieldSpec:
    return sim.choice([value])


def test_schema_change_fault_adds_and_removes_a_column() -> None:
    s = source(sim.Faults(schema_change=1.0))
    before = read_all(s)
    assert list(before[0]) == ["id", "store", "day", "amount", "customer"]
    after = read_all(s)
    assert list(after[0]) == ["id", "store", "day", "amount", sim.ADDED_FIELD]
    assert list(read_all(s)[0]) == list(after[0])  # the change happens once


def test_late_fault_holds_rows_for_a_later_read() -> None:
    s = source(sim.Faults(late=1.0))
    assert read_all(s) == []
    s.faults = sim.Faults()
    assert sorted(r["id"] for r in read_all(s)) == list(range(1, 21))
    assert len(read_all(s)) == 20


def test_late_rows_come_back_only_for_their_partition() -> None:
    s = sim.Source(
        generator={"id": sim.seq(), "day": sim.date_in("2026-10-01", "2026-10-02")},
        faults=sim.Faults(late=1.0),
    )
    assert read_all(s, "day=2026-10-01") == []
    s.faults = sim.Faults()
    other = read_all(s, "day=2026-10-02")  # nothing of the second day was held
    assert other and {r["day"] for r in other} == {"2026-10-02"}
    first = read_all(s, "day=2026-10-01")
    assert first and {r["day"] for r in first} == {"2026-10-01"}
    assert len({r["id"] for r in first}) == len(first)


def test_http_faults_raise_transient_errors() -> None:
    s = sim.Source(generator=GEN, faults=sim.Faults(http_429=1.0))
    with pytest.raises(TransientError) as e:
        read_all(s)
    assert e.value.retry_after is not None
    with pytest.raises(TransientError) as e5:
        read_all(sim.Source(generator=GEN, faults=sim.Faults(http_5xx=1.0)))
    assert e5.value.retry_after is None
    with pytest.raises(TransientError, match="timeout"):
        read_all(sim.Source(generator=GEN, faults=sim.Faults(timeout=1.0)))


def test_faults_are_repeatable_by_seed() -> None:
    faults = sim.Faults(duplicate=0.3, late=0.2, hard_delete=0.2, key_rewrite=0.2, schema_change=0.5)

    def reads(seed: int) -> list[list[dict[str, Any]]]:
        s = source(faults, seed=seed)
        return [read_all(s) for _ in range(4)]

    assert reads(3) == reads(3)
    assert reads(3) != reads(4)


def test_fan_out_on_the_local_runtime() -> None:
    clock = FrozenClock(NOW)
    store = MemoryStore(clock)
    engine = LocalEngine(clock)
    a, b = sim.Sink("a"), sim.Sink("b")
    src = source(sim.Faults(duplicate=0.2))
    flow = fc.Flow(
        "f",
        reads=fc.Dataset("d", source=src, key="id", strategy=fc.strategy.replace()),
        deliver=[fc.to(a), fc.to(b)],
    )
    rt = Runtime([flow], store, MemorySpool(), engine, code_sha="sha", clock=clock)
    engine.register(rt.tasks())
    run = rt.start_run("f")
    engine.run_until_idle()
    assert store.runs.get_run(run).status == "succeeded"
    expected = {r["id"] for r in read_all(source())}
    for sink in (a, b):
        ids = [r["id"] for r in sink.rows]
        assert len(ids) == len(set(ids)) == 20
        assert set(ids) == expected


def test_capture_to_writes_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "out" / "captured.jsonl"
    k = sim.Sink(capture_to=path)
    records = [ApplyRecord("k1", "upsert", {"a": 1}), ApplyRecord("k2", "delete", None)]
    batch = ApplyBatch("r", "v1", 0, "_", "upsert", records, "r:v1:0", True, True)
    k.apply(CTX, batch)
    k.apply(CTX, batch)  # a repeat is not written again
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert lines == [
        {"op": "upsert", "key": "k1", "payload": {"a": 1}},
        {"op": "delete", "key": "k2", "payload": None},
    ]
    assert k.rows == [{"a": 1}]


def test_sink_without_capture_behaves_like_the_memory_sink() -> None:
    k = sim.Sink()
    assert k.capture_to is None and isinstance(k, memory.Sink)
    assert k.spec.modes == memory.Sink.spec.modes


def _first_chunk(version_id: str) -> ApplyBatch:
    records = [ApplyRecord("k1", "upsert", {"a": 1})]
    return ApplyBatch("r", version_id, 0, "_", "upsert", records, f"r:{version_id}:0", True, True)


def _fails(k: sim.Sink, version_id: str) -> bool:
    try:
        k.apply(CTX, _first_chunk(version_id))
    except PermanentError:
        return True
    return False


def test_a_sink_with_no_fail_rate_never_fails() -> None:
    k = sim.Sink(fail_rate=0.0, seed=1)
    assert not any(_fails(k, f"v{n}") for n in range(200))


def test_a_sink_fails_a_version_once_with_a_permanent_error_and_then_lets_it_through() -> None:
    k = sim.Sink(fail_rate=1.0)
    with pytest.raises(PermanentError, match="simulated delivery failure"):
        k.apply(CTX, _first_chunk("v1"))
    assert k.rows == []  # nothing was written
    k.apply(CTX, _first_chunk("v1"))  # a retry of the same version goes through
    assert k.rows == [{"a": 1}]
    assert _fails(k, "v2")  # another version is still failed once


def test_only_the_first_chunk_of_a_version_can_fail() -> None:
    k = sim.Sink(fail_rate=1.0)
    later = ApplyBatch("r", "v1", 1, "_", "upsert", [ApplyRecord("k", "upsert", {})], "r:v1:1", False, True)
    k.apply(CTX, later)
    assert k.rows == [{}]


def test_the_failures_of_a_sink_repeat_with_the_seed() -> None:
    def verdicts(seed: int) -> list[bool]:
        k = sim.Sink(fail_rate=0.3, seed=seed)
        return [_fails(k, f"v{n}") for n in range(60)]

    assert verdicts(3) == verdicts(3)
    assert verdicts(3) != verdicts(4)
    assert 5 < sum(verdicts(3)) < 35  # about three in ten


def test_a_fail_rate_outside_zero_to_one_is_refused() -> None:
    with pytest.raises(ValueError, match="fail_rate"):
        sim.Sink(fail_rate=1.5)

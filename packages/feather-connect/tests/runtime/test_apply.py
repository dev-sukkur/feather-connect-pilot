import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pydantic import BaseModel

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors.memory import Sink, Source
from feather_connect.core.connector import ApplyResult, RecordResponse
from feather_connect.core.context import Context
from feather_connect.core.errors import BudgetDeferred, PermanentError, RecordRejected, TransientError
from feather_connect.core.records import Record, row_hash
from feather_connect.engine.local import LocalEngine
from feather_connect.engine.port import RetryPolicy, TaskCall
from feather_connect.logs import register_secrets
from feather_connect.runtime.apply import run_apply
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import finalize_versions, run_extract
from feather_connect.storage import spool
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import VersionDraft
from feather_connect.storage.spool import MemorySpool, records_to_table

T0 = datetime(2026, 10, 18, 10, tzinfo=UTC)
T1 = T0 + timedelta(hours=1)
ROWS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]

DEPS: RuntimeDeps
ENGINE: LocalEngine
LEDGER: Any
QUARANTINE: Any


@pytest.fixture(autouse=True)
def fresh_deps():
    global DEPS, ENGINE, LEDGER, QUARANTINE
    clock = FrozenClock(T0)

    def make_context(connection: str) -> Context:
        return Context({}, logging.getLogger(connection), clock.now(), "UTC")

    DEPS = RuntimeDeps(MemoryStore(clock), MemorySpool(), clock, "sha", make_context)
    ENGINE = LocalEngine(clock)
    LEDGER = DEPS.store.ledger
    QUARANTINE = DEPS.store.quarantine


def dataset(strategy=None, *, name="d", key: Any = "id", **kw) -> Any:
    return fc.Dataset(name, source=Source(), key=key, strategy=strategy or fc.strategy.replace(), **kw)


def route_to(name="r", *, map=None, mode: Any = "upsert") -> Any:
    return fc.to(Sink(name), name=name, map=map, mode=mode)


def seal(ds, rows, *routes, run="r1", partition="_"):
    """Extract `rows` through to a sealed version with a delivery per route."""
    ds.source.load(rows)
    return run_extract(ds, run, partition, routes=routes, stage_only=False, deps=DEPS)


def seal_versions(ds, rows_by_run, *routes, partition="_"):
    """Seal one version per item of `rows_by_run` and return their ids."""
    return [
        seal(ds, rows, *routes, run=f"r{n}", partition=partition)[0].version_id
        for n, rows in enumerate(rows_by_run, 1)
    ]


def submit(ds, route, version_id) -> str:
    """Queue `run_apply` as a new engine task, as a retry or a replay of a delivery does."""
    name = f"apply-{len(ENGINE.submitted())}"
    ENGINE.register({name: lambda ctx, args: run_apply(ctx, ds, route, args["version_id"], DEPS)})
    return ENGINE.submit(TaskCall(name, {"version_id": version_id}, name, retry=DEPS.retry))


def apply(ds, route, version_id) -> str:
    ref = submit(ds, route, version_id)
    ENGINE.run_until_idle()
    return ref


def statuses(route_name, version_id):
    return [a.to_status for a in LEDGER.attempts(route_name, version_id)]


def status(route_name, version_id):
    return LEDGER.get(route_name, version_id).status


def sent(sink):
    return [(r.key, r.op) for call in sink.calls for r in call.records]


def test_routes_progress_independently():  # H6
    ds = dataset()
    bad, good = route_to("bad"), route_to("good")
    [v1] = seal_versions(ds, [ROWS], bad, good)
    bad.sink.fail_next(PermanentError("down"))
    ref = apply(ds, bad, v1)
    apply(ds, good, v1)
    assert status("bad", v1) == "failed" and status("good", v1) == "applied"
    assert LEDGER.get("bad", v1).last_error == "down"
    assert ENGINE.status(ref) == "succeeded"  # ended without an engine-level failure


def test_extraction_continues_while_a_route_is_down():  # H6
    ds = dataset()
    bad = route_to("bad")
    [v1] = seal_versions(ds, [ROWS], bad)
    bad.sink.fail_next(PermanentError("down"))
    apply(ds, bad, v1)
    v2 = seal(ds, [{"id": 1, "v": "b"}], bad, run="r2")[0].version_id
    v3 = seal(ds, [{"id": 1, "v": "c"}], bad, run="r3")[0].version_id
    apply(ds, bad, v2)
    apply(ds, bad, v3)
    assert bad.sink.calls == [] and status("bad", v3) == "pending"
    LEDGER.transition("bad", v1, "pending", actor="op")
    apply(ds, bad, v3)
    assert [c.version_id for c in bad.sink.calls] == [v1, v2, v3]


def test_skipping_a_failed_version_unblocks_the_next():
    ds = dataset()
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 1, "v": "z"}]], route)
    route.sink.fail_next(PermanentError("down"))
    apply(ds, route, v1)
    LEDGER.transition("r", v1, "skipped", actor="op")
    apply(ds, route, v2)
    assert [c.version_id for c in route.sink.calls] == [v2]


def test_versions_apply_in_seal_order():  # I2
    ds = dataset()
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 1, "v": "z"}]], route)
    apply(ds, route, v2)  # submitted for the later version
    assert [c.version_id for c in route.sink.calls] == [v1, v2]
    assert status("r", v1) == status("r", v2) == "applied"


def test_a_later_version_is_left_for_its_own_task():
    ds = dataset()
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 1, "v": "z"}]], route)
    apply(ds, route, v1)
    assert [c.version_id for c in route.sink.calls] == [v1] and status("r", v2) == "pending"


def test_a_failed_version_blocks_its_partition_only():
    ds = dataset(partition=fc.partition.by_fields("s"))
    route = route_to()
    a = {"id": 1, "s": "a"}
    v1 = seal(ds, [a], route, run="r1", partition="s=a")[0].version_id
    v2 = seal(ds, [{**a, "v": 2}], route, run="r2", partition="s=a")[0].version_id
    v3 = seal(ds, [{"id": 2, "s": "b"}], route, run="r3", partition="s=b")[0].version_id
    route.sink.fail_next(PermanentError("down"))
    apply(ds, route, v1)
    apply(ds, route, v2)
    apply(ds, route, v3)
    assert status("r", v1) == "failed" and status("r", v2) == "pending" and status("r", v3) == "applied"
    assert [c.version_id for c in route.sink.calls] == [v3]


def test_a_held_version_blocks_its_partition():
    ds = dataset()
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 1, "v": "z"}]], route)
    LEDGER.transition("r", v1, "running", actor="t")
    LEDGER.transition("r", v1, "held", actor="t")
    apply(ds, route, v2)
    assert route.sink.calls == [] and status("r", v2) == "pending"


def test_a_route_with_two_datasets_orders_each_on_its_own():
    first, second = dataset(name="first"), dataset(name="second", key="sku")
    route = route_to()
    [v1] = seal_versions(first, [ROWS], route)
    [v2] = seal_versions(second, [[{"sku": "x"}]], route)
    apply(second, route, v2)  # v1 of the other dataset is older, and neither blocks nor comes along
    assert [c.version_id for c in route.sink.calls] == [v2]
    assert status("r", v1) == "pending" and status("r", v2) == "applied"


def test_transient_error_retries_and_succeeds():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.fail_next(TransientError("timeout"))
    apply(ds, route, v1)
    delivery = LEDGER.get("r", v1)
    assert delivery.status == "applied" and delivery.attempts == 2 and delivery.last_error is None
    assert statuses("r", v1) == ["running", "pending", "running", "applied"]
    assert LEDGER.attempts("r", v1)[1].error == "timeout"
    assert len(route.sink.calls) == 1


def test_transient_errors_that_use_up_the_attempts_fail_the_delivery():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    for _ in range(6):
        route.sink.fail_next(TransientError("timeout"))
    ref = apply(ds, route, v1)
    assert status("r", v1) == "failed" and "timeout" in LEDGER.get("r", v1).last_error
    assert ENGINE.status(ref) == "succeeded"


def test_applied_at_most_once():  # I3
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    apply(ds, route, v1)
    assert len(route.sink.calls) == 1
    assert statuses("r", v1) == ["running", "applied"]


def test_crash_mid_apply_resumes_without_duplicates(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 2)
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    ENGINE.crash_before(f"chunk:{v1}:1")
    ref = apply(ds, route, v1)
    assert ENGINE.status(ref) == "crashed" and status("r", v1) == "running"
    ENGINE.recover()
    ENGINE.run_until_idle()
    assert [c.chunk_no for c in route.sink.calls] == [0, 1] and status("r", v1) == "applied"
    assert LEDGER.get("r", v1).rows_applied == 3


@pytest.mark.parametrize("step", ["target", "next:0", "start:{v}", "end:{v}"])
def test_a_crash_before_any_step_recovers(step):
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    ENGINE.crash_before(step.format(v=v1))
    apply(ds, route, v1)
    ENGINE.recover()
    ENGINE.run_until_idle()
    assert len(route.sink.calls) == 1 and statuses("r", v1) == ["running", "applied"]


def test_a_task_that_dies_after_the_start_is_picked_up_by_the_next():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    LEDGER.transition("r", v1, "running", actor="dead task")
    apply(ds, route, v1)
    assert status("r", v1) == "applied" and len(route.sink.calls) == 1


def test_a_failed_delivery_retried_uses_the_same_sink_key():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.fail_next(PermanentError("down"))
    apply(ds, route, v1)
    LEDGER.transition("r", v1, "pending", actor="op")
    apply(ds, route, v1)
    assert route.sink.calls[0].idempotency_key == f"r:{v1}:0:0"


def test_a_replay_reaches_the_sink_again_under_a_new_key():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    LEDGER.transition("r", v1, "pending", actor="op")
    apply(ds, route, v1)
    assert [c.idempotency_key for c in route.sink.calls] == [f"r:{v1}:0:0", f"r:{v1}:0:1"]
    assert statuses("r", v1)[-1] == "applied"


def test_chunk_ends_are_marked(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 1)
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert [(c.is_first, c.is_last) for c in route.sink.calls] == [
        (True, False),
        (False, False),
        (False, True),
    ]


def test_sink_rejection_quarantines_and_continues():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.reject("2", "bad sku")
    apply(ds, route, v1)
    delivery = LEDGER.get("r", v1)
    assert delivery.status == "applied" and (delivery.rows_applied, delivery.rows_rejected) == (2, 1)
    row = QUARANTINE.list_open()[0]
    assert (row.key, row.error, row.route, row.version_id) == ("2", "bad sku", "r", v1)
    assert row.payload == {"id": 2, "v": "b"}  # kept, so that it can be looked at and retried (ruling 11)


class AnsweringSink(Sink):
    """A sink that reports what a destination answered, as an HTTP call sink does. With `fail_after`, the chunk fails
    once that many records have been answered, as a call that runs out of retries part-way does."""

    def __init__(self, name: str, fail_after: int | None = None, skip: frozenset[str] = frozenset()) -> None:
        super().__init__(name)
        self.fail_after = fail_after
        self.skip = skip

    def apply(self, ctx: Context, batch: Any) -> ApplyResult:
        responses: list[RecordResponse] = []
        for record in batch.records:
            if record.key in self.skip:
                continue
            if self.fail_after is not None and len(responses) == self.fail_after:
                raise TransientError("the destination went away")
            fields = {"erp_id": f"e-{record.key}", "token": "Bearer leaked-123", "plan": "Basic membership"}
            response = RecordResponse(record.key, 201, fields)
            ctx.answered(response)
            responses.append(response)
        return ApplyResult(len(responses), responses=responses)


def test_what_the_destination_answered_is_kept_redacted_and_logged(caplog):  # ruling 5, M4, M7
    register_secrets("leaked-123")
    caplog.set_level(logging.INFO)
    ds = dataset()
    route = fc.to(AnsweringSink("r"), name="r")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    rows = DEPS.store.responses.of_delivery("r", v1)
    assert [(r.key, r.status, r.fields["erp_id"]) for r in rows] == [
        ("1", 201, "e-1"),
        ("2", 201, "e-2"),
        ("3", 201, "e-3"),
    ]
    assert all(r.fields["token"] == "Bearer ***" for r in rows) and "leaked-123" not in caplog.text
    assert rows[0].fields["plan"] == "Basic membership"  # only known secret values are masked in stored data
    lines = [r.getMessage() for r in caplog.records if "answers" in r.getMessage()]
    assert lines == ["route r, chunk 0: 3 answers (HTTP 201: 3)"]  # one line per chunk, not one per record


def test_answers_that_came_before_a_chunk_failed_are_kept():  # I3
    ds = dataset()
    route = fc.to(AnsweringSink("r", fail_after=2), name="r")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert [r.key for r in DEPS.store.responses.of_delivery("r", v1)] == ["1", "2"]


def test_a_chunk_sent_again_shows_only_its_latest_answers(monkeypatch):  # M8
    ds = dataset()
    sink = AnsweringSink("r")
    route = fc.to(sink, name="r")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    sink.skip = frozenset({"2"})  # say the map now turns record 2 down
    DEPS.store.ledger.transition("r", v1, "pending", actor="t", reason="replay")
    apply(ds, route, v1)
    assert [r.key for r in DEPS.store.responses.of_delivery("r", v1)] == ["1", "3"]


def test_sink_rejection_with_on_record_error_fail_fails_the_version():
    ds = dataset(on_record_error="fail")
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.reject("2", "bad sku")
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and "bad sku" in LEDGER.get("r", v1).last_error
    assert QUARANTINE.list_open() == []


def test_a_sink_rejection_of_a_composite_key_quarantines_the_stored_key():
    ds = dataset(key=("s", "n"))
    route = route_to()
    [v1] = seal_versions(ds, [[{"s": "x", "n": 1}]], route)
    route.sink.reject(("x", 1), "no")
    apply(ds, route, v1)
    assert route.sink.calls[0].records[0].key == ("x", 1)
    assert QUARANTINE.list_open()[0].key == '["x",1]'


def test_map_none_filters_and_a_list_splits():
    def split(r, c):
        if r["id"] == 1:
            return None
        if r["id"] == 2:
            return [{"id": 2, "part": 1}, {"id": 2, "part": 2}]
        return dict(r)

    ds = dataset()
    route = route_to(map=split, mode="append")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert sent(route.sink) == [("2", "upsert"), ("2", "upsert"), ("3", "upsert")]
    assert [row["part"] for row in route.sink.rows[:2]] == [1, 2]
    assert LEDGER.get("r", v1).rows_applied == 3


def test_a_chunk_that_maps_to_nothing_makes_no_sink_call():
    ds = dataset()
    route = route_to(map=lambda r, c: None)
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert route.sink.calls == [] and status("r", v1) == "applied"


def test_map_error_quarantines_the_record():  # Review Focus 5
    def boom(r, c):
        if r["id"] == 2:
            raise ValueError("no price")
        return dict(r)

    ds = dataset()
    route = route_to(map=boom)
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert [x["id"] for x in route.sink.rows] == [1, 3] and "no price" in QUARANTINE.list_open()[0].error
    assert LEDGER.get("r", v1).rows_rejected == 1


def test_record_rejected_by_a_map_quarantines_the_record():
    def reject(r, c):
        raise RecordRejected("nope")

    ds = dataset()
    route = route_to(map=reject)
    [v1] = seal_versions(ds, [[{"id": 1}]], route)
    apply(ds, route, v1)
    assert status("r", v1) == "applied" and QUARANTINE.list_open()[0].error == "nope"


def test_map_error_with_on_record_error_fail_fails_the_version():
    def boom(r, c):
        raise ValueError("no price")

    ds = dataset(on_record_error="fail")
    route = route_to(map=boom)
    [v1] = seal_versions(ds, [ROWS], route)
    ref = apply(ds, route, v1)
    assert status("r", v1) == "failed" and "no price" in LEDGER.get("r", v1).last_error
    assert route.sink.calls == [] and QUARANTINE.list_open() == [] and ENGINE.status(ref) == "succeeded"


def test_a_map_giving_the_wrong_type_fails_the_version():
    ds = dataset()
    route = route_to(map=lambda r, c: "text")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and "produced a str" in LEDGER.get("r", v1).last_error


def test_a_transient_error_from_a_map_is_retried():
    failures = [TransientError("lookup down")]

    def flaky(r, c):
        if failures:
            raise failures.pop()
        return dict(r)

    ds = dataset()
    route = route_to(map=flaky)
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "applied" and LEDGER.get("r", v1).attempts == 2
    assert QUARANTINE.list_open() == []


def test_batch_map_transforms_a_chunk():
    @fc.batch_map
    def upper(table, ctx):
        return table.append_column("w", pc.utf8_upper(table.column("v")))

    ds = dataset()
    route = route_to(map=upper)
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert [row["w"] for row in route.sink.rows] == ["A", "B", "C"]
    assert not any(name.startswith("_fc_") for name in route.sink.rows[0])
    assert sent(route.sink) == [("1", "upsert"), ("2", "upsert"), ("3", "upsert")]


def test_batch_map_can_filter_and_deletes_skip_it():
    seen = []

    @fc.batch_map
    def keep_first(table, ctx):
        seen.append(table.num_rows)
        return table.slice(0, 1)

    ds = dataset(fc.strategy.snapshot_diff())
    route = route_to(map=keep_first)
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 1, "v": "z"}, {"id": 2, "v": "y"}]], route)
    apply(ds, route, v2)
    assert seen == [3, 2]
    assert sent(route.sink)[-2:] == [("1", "upsert"), ("3", "delete")]
    assert status("r", v1) == status("r", v2) == "applied"


def test_batch_map_decodes_a_column_kept_as_json():
    @fc.batch_map
    def same(table, ctx):
        return table

    ds = dataset()
    route = route_to(map=same)
    [v1] = seal_versions(ds, [[{"id": 1, "m": 1}, {"id": 2, "m": "two"}]], route)
    apply(ds, route, v1)
    assert [row["m"] for row in route.sink.rows] == [1, "two"]


@pytest.mark.parametrize(
    ("result", "message"),
    [
        (lambda table: table.select(["v"]), "_fc_key"),
        (lambda table: "text", "must return"),
    ],
)
def test_a_batch_map_that_loses_the_keys_fails_the_version(result, message):
    @fc.batch_map
    def wrong(table, ctx):
        return result(table)

    ds = dataset()
    route = route_to(map=wrong)
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and message in LEDGER.get("r", v1).last_error


def test_a_batch_map_error_fails_the_version():
    @fc.batch_map
    def boom(table, ctx):
        raise ValueError("bad batch")

    ds = dataset()
    route = route_to(map=boom)
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and "bad batch" in LEDGER.get("r", v1).last_error


def test_deletes_skip_the_map_and_reach_the_sink():
    def boom(r, c):
        if r.op == "delete":
            raise AssertionError("a delete reached the map")
        return dict(r)

    ds = dataset(fc.strategy.snapshot_diff())
    route = route_to(map=boom)
    v1, v2 = seal_versions(ds, [ROWS, ROWS[:2]], route)
    apply(ds, route, v2)
    assert route.sink.deleted_keys == ["3"] and status("r", v2) == "applied"
    delete = route.sink.calls[-1].records[0]
    assert (delete.op, delete.payload) == ("delete", None)
    assert status("r", v1) == "applied"


def test_append_adds_fc_columns():
    ds = dataset()
    route = route_to(mode="append")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    row = route.sink.rows[0]
    assert {"_fc_key", "_fc_op", "_fc_version", "_fc_run", "_fc_extracted_at", "_fc_partition"} <= row.keys()
    assert (row["_fc_key"], row["_fc_op"], row["_fc_version"]) == ("1", "upsert", v1)
    assert (row["_fc_run"], row["_fc_partition"], row["_fc_extracted_at"]) == ("r1", "_", T0)


def test_append_keeps_a_delete_as_a_record_with_the_columns():
    ds = dataset(fc.strategy.snapshot_diff())
    route = route_to(mode="append")
    v1, v2 = seal_versions(ds, [ROWS, ROWS[:2]], route)
    apply(ds, route, v2)
    delete = route.sink.calls[-1].records[0]
    assert delete.op == "delete" and delete.payload["_fc_op"] == "delete"
    assert delete.payload["_fc_version"] == v2 != v1


def replace_route() -> Any:
    return route_to(mode="replace_partition")


def test_replace_partition_gets_the_whole_slice():
    ds = dataset(fc.strategy.snapshot_diff())
    route = replace_route()
    v1, v2 = seal_versions(ds, [ROWS, [ROWS[0], {"id": 2, "v": "B"}]], route)
    apply(ds, route, v2)
    last = route.sink.calls[-1]
    assert last.version_id == v2 and last.is_first and last.is_last
    assert [r.key for r in last.records] == ["1", "2"]  # 1 didn't change, and comes anyway; 3 is gone
    assert route.sink.rows == [{"id": 1, "v": "a"}, {"id": 2, "v": "B"}] and status("r", v1) == "applied"


def test_replace_partition_keeps_the_last_row_of_a_key_across_chunks(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 2)
    ds = dataset()
    route = replace_route()
    rows = [{"id": 1, "v": "old"}, {"id": 2, "v": "x"}, {"id": 1, "v": "new"}]
    [v1] = seal_versions(ds, [rows], route)
    apply(ds, route, v1)
    assert [c.chunk_no for c in route.sink.calls] == [0, 1]
    assert sent(route.sink) == [("2", "upsert"), ("1", "upsert")]
    assert sorted(route.sink.rows, key=lambda r: r["id"]) == [{"id": 1, "v": "new"}, {"id": 2, "v": "x"}]


def test_replace_partition_sends_a_chunk_left_with_nothing_as_it_holds_the_start(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 1)
    ds = dataset()
    route = replace_route()
    [v1] = seal_versions(ds, [[{"id": 1, "v": "old"}, {"id": 1, "v": "new"}]], route)
    apply(ds, route, v1)
    assert [(len(c.records), c.is_first, c.is_last) for c in route.sink.calls] == [
        (0, True, False),
        (1, False, True),
    ]


def test_replace_partition_with_an_empty_slice_clears_it():
    ds = dataset(fc.strategy.snapshot_diff(), allow_empty=True)
    route = replace_route()
    v1, v2 = seal_versions(ds, [ROWS, []], route)
    apply(ds, route, v2)
    last = route.sink.calls[-1]
    assert (last.version_id, list(last.records), last.is_first, last.is_last) == (v2, [], True, True)
    assert route.sink.rows == [] and status("r", v2) == "applied" and status("r", v1) == "applied"


class Strict(BaseModel):
    id: int
    n: int


def test_a_version_with_no_records_goes_through_without_a_sink_call():
    ds = dataset(fc.strategy.snapshot_diff(), schema=Strict)
    route = route_to()
    [v1] = seal_versions(
        ds, [[{"id": 1, "n": "not a number"}]], route
    )  # sealed so that the key is remembered
    apply(ds, route, v1)
    assert route.sink.calls == []
    assert statuses("r", v1) == ["running", "applied"]


def test_move_reaches_the_sink_as_moved_from():
    seen = []

    def spy(r, c):
        seen.append(r.moved_from)
        return dict(r)

    ds = dataset(fc.strategy.by_stamp("u"), keep_state=True, partition=fc.partition.by_fields("s"))
    route = route_to(map=spy)
    seal(ds, [{"id": 1, "s": "a", "u": T0}], route, run="r1", partition="*")
    v2 = seal(ds, [{"id": 1, "s": "b", "u": T1}], route, run="r2", partition="*")[0].version_id
    apply(ds, route, v2)
    assert route.sink.calls[-1].records[0].moved_from == "s=a"
    assert seen == ["s=a"]


def test_moved_from_also_comes_from_the_record():
    ds = dataset()
    route = route_to()
    v1 = "moved"
    table = records_to_table([Record("1", "upsert", {"id": 1}, row_hash({"id": 1}), moved_from="s=old")])
    DEPS.spool.put_chunk(v1, 0, "data", table)
    DEPS.store.versions.stage(VersionDraft(v1, ds.name, "_", "r1", True, 1, 0, "h", None, None, "sha"))
    DEPS.store.versions.seal([v1], [route.name])
    apply(ds, route, v1)
    assert route.sink.calls[0].records[0].moved_from == "s=old"


def quarantine_statuses():
    rows = QUARANTINE._d.quarantine.values()
    return sorted((row.key, row.status) for row in rows)


def stated(**kw):
    return dataset(fc.strategy.snapshot_diff(), **kw)


def test_newer_version_supersedes_quarantined_record():  # H10
    ds = stated(keep_state=True)
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [ROWS[0], {"id": 2, "v": "B"}, ROWS[2]]], route)
    route.sink.reject("2", "bad")
    route.sink.reject("3", "bad")
    apply(ds, route, v1)
    assert {row.key for row in QUARANTINE.list_open()} == {"2", "3"}
    apply(ds, route, v2)
    assert quarantine_statuses() == [("2", "superseded"), ("3", "open")]  # 3 is not in the second version


def test_a_rejection_again_does_not_supersede():
    ds = stated(keep_state=True)
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 2, "v": "B"}, *ROWS[:1], *ROWS[2:]]], route)
    route.sink.reject("2", "bad")
    apply(ds, route, v1)
    route.sink.reject("2", "worse")
    apply(ds, route, v2)
    assert [(row.version_id, row.status) for row in QUARANTINE.list_open()] == [(v1, "open"), (v2, "open")]


def test_without_keep_state_nothing_is_superseded():
    ds = stated()
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [ROWS[0], {"id": 2, "v": "B"}, ROWS[2]]], route)
    route.sink.reject("2", "bad")
    apply(ds, route, v1)
    apply(ds, route, v2)
    assert quarantine_statuses() == [("2", "open")]


def test_budget_deferral_sleeps_durably_and_does_not_fail():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.fail_next(BudgetDeferred(timedelta(hours=8), "daily"))
    route.sink.fail_next(BudgetDeferred(timedelta(hours=1), "daily"))
    start = ENGINE.clock.now()
    apply(ds, route, v1)
    assert ENGINE.clock.now() - start >= timedelta(hours=9)
    assert statuses("r", v1) == ["running", "applied"] and len(route.sink.calls) == 1


def test_a_short_budget_wait_that_escapes_is_retried_by_the_engine():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.fail_next(BudgetDeferred(timedelta(seconds=5), "minute"))
    apply(ds, route, v1)
    assert statuses("r", v1) == ["running", "applied"] and len(route.sink.calls) == 1


def test_step_results_stay_picklable_with_a_replay_after_each_sleep(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 2)
    ds = dataset()
    route = route_to(mode="replace_partition")
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.fail_next(BudgetDeferred(timedelta(minutes=5), "daily"))
    apply(ds, route, v1)
    assert [c.chunk_no for c in route.sink.calls] == [0, 1] and status("r", v1) == "applied"


def test_a_version_that_is_not_sealed_fails_the_task():
    ds = dataset()
    route = route_to()
    ds.source.load(ROWS)
    [v1] = run_extract(ds, "r1", "_", routes=[route], stage_only=True, deps=DEPS)
    ref = apply(ds, route, v1.version_id)
    assert ENGINE.status(ref) == "failed" and route.sink.calls == []


# Replacing a slice must not lose records.


def test_a_slice_emptied_by_moves_is_cleared():
    ds = dataset(fc.strategy.snapshot_diff(), keep_state=True, partition=fc.partition.by_fields("s"))
    route = replace_route()
    v1 = seal(ds, [{"id": 1, "s": "a"}], route, run="r1", partition="s=a")[0].version_id
    apply(ds, route, v1)
    ds.source.load([{"id": 1, "s": "b"}])
    staged = [
        version
        for partition in ("s=a", "s=b")
        for version in run_extract(ds, "r2", partition, routes=[route], stage_only=True, deps=DEPS)
    ]
    for version in finalize_versions(ds, staged, [route], DEPS):
        apply(ds, route, version.version_id)
    calls = route.sink.calls[1:]
    assert [(c.partition, len(c.records), c.is_first, c.is_last) for c in calls] == [
        ("s=a", 0, True, True),
        ("s=b", 1, True, True),
    ]
    assert route.sink.rows == [{"id": 1, "s": "b"}]


def test_a_row_turned_down_at_extraction_fails_a_replace_and_a_newer_version_recovers():
    ds = dataset(fc.strategy.snapshot_diff(), schema=Strict)
    route = replace_route()
    v1, v2, v3 = seal_versions(
        ds,
        [
            [{"id": 1, "n": 1}, {"id": 2, "n": 2}],
            [{"id": 1, "n": 9}, {"id": 2, "n": "bad"}],
            [{"id": 1, "n": 9}, {"id": 2, "n": 3}],
        ],
        route,
    )
    apply(ds, route, v1)
    apply(ds, route, v2)
    assert status("r", v2) == "failed" and "1 records" in LEDGER.get("r", v2).last_error
    assert "first keys: 2" in LEDGER.get("r", v2).last_error
    assert route.sink.rows == [{"id": 1, "n": 1}, {"id": 2, "n": 2}]  # nothing was dropped
    apply(ds, route, v3)
    assert status("r", v3) == "pending"  # the stream is blocked
    LEDGER.transition("r", v2, "skipped", actor="op")
    apply(ds, route, v3)
    assert route.sink.rows == [{"id": 1, "n": 9}, {"id": 2, "n": 3}]


def test_a_key_never_delivered_does_not_fail_a_replace():
    ds = dataset(fc.strategy.snapshot_diff(), schema=Strict)
    route = replace_route()
    [v1] = seal_versions(ds, [[{"id": 1, "n": 1}, {"id": 2, "n": "bad"}]], route)
    apply(ds, route, v1)
    assert status("r", v1) == "applied" and route.sink.rows == [{"id": 1, "n": 1}]
    assert [q.key for q in QUARANTINE.list_open()] == ["2"]
    v2 = seal(ds, [{"id": 1, "n": 1}, {"id": 2, "n": 2}], route, run="r2")[0].version_id
    apply(ds, route, v2)  # the row is fixed, and arrives
    assert QUARANTINE.list_open() == []
    assert route.sink.rows == [{"id": 1, "n": 1}, {"id": 2, "n": 2}]


def test_a_slice_of_only_keys_never_delivered_replaces_with_nothing():
    ds = dataset(fc.strategy.snapshot_diff(), schema=Strict)
    route = replace_route()
    [v1] = seal_versions(ds, [[{"id": 1, "n": "bad"}]], route)
    apply(ds, route, v1)
    assert status("r", v1) == "applied" and route.sink.rows == []
    assert [(list(c.records), c.is_first, c.is_last) for c in route.sink.calls] == [([], True, True)]


def test_a_key_never_delivered_does_not_keep_a_slice_from_being_cleared():
    ds = dataset(fc.strategy.snapshot_diff(), schema=Strict, allow_empty=True)
    route = replace_route()
    v1, v2 = seal_versions(ds, [[{"id": 1, "n": 1}], [{"id": 2, "n": "bad"}]], route)
    apply(ds, route, v2)
    assert status("r", v1) == status("r", v2) == "applied" and route.sink.rows == []


def test_a_row_turned_down_at_extraction_does_not_fail_an_upsert_route():
    ds = dataset(fc.strategy.snapshot_diff(), schema=Strict)
    route = route_to()
    v1, v2 = seal_versions(
        ds, [[{"id": 1, "n": 1}, {"id": 2, "n": 2}], [{"id": 1, "n": 9}, {"id": 2, "n": "bad"}]], route
    )
    apply(ds, route, v2)
    assert status("r", v1) == status("r", v2) == "applied"
    assert route.sink.rows == [{"id": 1, "n": 9}, {"id": 2, "n": 2}]


def test_a_map_error_fails_a_replace():
    def boom(r, c):
        if r["id"] == 2:
            raise ValueError("no price")
        return dict(r)

    ds = dataset()
    route = route_to(map=boom, mode="replace_partition")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and "no price" in LEDGER.get("r", v1).last_error
    assert route.sink.calls == [] and QUARANTINE.list_open() == []


def test_a_sink_rejection_fails_a_replace():
    ds = dataset()
    route = replace_route()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.reject("2", "bad sku")
    apply(ds, route, v1)
    error = LEDGER.get("r", v1).last_error
    assert status("r", v1) == "failed" and "bad sku" in error
    assert "partial" in error and "already applied" in error and "would drop" not in error
    assert QUARANTINE.list_open() == []


# Any failure ends in a ledger status, counted per delivery.


def test_unclassified_errors_retry_and_then_fail_the_delivery():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    for _ in range(6):
        route.sink.fail_next(KeyError("bug"))
    ref = apply(ds, route, v1)
    delivery = LEDGER.get("r", v1)
    assert (ENGINE.status(ref), delivery.status, delivery.attempts) == ("succeeded", "failed", 6)
    assert "bug" in delivery.last_error


def test_an_unclassified_error_can_recover():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    route.sink.fail_next(KeyError("bug"))
    apply(ds, route, v1)
    assert status("r", v1) == "applied" and LEDGER.get("r", v1).attempts == 2


def test_short_budget_waits_end_in_a_sleep_on_the_last_attempt():
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    for _ in range(6):
        route.sink.fail_next(BudgetDeferred(timedelta(seconds=5), "minute"))
    ref = apply(ds, route, v1)
    assert ENGINE.status(ref) == "succeeded" and statuses("r", v1) == ["running", "applied"]
    assert len(route.sink.calls) == 1


@pytest.mark.parametrize("key", ["id", ("id", "v")])
def test_a_batch_map_giving_a_null_key_fails_the_version(key):
    @fc.batch_map
    def lose_keys(table, ctx):
        index = table.column_names.index("_fc_key")
        return table.set_column(index, "_fc_key", pa.nulls(table.num_rows, pa.string()))

    ds = dataset(key=key)
    route = route_to(map=lose_keys)
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and route.sink.calls == []


def test_attempts_count_per_delivery_not_per_task():
    ds = dataset(fc.strategy.snapshot_diff())
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 1, "v": "z"}, *ROWS[1:]]], route)
    for _ in range(5):
        route.sink.fail_next(TransientError("timeout"))
    real_apply = route.sink.apply

    def apply_then_fail(ctx, batch):
        result = real_apply(ctx, batch)
        if batch.version_id == v1:
            route.sink.fail_next(TransientError("blip on v2"))
        return result

    route.sink.apply = apply_then_fail
    ref = apply(ds, route, v2)  # its sixth attempt, the engine's last, meets the first error of v2
    assert ENGINE.status(ref) == "succeeded"
    assert (status("r", v1), LEDGER.get("r", v1).attempts) == ("applied", 6)
    assert (status("r", v2), LEDGER.get("r", v2).attempts) == ("applied", 2)


def test_a_delivery_on_the_engines_last_attempt_still_gets_all_its_tries():
    ds = dataset(fc.strategy.snapshot_diff())
    route = route_to()
    v1, v2 = seal_versions(ds, [ROWS, [{"id": 1, "v": "z"}, *ROWS[1:]]], route)
    for _ in range(5):
        route.sink.fail_next(TransientError("timeout"))
    real_apply = route.sink.apply

    def always_failing_v2(ctx, batch):
        if batch.version_id == v2:
            raise TransientError("v2 down")
        return real_apply(ctx, batch)

    route.sink.apply = always_failing_v2
    ref = apply(ds, route, v2)
    delivery = LEDGER.get("r", v2)
    assert (ENGINE.status(ref), delivery.status, delivery.attempts) == ("succeeded", "failed", 6)
    assert status("r", v1) == "applied"


def test_the_limit_comes_from_the_deps_retry_policy():
    global DEPS
    DEPS = replace(DEPS, retry=RetryPolicy(max_attempts=2))
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    for _ in range(2):
        route.sink.fail_next(TransientError("timeout"))
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and LEDGER.get("r", v1).attempts == 2


def test_an_operator_retry_gets_a_fresh_set_of_tries():
    global DEPS
    DEPS = replace(DEPS, retry=RetryPolicy(max_attempts=2))
    ds = dataset()
    route = route_to()
    [v1] = seal_versions(ds, [ROWS], route)
    for _ in range(3):
        route.sink.fail_next(TransientError("timeout"))
    apply(ds, route, v1)
    LEDGER.transition("r", v1, "pending", actor="op")
    apply(ds, route, v1)
    assert status("r", v1) == "applied"


# A shared route.


def two_datasets():
    return stated(name="a", keep_state=True), stated(name="b", keep_state=True)


def test_supersede_stays_within_the_dataset():
    a, b = two_datasets()
    route = route_to()
    [va] = seal_versions(a, [ROWS], route)
    route.sink.reject("2", "bad")
    apply(a, route, va)
    vb = seal(b, [{"id": 2, "v": "other"}], route, run="rb")[0].version_id
    apply(b, route, vb)
    assert [(q.key, q.version_id) for q in QUARANTINE.list_open("r")] == [("2", va)]


def test_supersede_leaves_a_key_open_in_two_datasets_rather_than_close_the_other():
    a, b = two_datasets()
    route = route_to()
    va1, va2 = seal_versions(a, [ROWS, [ROWS[0], {"id": 2, "v": "B"}, ROWS[2]]], route)
    route.sink.reject("2", "bad")
    apply(a, route, va1)
    vb = seal(b, [{"id": 2, "v": "other"}], route, run="rb")[0].version_id
    route.sink.reject("2", "bad")
    apply(b, route, vb)
    apply(a, route, va2)
    assert {q.version_id for q in QUARANTINE.list_open("r")} == {va1, vb}


class ConfirmLaterSink(Sink):
    def apply(self, ctx, batch):
        return ApplyResult(applied=0, pending_confirmation="token")


def test_a_confirm_later_result_fails_the_delivery():
    ds = dataset()
    route = fc.to(ConfirmLaterSink("r"), name="r")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and "M2" in LEDGER.get("r", v1).last_error


def broken_spool(kind):
    spool_: Any = DEPS.spool
    real = spool_.get_chunk
    failures = []

    def get_chunk(version_id, chunk_no, wanted: Any = "data"):
        if wanted == kind and (limit := failures[0] if failures else None) != 0:
            if limit is not None:
                failures[0] -= 1
            raise OSError("spool down")
        return real(version_id, chunk_no, wanted)

    spool_.get_chunk = get_chunk
    return failures


def test_an_error_while_planning_counts_as_a_try_and_fails_the_delivery():
    ds = dataset()
    route = replace_route()
    [v1] = seal_versions(ds, [ROWS], route)
    broken_spool("snapshot")
    ref = apply(ds, route, v1)
    delivery = LEDGER.get("r", v1)
    assert (ENGINE.status(ref), delivery.status, delivery.attempts) == ("succeeded", "failed", 6)
    assert "spool down" in delivery.last_error and route.sink.calls == []


def test_an_error_while_planning_can_recover():
    ds = dataset()
    route = replace_route()
    [v1] = seal_versions(ds, [ROWS], route)
    broken_spool("snapshot").append(1)
    apply(ds, route, v1)
    delivery = LEDGER.get("r", v1)
    assert (delivery.status, delivery.attempts) == ("applied", 2)
    assert statuses("r", v1) == ["running", "pending", "running", "applied"]


def moved_and_turned_down(first_rows, second_rows, route):
    """Deliver `first_rows`, all in `s=a`, then seal a run over both slices. Return the sealed versions by partition."""
    ds = dataset(
        fc.strategy.snapshot_diff(),
        schema=StrictInSlice,
        keep_state=True,
        partition=fc.partition.by_fields("s"),
    )
    v1 = seal(ds, first_rows, route, run="r1", partition="s=a")[0].version_id
    apply(ds, route, v1)
    ds.source.load(second_rows)
    staged = [
        version
        for partition in ("s=a", "s=b")
        for version in run_extract(ds, "r2", partition, routes=[route], stage_only=True, deps=DEPS)
    ]
    sealed = finalize_versions(ds, staged, [route], DEPS)
    return ds, {version.partition: version.version_id for version in sealed}


class StrictInSlice(BaseModel):
    id: int
    n: int
    s: str


def test_a_delivered_key_that_moved_and_was_turned_down_fails_the_clear():
    route = replace_route()
    live = [{"id": 1, "s": "a", "n": 1}]
    ds, versions = moved_and_turned_down(live, [{"id": 1, "s": "b", "n": "bad"}], route)
    apply(ds, route, versions["s=a"])
    assert status("r", versions["s=a"]) == "failed"
    assert "first keys: 1" in LEDGER.get("r", versions["s=a"]).last_error
    assert route.sink.rows == live


def test_a_delivered_key_that_moved_and_was_turned_down_fails_the_replace_of_the_rest():
    route = replace_route()
    live = [{"id": 1, "s": "a", "n": 1}, {"id": 2, "s": "a", "n": 2}]
    second = [{"id": 1, "s": "b", "n": "bad"}, {"id": 2, "s": "a", "n": 3}]
    ds, versions = moved_and_turned_down(live, second, route)
    apply(ds, route, versions["s=a"])
    assert status("r", versions["s=a"]) == "failed" and route.sink.rows == live


def test_a_map_error_in_a_later_chunk_fails_a_replace_before_anything_is_sent(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 2)
    plain = replace_route()
    ds = dataset(fc.strategy.snapshot_diff())
    rows = [{"id": i} for i in range(1, 5)]
    [v1] = seal_versions(ds, [rows], plain)
    apply(ds, plain, v1)
    assert len(plain.sink.calls) == 2

    def boom(r, c):
        if r["id"] == 4:
            raise ValueError("no price")
        return dict(r)

    route = fc.to(plain.sink, name="r", map=boom, mode="replace_partition")
    v2 = seal(ds, [{"id": i, "v": 1} for i in range(1, 5)], route, run="r2")[0].version_id
    apply(ds, route, v2)
    assert status("r", v2) == "failed" and "no price" in LEDGER.get("r", v2).last_error
    assert len(plain.sink.calls) == 2 and plain.sink.rows == rows


def test_a_batch_map_error_in_a_later_chunk_fails_a_replace_before_anything_is_sent(monkeypatch):
    monkeypatch.setattr(spool, "CHUNK_ROWS", 1)

    @fc.batch_map
    def boom(table, ctx):
        if table.column("id").to_pylist() == [2]:
            raise ValueError("bad batch")
        return table

    ds = dataset()
    route = route_to(map=boom, mode="replace_partition")
    [v1] = seal_versions(ds, [ROWS], route)
    apply(ds, route, v1)
    assert status("r", v1) == "failed" and route.sink.calls == []


class _LastAttempt:
    """Just enough of a task context on its last attempt for `_step_durably`."""

    def __init__(self, attempt: int) -> None:
        self.attempt = attempt
        self.sleeps: list[timedelta] = []

    def step(self, name: str, fn: Any) -> Any:
        return fn()

    def sleep(self, wait: timedelta) -> None:
        self.sleeps.append(wait)


def test_a_step_that_keeps_failing_on_the_last_attempt_ends_instead_of_spinning():  # Stage Z review 4
    from feather_connect.runtime.apply import _step_durably  # pyright: ignore[reportPrivateUsage]

    policy = RetryPolicy(max_attempts=3)
    ctx = _LastAttempt(attempt=3)
    calls: list[int] = []

    def down() -> None:
        calls.append(1)
        raise TransientError("daily cap", retry_after=7200.0)

    with pytest.raises(PermanentError, match="still failing after 3 more tries"):
        _step_durably(ctx, "chunk:v1:0", down, policy)  # pyright: ignore[reportArgumentType]
    assert (
        len(calls) == 3 and ctx.sleeps == [timedelta(seconds=7200)] * 2
    )  # the server's wait, not the backoff

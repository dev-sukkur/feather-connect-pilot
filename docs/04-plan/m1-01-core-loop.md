# M1 Plan 1: Core loop on LocalEngine

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The `feather_connect` SDK and runtime, running in-process with no database, so the design's §5.7 flow test passes verbatim.

**Architecture:** Flow authors use a small public API (`fc.Dataset`, `fc.Flow`, `fc.to`). The runtime turns a run into `plan`, `extract`, `seal_run` and `apply` tasks, written against three ports: `Engine`, `Store` and `Spool`. This plan implements the in-memory side of each port. Plan 2 adds Postgres and DBOS behind the same ports.

**Tech Stack:** Python ≥ 3.12, uv workspace, pyarrow, pydantic 2, pytest, hypothesis, ruff, pyright.

**Spec:** [design v0.2](../02-design/2026-10-30-feather-connect-design.md) §4, §5, §6.1–6.6, §6.8, §7.2, §8.1, §8.5. Read §4.2 (invariants) before you start.

## Global Constraints

- Python ≥ 3.12. The SDK is fully typed: pyright strict on `packages/feather-connect/src`. ruff for lint and format.
- Coverage ≥ 85% for the SDK core, enforced in CI.
- `feather_connect` base dependencies are `pyarrow` and `pydantic` only. No Postgres or DBOS imports in this plan.
- Engine task arguments carry ids only, never data, ≤ 64 KB (I8).
- **Replay rule:** anything in a task body that reads the clock, a database or the network goes through `ctx.step`, and step names are stable across replays.
- A step that has succeeded is never run again in the same task, whether after a retry or a replay.
- Sealed versions are immutable (I1). A route applies a partition's versions in seal order (I2). A delivery reaches `applied` at most once (I3).
- Flow authors import only `feather_connect as fc` and connector modules.
- Single-tenant for now: every store is bound to tenant `default`.

Paths are relative to `packages/feather-connect/` unless a task says otherwise. Run commands from the repository root.

## Review Focus

These are inputs the design implies but doesn't spell out. Each has a test in the task named.

1. A source returns the same key twice in one read, as overlapping API pages do: the last row wins and the version holds one record (Task 8).
2. A record has a null or missing key or partition field: that record is quarantined with a reason, and the rest of the read proceeds (Task 8).
3. A complete read returns no rows, as a transient API fault can: the extract fails with a clear message instead of deleting the whole slice, unless `allow_empty=True` (Task 8).
4. Payloads hold `Decimal`, timezone-aware datetimes, nested lists, or a column with mixed types: hashing is stable, and chunks round-trip without losing values (Tasks 2 and 7).
5. A transform raises on one record: that record is quarantined with the error, and the rest of the version applies, unless `on_record_error="fail"` (Task 10).

---

### Task 1: Workspace, package skeleton and CI

**Files:**
- Create: root `pyproject.toml`, `.gitignore`, `LICENSE` (MIT, copyright "Feather Connect contributors"), `.github/workflows/ci.yml`
- Create: `pyproject.toml`, `src/feather_connect/__init__.py`, `src/feather_connect/py.typed`, `src/feather_connect/clock.py`, `src/feather_connect/ids.py`
- Modify: root `README.md` (status line: "Under construction: milestone M1")
- Test: `tests/test_smoke.py`

**Interfaces:**
- Produces: `fc.__version__ = "0.1.0"`; `Clock` protocol with `now() -> datetime` (timezone-aware UTC), `SystemClock`, `FrozenClock(start)` with `advance(timedelta)`; `new_id() -> str` (26-character ULID, time-ordered); `stable_id(*parts: str) -> str` (same parts, same id).

- [ ] **Step 1: Write the failing tests**

```python
import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.ids import new_id, stable_id

def test_version():
    assert fc.__version__ == "0.1.0"

def test_ids():
    assert len(new_id()) == 26 and new_id() != new_id()
    assert stable_id("run", "ds", "p") == stable_id("run", "ds", "p") != stable_id("run", "ds", "q")

def test_frozen_clock_advances():
    from datetime import datetime, timedelta, timezone
    c = FrozenClock(datetime(2026, 10, 18, tzinfo=timezone.utc))
    c.advance(timedelta(hours=1))
    assert c.now().hour == 1
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/test_smoke.py -q`. Expected: FAIL, with an error that the workspace or module doesn't exist.

- [ ] **Step 3: Create the workspace.** The root `pyproject.toml` has `[tool.uv] package = false`, and `[tool.uv.workspace]` with `members = ["packages/*", "packages/connectors/*"]` and `exclude = ["packages/connectors"]`, because that folder holds packages but isn't one. Add `.python-version` with `3.13`. The dev group is `pytest pytest-cov pytest-timeout hypothesis ruff pyright pyarrow-stubs`. Ruff: `line-length = 110`, `target-version = "py312"`, `extend-exclude = ["spikes"]`. Pyright: `strict = ["packages/feather-connect/src"]`. Pytest: `addopts = "--import-mode=importlib"`, `timeout = 120`. The package uses hatchling, with dependencies `pyarrow` and `pydantic>=2`. `stable_id` is the first 26 characters of the base32 of a blake2b hash over the parts.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: 3 passed.

- [ ] **Step 5: Add CI and check the gates.** `.github/workflows/ci.yml` runs on pull requests, on `ubuntu-latest` and `windows-latest`: `uv sync`, `uv run ruff check .`, `uv run ruff format --check .`, `uv run pyright`, and `uv run pytest packages/feather-connect/tests --cov=feather_connect --cov-fail-under=85`. Run the same four commands locally. Expected: all clean.

- [ ] **Step 6: Commit.** `git add . && git commit -m "feat: add workspace and feather-connect skeleton"`

### Task 2: Records and hashing

**Files:**
- Create: `src/feather_connect/core/__init__.py`, `src/feather_connect/core/records.py`, `src/feather_connect/core/errors.py`
- Test: `tests/core/test_records.py`

**Interfaces:**
- Produces in `core/records.py`: `Op = Literal["upsert", "delete"]`; `canonical_json(value: Any) -> bytes`; `row_hash(payload: Mapping[str, Any]) -> int` (unsigned 64-bit); `make_key(payload, key: str | tuple[str, ...]) -> str`; `decode_key(key: str, spec: str | tuple[str, ...]) -> Any`; `Record(key: str, op: Op, payload: Mapping[str, Any], row_hash: int, moved_from: str | None = None)`, a frozen dataclass that implements `Mapping[str, Any]` over `payload`.
- Produces in `core/errors.py`: `MissingKeyError`, `PartitionError`, `TransientError(message, retry_after: float | None = None)`, `PermanentError`, `BudgetDeferred(wait: timedelta, budget: str)`, `RecordRejected(message)`. `PermanentError` is defined here and re-exported by `engine/port.py`.

- [ ] **Step 1: Write the failing tests**

```python
def test_row_hash_ignores_key_order_and_detects_change():
    assert row_hash({"a": 1, "b": 2}) == row_hash({"b": 2, "a": 1})
    assert row_hash({"a": 1}) != row_hash({"a": 2})
    assert 0 <= row_hash({"a": 1}) < 2**64

def test_canonical_json_handles_non_json_types():
    v = {"d": Decimal("1.10"), "t": datetime(2026, 10, 18, 5, tzinfo=timezone.utc),
         "day": date(2026, 10, 18), "b": b"\x01", "n": [1, {"x": None}]}
    assert canonical_json(v) == (b'{"b":"AQ==","d":"1.10","day":"2026-10-18",'
                                 b'"n":[1,{"x":null}],"t":"2026-10-18T05:00:00+00:00"}')

def test_equal_instants_hash_the_same():
    ist = timezone(timedelta(hours=5, minutes=30))
    assert row_hash({"t": datetime(2026, 10, 18, 5, tzinfo=timezone.utc)}) == \
           row_hash({"t": datetime(2026, 10, 18, 10, 30, tzinfo=ist)})

def test_make_and_decode_key():
    assert make_key({"id": 7}, "id") == "7"
    assert make_key({"m": "M2", "p": "RET"}, ("m", "p")) == '["M2","RET"]'
    assert decode_key('["M2","RET"]', ("m", "p")) == ("M2", "RET")
    assert decode_key("7", "id") == "7"

@pytest.mark.parametrize("payload", [{}, {"id": None}])
def test_make_key_rejects_missing(payload):
    with pytest.raises(MissingKeyError):
        make_key(payload, "id")

def test_record_reads_like_a_mapping():
    r = Record("k", "upsert", {"material": "M1"}, 1)
    assert r["material"] == "M1" and list(r) == ["material"]

@given(st.recursive(st.none() | st.booleans() | st.integers() | st.text(),
                    lambda c: st.lists(c) | st.dictionaries(st.text(), c)))
def test_canonical_json_is_deterministic(v):
    assert canonical_json({"v": v}) == canonical_json({"v": v})
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/core/test_records.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `core/records.py` and `core/errors.py`.** `canonical_json` uses `json.dumps(sort_keys=True, separators=(",", ":"))` with a default handler: `Decimal` as `str`, aware datetime converted to UTC ISO-8601, naive datetime as ISO-8601 unchanged, `date` as ISO-8601, `bytes` as base64. `row_hash` is `hashlib.blake2b(canonical_json(payload), digest_size=8)` as a big-endian unsigned int.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add records, hashing and error types"`

### Task 3: Partition schemes

**Files:**
- Create: `src/feather_connect/core/partitions.py`
- Test: `tests/core/test_partitions.py`

**Interfaces:**
- Consumes: `PartitionError` (Task 2).
- Produces: protocol `PartitionScheme` with `key_of(payload) -> str`, `label_of(key: str) -> str`, `bounds(key: str) -> Mapping[str, Bound]`, `recent(now: datetime) -> list[str] | None` (the default window, or `None` when the scheme can't enumerate its own partitions). `Bound(low: Any, high: Any)` is half-open; `low == high` means equality. Factories: `none()`, `by_day(field, tz)`, `by_month(field, tz)`, `by_fields(*parts: str | PartitionScheme)`, `by_bucket(field, n)`.

- [ ] **Step 1: Write the failing tests**

```python
IST = "Asia/Kolkata"

def test_none_has_one_partition():
    assert none().key_of({"x": 1}) == "_" and none().recent(NOW) == ["_"]

def test_by_day_uses_the_explicit_timezone():
    s = by_day("date", tz=IST)
    assert s.key_of({"date": "2026-10-17T20:00:00Z"}) == "date=2026-10-18"
    assert s.key_of({"date": date(2026, 10, 18)}) == "date=2026-10-18"
    assert s.key_of({"date": datetime(2026, 10, 18, 1, 0)}) == "date=2026-10-18"  # naive means scheme tz

def test_by_fields_composes_and_labels_in_business_words():
    s = by_fields("store_id", by_day("date", tz=IST))
    key = s.key_of({"store_id": 1509, "date": "2026-10-18"})
    assert key == "store_id=1509/date=2026-10-18"
    assert s.label_of(key) == "Store 1509 · 18 Oct 2026"
    assert s.recent(NOW) is None  # stores can't be enumerated

def test_by_bucket_is_stable():
    s = by_bucket("id", 8)
    assert s.key_of({"id": "A"}) == s.key_of({"id": "A"})
    assert s.key_of({"id": "A"}).startswith("bucket=")
    assert s.recent(NOW) == [f"bucket={i}" for i in range(8)]

def test_by_day_recent_is_today_and_yesterday_in_scheme_tz():
    now = datetime(2026, 10, 17, 20, 0, tzinfo=timezone.utc)  # already 18 Oct in India
    assert by_day("date", tz=IST).recent(now) == ["date=2026-10-17", "date=2026-10-18"]

def test_bounds_for_pushdown():
    b = by_day("date", tz=IST).bounds("date=2026-10-18")["date"]
    assert b.low == date(2026, 10, 18) and b.high == date(2026, 10, 19)

@pytest.mark.parametrize("payload", [{}, {"date": None}, {"date": "not a date"}])
def test_bad_partition_field_raises(payload):
    with pytest.raises(PartitionError):
        by_day("date", tz=IST).key_of(payload)
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/core/test_partitions.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `core/partitions.py`.** Use `zoneinfo.ZoneInfo`. `label_of` title-cases the field name after dropping a trailing `_id`, and renders days as `18 Oct 2026`. Buckets hash the canonical JSON of the field with blake2b, modulo n.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add partition schemes"`

### Task 4: Connector types and the in-memory connector

**Files:**
- Create: `src/feather_connect/core/connector.py`, `src/feather_connect/core/context.py`, `src/feather_connect/connectors/memory.py` (no `connectors/__init__.py`: it's a namespace package)
- Test: `tests/connectors/test_memory.py`

**Interfaces:**
- Consumes: `Record`, `Op`, `PartitionScheme`.
- Produces in `core/connector.py`: `ConnectorSpec(name, config=None, strategies=frozenset(), modes=frozenset({"upsert"}), deletes="none", cost={})`; `CheckResult(ok, message="")`; `StreamSchema(name, fields)`; `ReadRequest(partition, scheme, since=None, stamp_field=None, resume=None)` with `contains(payload) -> bool`; `Batch = pa.RecordBatch | list[dict[str, Any]]`; `ApplyRecord(key, op, payload, moved_from=None)`; `RouteMode = Literal["upsert", "append", "replace_partition", "side_effect"]`; `ApplyBatch(route, version_id, chunk_no, partition, mode, records, idempotency_key, is_first, is_last)`; `Rejection(key, error_class, message)`; `ApplyResult(applied, rejected=(), pending_confirmation=None)`; `Deleted(payload)`, which a push source returns for an explicit delete; protocols `Source`, `PushSource`, `Sink` exactly as design §8.1, without `confirm` for now. Sources and sinks expose a `connection: str` attribute.
- Produces in `core/context.py`: `Context(config, log, now, tz, budget=None, http=None)`, with `secret(name) -> str` and `checkpoint(value=None) -> str | None`.
- Produces in `connectors/memory.py`: `Source(connection="memory", rows=())`, with `load(rows)`; `Sink(connection="memory")`, with `rows`, `deleted_keys`, `calls`, `fail_next(exc)` and `reject(key, message)`.

- [ ] **Step 1: Write the failing tests**

```python
def batch(chunk=0, mode="upsert", records=(), version="v1", partition="_", first=True, last=True):
    return ApplyBatch("r", version, chunk, partition, mode, list(records), f"r:{version}:{chunk}", first, last)

UP = ApplyRecord("k1", "upsert", {"a": 1})

def test_source_filters_by_partition_and_since():
    s = memory.Source(); s.load([{"id": 1, "date": "2026-10-18", "u": 5}, {"id": 2, "date": "2026-10-19", "u": 9}])
    req = ReadRequest("date=2026-10-18", by_day("date", tz="UTC"))
    assert [r["id"] for b in s.read(CTX, req) for r in b] == [1]
    req = ReadRequest("*", none(), since=6, stamp_field="u")
    assert [r["id"] for b in s.read(CTX, req) for r in b] == [2]

def test_sink_upsert_then_delete():
    k = memory.Sink()
    k.apply(CTX, batch(records=[UP])); assert k.rows == [{"a": 1}]
    k.apply(CTX, batch(version="v2", records=[ApplyRecord("k1", "delete", None)]))
    assert k.rows == [] and k.deleted_keys == ["k1"]

def test_sink_apply_is_idempotent_per_version_and_chunk():
    k = memory.Sink(); k.apply(CTX, batch(records=[UP])); k.apply(CTX, batch(records=[UP]))
    assert k.rows == [{"a": 1}]

def test_sink_append_keeps_every_record():
    k = memory.Sink()
    k.apply(CTX, batch(mode="append", records=[UP])); k.apply(CTX, batch(mode="append", version="v2", records=[UP]))
    assert k.rows == [{"a": 1}, {"a": 1}]

def test_sink_replace_partition_swaps_the_slice():
    k = memory.Sink()
    k.apply(CTX, batch(mode="replace_partition", partition="p1", records=[UP]))
    k.apply(CTX, batch(mode="replace_partition", partition="p1", version="v2", records=[ApplyRecord("k2", "upsert", {"a": 2})]))
    assert k.rows == [{"a": 2}]

def test_sink_failure_and_rejection_hooks():
    k = memory.Sink(); k.fail_next(TransientError("boom"))
    with pytest.raises(TransientError): k.apply(CTX, batch(records=[UP]))
    k.reject("k1", "bad sku")
    r = k.apply(CTX, batch(records=[UP]))
    assert r.applied == 0 and r.rejected[0].message == "bad sku"
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/connectors/test_memory.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the types and the connector.** `ReadRequest.contains` is `partition == "*" or scheme.key_of(payload) == partition`. The sink's spec declares all three of `upsert`, `append` and `replace_partition`, and remembers each `idempotency_key` to skip repeats. `calls` records every call that didn't raise, repeats included. A `replace_partition` batch with `is_first=True` clears that partition's rows, and `is_last` finishes it; the whole swap happens in memory.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add connector protocols and in-memory connector"`

### Task 5: Strategies, datasets, flows and the public API

**Files:**
- Create: `src/feather_connect/core/strategies.py`, `core/dataset.py`, `core/flow.py`, `core/triggers.py`, `core/transforms.py`
- Modify: `src/feather_connect/__init__.py`
- Test: `tests/core/test_flow.py`, `tests/core/test_strategies.py`

**Interfaces:**
- Consumes: `PartitionScheme` and its factories; `ConnectorSpec`; `Source`, `Sink`; `Record`; `Context`.
- Produces in `core/strategies.py`: `Diff(upsert_keys: pa.Array, delete_keys: pa.Array)`. Protocol `Strategy` with `name: str`, `complete: bool` and `diff(new: pa.Table, old: pa.Table | None) -> Diff`, both tables having columns `_fc_key` (string) and `_fc_row_hash` (uint64). Factories `by_stamp(field, lookback="0s", deleted_field=None)`, `snapshot_diff()`, `replace()`, `push()`. `parse_duration(text) -> timedelta`.
- Produces in `core/dataset.py`: `Dataset(name, *, source, key, strategy, partition=none(), keep_state=False, priority="normal", schema=None, on_record_error="quarantine", allow_empty=False, planner=None)`. `planner` is `Callable[[datetime], list[str]] | None`.
- Produces in `core/flow.py`: `to(sink, *, map=None, mode="upsert", priority="normal", name=None) -> RouteSpec`; `RouteSpec(name, sink, map, mode, priority)`; `Flow(name, *, reads, deliver, triggers=(), owner=None, tags=(), overlap="queue", catch_up="latest")`, with `datasets: list[Dataset]`, `routes: list[RouteSpec]`, and `validate() -> list[Problem]`; `Problem(severity, code, message, path, hint)`.
- Produces in `core/triggers.py`: `cron(expr, *, tz)`, `every(interval)`, `manual()`, `on_event(dataset)`.
- Produces in `core/transforms.py`: `batch_map` decorator, `apply_map(route, record, ctx) -> list[dict[str, Any]]`.
- Produces in `feather_connect/__init__.py`: `Dataset, Flow, to, Record, Context, cron, every, manual, on_event, batch_map, strategy, partition, ConnectorSpec, CheckResult, Source, PushSource, Sink, PermanentError, TransientError, RecordRejected`.

- [ ] **Step 1: Write the failing tests**

```python
def make_flow(strategy=None, **route):
    ds = fc.Dataset("sap.price", source=memory.Source(), key=("material", "price_list"),
                    strategy=strategy or fc.strategy.snapshot_diff())
    return fc.Flow("prices", reads=ds, deliver=[fc.to(memory.Sink(connection="zakya"), **route)],
                   triggers=[fc.cron("*/15 * * * *", tz="Asia/Kolkata"), fc.manual()])

def codes(flow): return {p.code for p in flow.validate()}

def test_route_is_named_after_its_connection_unless_named():
    assert make_flow().routes[0].name == "zakya"
    assert make_flow(name="z2").routes[0].name == "z2"

def test_a_valid_flow_has_no_problems():
    assert make_flow().validate() == []

def test_validate_rejects_replace_partition_on_incomplete_strategy():
    assert "mode_needs_complete" in codes(make_flow(fc.strategy.by_stamp("u"), mode="replace_partition"))

def test_validate_rejects_mode_the_sink_does_not_support():
    assert "mode_unsupported" in codes(make_flow(mode="side_effect"))

def test_validate_rejects_duplicate_route_names():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    f = fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink(connection="a")), fc.to(memory.Sink(connection="a"))])
    assert "route_name_duplicate" in codes(f)

def test_on_event_needs_a_push_strategy():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.replace())
    f = fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())], triggers=[fc.on_event(ds)])
    assert "trigger_needs_push" in codes(f)

def test_unenumerable_partitions_need_a_planner():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.snapshot_diff(),
                    partition=fc.partition.by_fields("store_id", fc.partition.by_day("date", tz="UTC")))
    assert "planner_required" in codes(fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())]))

def test_partitioned_complete_dataset_without_state_warns_about_moves():
    ds = fc.Dataset("d", source=memory.Source(), key="id", strategy=fc.strategy.snapshot_diff(),
                    partition=fc.partition.by_day("date", tz="UTC"))
    p = {x.code: x for x in fc.Flow("f", reads=ds, deliver=[fc.to(memory.Sink())]).validate()}
    assert p["moves_need_state"].severity == "warning"

def test_cron_requires_a_timezone():
    with pytest.raises(TypeError):
        fc.cron("* * * * *")

def test_snapshot_diff_finds_changes_and_deletes():
    new = snap([("a", 1), ("b", 9), ("d", 4)]); old = snap([("a", 1), ("b", 2), ("c", 3)])
    diff = fc.strategy.snapshot_diff().diff(new, old)
    assert set(diff.upsert_keys.to_pylist()) == {"b", "d"} and diff.delete_keys.to_pylist() == ["c"]

def test_replace_upserts_everything_and_implies_deletes():
    diff = fc.strategy.replace().diff(snap([("a", 1)]), snap([("a", 1), ("c", 3)]))
    assert diff.upsert_keys.to_pylist() == ["a"] and diff.delete_keys.to_pylist() == ["c"]

def test_by_stamp_suppresses_unchanged_and_never_implies_deletes():
    s = fc.strategy.by_stamp("u", lookback="2h")
    assert s.lookback == timedelta(hours=2) and s.complete is False
    diff = s.diff(snap([("a", 1), ("b", 2)]), snap([("a", 1), ("c", 3)]))
    assert diff.upsert_keys.to_pylist() == ["b"] and len(diff.delete_keys) == 0
```

`snap(pairs)` is a test helper that builds the two-column table.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/core/test_flow.py packages/feather-connect/tests/core/test_strategies.py -q`. Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement the model.** Diffs use `pa.Table.join(..., join_type="left anti")`, on `_fc_key` for deletes and on both columns for upserts. `reads` accepts one dataset or a list. A route's name falls back to `sink.connection`. Validation is pure, with no connections opened. `fc.strategy` and `fc.partition` are the modules themselves.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add datasets, flows, strategies and public API"`

### Task 6: Engine port and LocalEngine

**Files:**
- Create: `src/feather_connect/engine/__init__.py`, `engine/port.py`, `engine/local.py`
- Test: `tests/engine/test_local_engine.py`

**Interfaces:**
- Consumes: `PermanentError` (Task 2); `Clock`, `FrozenClock` (Task 1).
- Produces in `engine/port.py`: exactly the code in design §6.8: `RetryPolicy`, `TaskCall`, `TaskContext`, `TaskFn`, `Engine`, `WorkerRuntime`, with `PermanentError` re-exported. Add `RetryPolicy.delay(failed_attempt: int, rand: Callable[[], float] = random.random) -> timedelta`, computed as `min(first_delay * 2 ** (failed_attempt - 1), max_delay)` times `0.5 + rand() / 2`.
- Produces in `engine/local.py`: `LocalEngine(clock=None)`, implementing `Engine`, with `register(tasks)`, `run_until_idle(max_tasks=10_000)`, `crash_before(step_name)` and `recover()`. Also exposes `submitted() -> list[TaskCall]` and `task_ids() -> list[str]`, in submit and start order.

- [ ] **Step 1: Write the failing tests**

```python
def test_same_idempotency_key_runs_once(): ...  # submit twice, assert the body ran once with the first args
def test_one_at_a_time_per_key_higher_priority_first():
    # queue keys A and A with priorities 0 and 2 before running; body logs start and end
    assert log == ["start:high", "end:high", "start:low", "end:low"]
def test_retry_gaps_follow_policy():
    # fails 3 times then succeeds; policy first_delay=1s, rand fixed at 1.0
    assert gaps == [1.0, 2.0, 4.0]
def test_permanent_error_is_not_retried(): ...  # attempts == 1, task status "failed"
def test_succeeded_step_is_not_rerun_on_retry():
    # body: ctx.step("a", a); raises TransientError on attempt 1
    assert calls_a == 1 and attempts == 2
def test_crash_then_recover_replays_saved_steps():
    calls = []
    def body(ctx, a):
        ctx.step("s1", lambda: calls.append("s1"))
        ctx.step("s2", lambda: calls.append("s2"))
    e = LocalEngine(); e.register({"t": body}); e.crash_before("s2")
    e.submit(TaskCall("t", {}, idempotency_key="k")); e.run_until_idle()
    assert calls == ["s1"]
    e.recover(); e.run_until_idle()
    assert calls == ["s1", "s2"]
def test_sleep_parks_the_task_and_others_run():
    # first task sleeps 1 hour, second has no sleep; the virtual clock advances only when idle
    assert order == ["a:before", "b", "a:after"] and clock_moved == timedelta(hours=1)
def test_signal_wakes_a_waiter_and_timeout_returns_none(): ...
def test_cancel_stops_a_queued_task(): ...
def test_recover_worker_returns_parked_crashed_tasks(): ...  # returns refs and re-queues them
```

Write each body in full; the comments above fix what each one asserts.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/engine/test_local_engine.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `engine/port.py` and `engine/local.py`.** Use one thread. `sleep` and `wait_signal` raise a private `_Park(BaseException)` that parks the task; when woken, the body runs again from the top, and saved steps return their results, the same replay semantics as a durable engine. Steps are saved in a dict keyed by `(task_ref, step_name)`. When the queue is empty, advance the virtual clock to the earliest wake-up. Retries wait on the virtual clock with `rand=lambda: 1.0`. A crash uses a private `_Crash(BaseException)` and marks the task `crashed`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add engine port and LocalEngine"`

### Task 7: Store ports, memory store and spool

**Files:**
- Create: `src/feather_connect/storage/__init__.py`, `storage/port.py`, `storage/memory.py`, `storage/spool.py`, `src/feather_connect/testing/__init__.py`, `testing/store_contract.py`
- Test: `tests/storage/test_memory_store.py`

**Interfaces:**
- Consumes: `Record`, `Clock`, `new_id`.
- Produces in `storage/port.py`:
  - Dataclasses `Run`, `TaskRow`, `VersionDraft(version_id, dataset, partition, run_id, complete, upserts, deletes, content_hash, schema_hash, watermark, code_sha)`, `Version` (the draft plus `status: "staged" | "sealed"`, `seal_seq: int | None`, `sealed_at`), `Delivery(route, version_id, dataset, partition, seal_seq, status, attempts, last_error, rows_applied, rows_rejected, replays, retries)` (`transition` adds 1 to `replays` on `applied → pending`, and to `retries` on `failed → pending`), `Attempt`, `StateRow(key, partition, row_hash, payload, last_version)`, `QuarantineRow(route, version_id, key, error, status)`.
  - `RunJournal`: `create_run(flow, trigger, actor, params, status="queued", reason=None) -> str`; `set_run_status(run_id, status, reason=None)`; `get_run(run_id) -> Run`; `list_runs(flow=None, status=None, limit=50, cursor=None) -> list[Run]`; `start_task(run_id, kind, ref, dataset=None, partition=None, route=None, worker=None, code_sha=None) -> str`; `finish_task(task_id, status, error=None)`; `list_tasks(run_id) -> list[TaskRow]`; `log(task_id, level, message)`; `task_logs(task_id) -> list[str]`.
  - `VersionStore`: `stage(draft) -> Version`; `seal(version_ids, routes: Sequence[str]) -> list[Version]`; `get(version_id) -> Version`; `latest_sealed(dataset, partition) -> Version | None`; `since_watermark(dataset, partition, since) -> list[Version]` (sealed versions whose stored watermark is at or after `since`, newest first); `latest_watermark(dataset) -> Any | None`; `register_slices(run_id, dataset, partitions)`; `finish_slice(run_id, dataset, partition) -> bool` (True when all are done); `staged_for_run(run_id, dataset) -> list[Version]`; `list_versions(dataset, partition=None, limit=100) -> list[Version]`.
  - `Ledger`: `oldest_open(route, partition) -> Delivery | None` (skips `applied`, `skipped` and `compensated`); `get(route, version_id) -> Delivery`; `transition(route, version_id, to, *, actor, reason=None, error=None, rows_applied=0, rows_rejected=0, code_sha=None) -> Delivery`; `attempts(route, version_id) -> list[Attempt]`; `list_deliveries(route=None, status=None, partition=None, limit=100, cursor=None) -> list[Delivery]`; `all_applied() -> bool`.
  - `RecordStateStore`: `get_many(dataset, keys) -> dict[str, StateRow]`; `put_many(dataset, rows)`; `delete_many(dataset, keys)`.
  - `QuarantineStore`: `add(route, version_id, key, error)`; `supersede(route, key, before_seal_seq) -> int`; `list_open(route=None) -> list[QuarantineRow]`.
  - `Store` dataclass with attributes `runs`, `versions`, `ledger`, `state`, `quarantine`.
  - Exceptions `VersionSealed`, `InvalidTransition`.
- Produces in `storage/spool.py`: `Spool` protocol: `put_chunk(version_id, chunk_no, kind, table) -> ChunkRef`, `get_chunk(version_id, chunk_no, kind="data") -> pa.Table`, `list_chunks(version_id, kind="data") -> list[ChunkRef]`, `delete_chunks(version_id, kind, from_chunk=0)`, `delete_version(version_id)`. `kind` is one of `data`, `snapshot`, `full`, `candidates`, `moves`. `MemorySpool`. Helpers `records_to_table(records) -> pa.Table`, `table_to_records(table) -> list[Record]`, `snapshot_table(pairs: Iterable[tuple[str, int]]) -> pa.Table`, and `CHUNK_ROWS = 50_000`.
- Produces in `storage/memory.py`: `MemoryStore(clock=None) -> Store`.
- Produces in `testing/store_contract.py`: `class StoreContract` (test methods using fixtures `store`, `spool`, and `clock`, the `FrozenClock` that both were built with), which Plan 2 reuses.

- [ ] **Step 1: Write the failing tests.** Put the tests in `StoreContract`, and in `tests/storage/test_memory_store.py` subclass it with fixtures that return `MemoryStore()` and `MemorySpool()`.

```python
def draft(vid="v1", ds="d", part="p", run="r1", complete=True, **kw):
    return VersionDraft(vid, ds, part, run, complete, kw.get("up", 1), 0, "h", None, kw.get("wm"), "sha")

class StoreContract:
    def test_seal_creates_one_pending_delivery_per_route(self, store):
        store.versions.stage(draft()); store.versions.seal(["v1"], ["a", "b"])
        assert store.ledger.get("a", "v1").status == "pending" and store.ledger.get("b", "v1").status == "pending"

    def test_sealed_version_is_immutable(self, store):  # I1
        store.versions.stage(draft()); store.versions.seal(["v1"], ["a"])
        with pytest.raises(VersionSealed):
            store.versions.stage(draft(up=99))

    def test_seal_is_idempotent(self, store):
        store.versions.stage(draft())
        first = store.versions.seal(["v1"], ["a"]); second = store.versions.seal(["v1"], ["a"])
        assert first[0].seal_seq == second[0].seal_seq and len(store.ledger.attempts("a", "v1")) == 0

    def test_oldest_open_follows_seal_order_and_a_failure_blocks(self, store):  # I2
        for v in ("v1", "v2"):
            store.versions.stage(draft(v)); store.versions.seal([v], ["a"])
        assert store.ledger.oldest_open("a", "p").version_id == "v1"
        store.ledger.transition("a", "v1", "running", actor="t"); store.ledger.transition("a", "v1", "failed", actor="t", error="x")
        assert store.ledger.oldest_open("a", "p").status == "failed"
        store.ledger.transition("a", "v1", "skipped", actor="op")
        assert store.ledger.oldest_open("a", "p").version_id == "v2"

    def test_illegal_transition_is_rejected(self, store):  # I3
        store.versions.stage(draft()); store.versions.seal(["v1"], ["a"])
        with pytest.raises(InvalidTransition):
            store.ledger.transition("a", "v1", "applied", actor="t")  # pending cannot jump to applied

    def test_every_transition_writes_an_attempt(self, store):
        store.versions.stage(draft()); store.versions.seal(["v1"], ["a"])
        store.ledger.transition("a", "v1", "running", actor="t")
        store.ledger.transition("a", "v1", "applied", actor="t", rows_applied=3, code_sha="sha")
        a = store.ledger.attempts("a", "v1")
        assert [x.to_status for x in a] == ["running", "applied"] and a[1].code_sha == "sha"
        assert store.ledger.get("a", "v1").rows_applied == 3

    def test_replay_returns_applied_to_pending(self, store):
        ...  # applied -> pending is legal, and appears in the attempts

    def test_skipped_tick_is_recorded(self, store):  # I6
        rid = store.runs.create_run("f", "cron", "scheduler", {}, status="skipped", reason="paused")
        assert store.runs.get_run(rid).reason == "paused"

    def test_finish_slice_reports_when_all_are_done(self, store):
        store.versions.register_slices("r1", "d", ["p1", "p2"])
        assert store.versions.finish_slice("r1", "d", "p1") is False
        assert store.versions.finish_slice("r1", "d", "p1") is False  # repeated finish is safe
        assert store.versions.finish_slice("r1", "d", "p2") is True

    def test_supersede_resolves_only_older_entries(self, store): ...
    def test_record_state_round_trip(self, store): ...
    def test_latest_watermark_ignores_staged_versions(self, store): ...

    def test_chunk_round_trip_keeps_types(self, spool):
        recs = [Record("k", "upsert", {"d": Decimal("1.10"), "t": T0, "n": [1, 2], "o": {"x": None}}, 7)]
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert table_to_records(spool.get_chunk("v1", 0))[0] == recs[0]

    def test_mixed_type_column_round_trips(self, spool):  # Review Focus 4
        recs = [Record("a", "upsert", {"x": 1}, 1), Record("b", "upsert", {"x": "two"}, 2)]
        spool.put_chunk("v1", 0, "data", records_to_table(recs))
        assert [r["x"] for r in table_to_records(spool.get_chunk("v1", 0))] == [1, "two"]

    def test_putting_a_chunk_twice_overwrites(self, spool): ...
    def test_delete_chunks_from_a_number_drops_the_tail(self, spool): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/storage -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement ports, memory store and spool.** The legal transitions are exactly those in design §6.5. `seal_seq` comes from one counter per store. `records_to_table` writes payload columns plus `_fc_key`, `_fc_op`, `_fc_row_hash` (uint64) and `_fc_moved_from`. A column whose values can't be unified by `pa.array` is stored as JSON text, named in schema metadata key `fc.json_columns`, and decoded in `table_to_records`. `Decimal` values are kept as Arrow decimals, with a fallback to the JSON path if precision varies.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add store ports, memory store and spool"`

### Task 8: Extraction

**Files:**
- Create: `src/feather_connect/runtime/__init__.py`, `runtime/deps.py`, `runtime/extract.py`
- Test: `tests/runtime/test_extract.py`

**Interfaces:**
- Consumes: `Dataset`, `Strategy`, `Diff`, `ReadRequest`, `Context`; `Store`; `Spool`, `records_to_table`, `snapshot_table`, `CHUNK_ROWS`; `make_key`, `row_hash`, `MissingKeyError`, `PartitionError`; `stable_id`.
- Produces in `runtime/deps.py`: `RuntimeDeps(store, spool, clock, code_sha, make_context: Callable[[str], Context])`.
- Produces in `runtime/extract.py`: `run_extract(dataset, run_id, partition, *, routes: Sequence[RouteSpec], stage_only: bool, deps) -> list[Version]`, and `finalize_versions(dataset, versions, routes, deps) -> list[Version]` for Task 9. Version ids are `stable_id(run_id, dataset.name, partition)`. `ExtractFailed(message)` is raised for version-level problems.

- [ ] **Step 1: Write the failing tests**

```python
def extract(ds, rows, run="r1", partition="_", routes=("a",), **kw):
    ds.source.load(rows)
    return run_extract(ds, run, partition, routes=route_specs(routes), stage_only=False, deps=DEPS, **kw)

def recs(v): return table_to_records(DEPS.spool.get_chunk(v.version_id, 0))

def test_snapshot_diff_detects_insert_update_and_hard_delete():  # H2
    ds = dataset(fc.strategy.snapshot_diff())
    extract(ds, [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}])
    v = extract(ds, [{"id": 1, "v": "a"}, {"id": 2, "v": "B"}, {"id": 4, "v": "d"}], run="r2")[0]
    assert v.complete and (v.upserts, v.deletes) == (2, 1)
    assert {(r.key, r.op) for r in recs(v)} == {("2", "upsert"), ("4", "upsert"), ("3", "delete")}

def test_unchanged_slice_seals_no_version():
    ds = dataset(fc.strategy.snapshot_diff()); extract(ds, ROWS)
    assert extract(ds, ROWS, run="r2") == []
    assert DEPS.store.versions.latest_sealed("d", "_").run_id == "r1"

def test_replace_delivers_the_whole_slice_and_implies_deletes():
    ds = dataset(fc.strategy.replace()); extract(ds, [{"id": 1}, {"id": 2}])
    v = extract(ds, [{"id": 1}], run="r2")[0]
    assert {(r.key, r.op) for r in recs(v)} == {("1", "upsert"), ("2", "delete")}

def test_by_stamp_lookback_reread_is_suppressed():  # H4
    ds = dataset(fc.strategy.by_stamp("u", lookback="2h"))
    extract(ds, [{"id": 1, "u": T0}, {"id": 2, "u": T0}, {"id": 3, "u": T0}], partition="*")
    v = extract(ds, [{"id": 1, "u": T0}, {"id": 2, "u": T0}, {"id": 3, "u": T1, "x": 1}], run="r2", partition="*")[0]
    assert [r.key for r in recs(v)] == ["3"] and v.complete is False

def test_by_stamp_reads_from_the_watermark_minus_lookback():
    # capture the ReadRequest; since == previous max("u") - 2h, and the version stores the new watermark

def test_by_stamp_splits_rows_into_one_version_per_partition():
    ds = dataset(fc.strategy.by_stamp("u"), partition=fc.partition.by_day("date", tz="UTC"))
    vs = extract(ds, [{"id": 1, "date": "2026-10-18", "u": T0}, {"id": 2, "date": "2026-10-19", "u": T0}], partition="*")
    assert sorted(v.partition for v in vs) == ["date=2026-10-18", "date=2026-10-19"]

def test_by_stamp_deleted_field_emits_a_delete():
    ds = dataset(fc.strategy.by_stamp("u", deleted_field="gone"))
    v = extract(ds, [{"id": 1, "u": T0, "gone": True}], partition="*")[0]
    assert (recs(v)[0].key, recs(v)[0].op) == ("1", "delete")

def test_key_rewrite_keeps_one_current_row():  # H3
    ds = dataset(fc.strategy.snapshot_diff(), key="sku")
    extract(ds, [{"row_id": 10, "sku": "A", "p": 1}])
    v = extract(ds, [{"row_id": 99, "sku": "A", "p": 1}], run="r2")[0]
    assert [(r.key, r.op) for r in recs(v)] == [("A", "upsert")]

def test_duplicate_keys_in_one_read_keep_the_last():  # Review Focus 1
    v = extract(dataset(fc.strategy.replace()), [{"id": 1, "v": "old"}, {"id": 1, "v": "new"}])[0]
    assert [r["v"] for r in recs(v)] == ["new"]

def test_missing_key_is_quarantined_and_the_rest_proceed():  # Review Focus 2
    v = extract(dataset(fc.strategy.replace()), [{"id": None}, {"id": 2}])[0]
    assert [r.key for r in recs(v)] == ["2"]
    assert "id" in DEPS.store.quarantine.list_open()[0].error

def test_empty_read_cannot_wipe_a_slice_unless_allowed():  # Review Focus 3
    ds = dataset(fc.strategy.snapshot_diff()); extract(ds, [{"id": 1}])
    with pytest.raises(ExtractFailed, match="allow_empty"):
        extract(ds, [], run="r2")
    ds2 = dataset(fc.strategy.snapshot_diff(), name="d2", allow_empty=True); extract(ds2, [{"id": 1}])
    assert extract(ds2, [], run="r2")[0].deletes == 1

def test_first_empty_read_is_fine(): ...  # no previous snapshot: seals nothing, raises nothing

def test_validation_contract_quarantines_invalid_records():  # FR-C3
    class M(BaseModel): id: int; price: float
    v = extract(dataset(fc.strategy.replace(), schema=M), [{"id": 1, "price": "x"}, {"id": 2, "price": 3.0}])[0]
    assert [r.key for r in recs(v)] == ["2"]

def test_on_record_error_fail_fails_the_version(): ...

def test_restart_rewrites_chunks_and_drops_leftovers():
    # stage a version with 3 chunks using CHUNK_ROWS=1, then run again under the same run id with 1 row
    assert len(DEPS.spool.list_chunks(vid)) == 1

def test_large_read_stays_chunked():
    # with CHUNK_ROWS=2 and 5 rows, three data chunks are written, in order

def test_extract_never_calls_a_sink(): ...  # I5: the sink's calls stay empty
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_extract.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `run_extract`.** Keep memory bounded to one chunk, in this order:

```text
read batches -> make Records (bad ones go to quarantine; last row wins per key within a chunk window)
             -> write whole-slice chunks of kind "full", and accumulate (key, row_hash) pairs
new snapshot = pairs, de-duplicated by key, last wins
old snapshot = latest sealed "snapshot" chunk for the partition (complete strategies),
               or row hashes of versions from since_watermark(latest watermark - lookback) (by_stamp),
               or record state (when keep_state)
diff = strategy.diff(new, old); nothing to do -> no version
second pass over "full" chunks: keep rows in diff.upsert_keys, write "data" chunks
write new "snapshot" chunk; write delete candidates as a "candidates" chunk
drop "full" chunks unless a route uses replace_partition
stage version; if not stage_only: finalize_versions([version]) (Task 9 extends it)
```

For now `finalize_versions` turns candidates into delete records in a final data chunk and calls `versions.seal` with the route names. It also holds the empty-read guard: a complete version whose new snapshot has no rows and which still implies deletes raises `ExtractFailed` naming `allow_empty`, unless the dataset allows it. For `by_stamp`, the read uses `partition="*"`, and rows are grouped by `dataset.partition.key_of` into one version each. The new watermark is the maximum of the stamp field.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add extraction with change strategies"`

### Task 9: Run-level delete reconciliation and moves

**Files:**
- Create: `src/feather_connect/runtime/seal.py`
- Modify: `src/feather_connect/runtime/extract.py` (`finalize_versions`)
- Test: `tests/runtime/test_seal.py`

**Interfaces:**
- Consumes: `run_extract`, `finalize_versions`; `VersionStore.staged_for_run`, `register_slices`, `finish_slice`; `RecordStateStore`.
- Produces: `run_seal(dataset, run_id, *, routes, deps) -> list[Version]`. `finalize_versions` applies I10 across all versions it is given.

- [ ] **Step 1: Write the failing tests**

```python
def run_slices(ds, run, slices):  # slices: {partition: rows}
    ds.source.load([r for rows in slices.values() for r in rows])
    DEPS.store.versions.register_slices(run, ds.name, list(slices))
    for p in slices:
        run_extract(ds, run, p, routes=ROUTES, stage_only=True, deps=DEPS)
    return run_seal(ds, run, routes=ROUTES, deps=DEPS)

def test_move_between_slices_of_one_run_is_not_a_delete():  # H1b, I10
    ds = day_dataset()
    run_slices(ds, "r1", {"date=2026-10-18": [{"id": 1, "date": "2026-10-18"}], "date=2026-10-19": []})
    vs = run_slices(ds, "r2", {"date=2026-10-18": [], "date=2026-10-19": [{"id": 1, "date": "2026-10-19"}]})
    ops = [(r.key, r.op) for v in vs for r in all_records(v)]
    assert ("1", "delete") not in ops and ("1", "upsert") in ops
    assert moves_of(version_for(vs, "date=2026-10-19")) == {"1": "date=2026-10-18"}

def test_key_absent_from_every_slice_is_a_delete():
    ds = day_dataset()
    run_slices(ds, "r1", {"date=2026-10-18": [{"id": 1, "date": "2026-10-18"}, {"id": 2, "date": "2026-10-18"}]})
    vs = run_slices(ds, "r2", {"date=2026-10-18": [{"id": 1, "date": "2026-10-18"}]})
    assert ("2", "delete") in [(r.key, r.op) for r in all_records(vs[0])]

def test_nothing_is_sealed_until_seal_run():
    ds = day_dataset(); ds.source.load([{"id": 1, "date": "2026-10-18"}])
    DEPS.store.versions.register_slices("r1", "d", ["date=2026-10-18"])
    v = run_extract(ds, "r1", "date=2026-10-18", routes=ROUTES, stage_only=True, deps=DEPS)[0]
    assert v.status == "staged" and DEPS.store.versions.latest_sealed("d", "date=2026-10-18") is None

def test_seal_run_seals_all_versions_together_and_is_idempotent(): ...

def test_move_across_runs_with_keep_state():  # H1a
    ds = day_dataset(keep_state=True)
    run_slices(ds, "r1", {"date=2026-10-18": [{"id": 1, "date": "2026-10-18"}]})
    v = run_slices(ds, "r2", {"date=2026-10-19": [{"id": 1, "date": "2026-10-19"}]})[0]
    assert moves_of(v) == {"1": "date=2026-10-18"}
    vs = run_slices(ds, "r3", {"date=2026-10-18": []})  # the old slice is read later
    assert ("1", "delete") not in [(r.key, r.op) for v in vs for r in all_records(v)]

def test_keep_state_records_the_latest_row(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_seal.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `run_seal` and extend `finalize_versions`.** Load the `candidates` of all the versions, and the `snapshot` chunks of all the run's versions. A candidate key present in another version's new snapshot is a move: drop it, and write `{key: old_partition}` to the receiving version's `moves` chunk. With `keep_state`, also drop a candidate whose current state row is in another partition, and record a move when an upserted key's state row is in another partition. Apply the empty-read guard only after this reconciliation, slice by slice, so a slice emptied by a move doesn't trip it. Write remaining candidates as delete records in a final data chunk. Call `versions.seal` once with all version ids, so sealing is one transaction. Update record state after sealing. Versions left with no records and no moves still seal when they are complete, because a complete read is itself a fact.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass. Also run `uv run pytest packages/feather-connect/tests/runtime/test_extract.py -q`, which must still pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: reconcile implied deletes per run (I10)"`

### Task 10: Applying versions to routes

**Files:**
- Create: `src/feather_connect/runtime/apply.py`
- Test: `tests/runtime/test_apply.py`

**Interfaces:**
- Consumes: `TaskContext`, `LocalEngine`; `Ledger`, `QuarantineStore`, `Spool`, `table_to_records`; `RouteSpec`, `apply_map`; `ApplyBatch`, `ApplyRecord`, `ApplyResult`; `TransientError`, `PermanentError`, `RecordRejected`, `BudgetDeferred`; `decode_key`.
- Produces: `run_apply(ctx: TaskContext, dataset, route: RouteSpec, version_id: str, deps) -> None`.

- [ ] **Step 1: Write the failing tests.** Helper `seal_versions(rows_by_run)` extracts through Task 8. Helper `apply(route, version_id)` runs `run_apply` inside a `LocalEngine` task.

```python
def test_routes_progress_independently():  # H6
    bad.fail_next(PermanentError("down"))
    apply(route_bad, v1); apply(route_good, v1)
    assert ledger.get("bad", v1).status == "failed" and ledger.get("good", v1).status == "applied"

def test_extraction_continues_while_a_route_is_down():  # H6
    # two more versions seal while bad is failed; after skip or retry, all apply in seal order
    assert [c.version_id for c in bad.calls] == [v1, v2, v3]

def test_versions_apply_in_seal_order():  # I2
    apply(route, v2)  # submitted for the later version
    assert [c.version_id for c in sink.calls] == [v1, v2]

def test_a_failed_version_blocks_its_partition_only():
    # v1 in p1 failed: v2 in p1 waits, and v3 in p2 applies
def test_transient_error_retries_and_succeeds(): ...  # delivery ends applied, attempts recorded
def test_applied_at_most_once():  # I3
    apply(route, v1); apply(route, v1)
    assert len(sink.calls) == 1
def test_crash_mid_apply_resumes_without_duplicates():
    # two chunks, crash before the second chunk's step, recover
    assert [c.chunk_no for c in sink.calls] == [0, 1] and ledger.get("r", v1).status == "applied"
def test_sink_rejection_quarantines_and_continues():
    sink.reject("2", "bad sku"); apply(route, v1)
    assert ledger.get("r", v1).status == "applied" and quarantine.list_open()[0].key == "2"
def test_map_none_filters_and_a_list_splits(): ...
def test_map_error_quarantines_the_record():  # Review Focus 5
    def boom(r, c):
        if r["id"] == 2: raise ValueError("no price")
        return dict(r)
    apply(route(map=boom), v1)
    assert [x["id"] for x in sink.rows] == [1, 3] and "no price" in quarantine.list_open()[0].error
def test_map_error_with_on_record_error_fail_fails_the_version(): ...
def test_batch_map_transforms_a_chunk(): ...
def test_deletes_skip_the_map_and_reach_the_sink(): ...
def test_append_adds_fc_columns():
    row = append_sink.rows[0]
    assert {"_fc_key", "_fc_op", "_fc_version", "_fc_run", "_fc_extracted_at", "_fc_partition"} <= row.keys()
def test_replace_partition_gets_the_whole_slice(): ...  # uses "full" chunks, is_first and is_last set
def test_move_reaches_the_sink_as_moved_from(): ...
def test_newer_version_supersedes_quarantined_record():  # H10
    # keep_state: v1's record rejected; v2 with the same key applies
    assert quarantine.list_open() == [] and row_status == "superseded"
def test_budget_deferral_sleeps_durably_and_does_not_fail(): ...  # sink raises BudgetDeferred once
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_apply.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `run_apply`.** The loop decides nothing outside a step:

```text
target = step("target", versions.get(version_id))
for n in count():
    d = step(f"next:{n}", ledger.oldest_open(route, target.partition))
    if d is None or d.seal_seq > target.seal_seq: return
    if d.status in ("failed", "held"): log "blocked by {d.version_id}"; return
    step(f"start:{d.version_id}", transition -> running)
    for each chunk of the mode's kind ("full" for replace_partition, else "data"):
        result = step(f"chunk:{d.version_id}:{chunk_no}", transform and sink.apply)
    step(f"end:{d.version_id}", transition -> applied with counts, then quarantine.supersede)
```

`PermanentError` moves the delivery to `failed` and ends the task without an engine-level failure, so other partitions continue. `TransientError` propagates so that the engine retries. `BudgetDeferred` calls `ctx.sleep(wait)` when the wait is 60 s or longer, and retries the chunk step. Shorter waits are handled in `Context.budget`. The sink's idempotency key is `{route}:{version_id}:{chunk_no}:{replays}`, so a retry can't double-apply a chunk, but a replay reaches the sink again. Step results hold counts only, never records.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: apply versions to routes with ledger and quarantine"`

### Task 11: Runtime wiring and the plan task

**Files:**
- Create: `src/feather_connect/runtime/runtime.py`, `runtime/plan.py`
- Test: `tests/runtime/test_runtime.py`

**Interfaces:**
- Consumes: `run_extract`, `run_seal`, `run_apply`, `RuntimeDeps`, `Engine`, `TaskCall`, `Flow`.
- Produces: `Runtime(flows, store, spool, engine, *, code_sha="dev", clock=None, make_context=None)` with:
  - `tasks() -> Mapping[str, TaskFn]`, keys `feather.plan`, `feather.extract`, `feather.seal_run`, `feather.apply`;
  - `start_run(flow, *, trigger="manual", actor="system", partitions=None, reason=None) -> str`;
  - `retry(route, version_id, *, actor, reason=None)`, `skip(route, version_id, *, actor, reason=None)`, `replay(route, version_id, *, actor, reason=None)`, `submit_apply(route, version_id)`, `submit_seal(flow, dataset, run_id)`;
  - `flow(name)`, `dataset(name)`, `route(name)`.
- Task names and keys are as design §6.2, with tenant `default`: concurrency `extract:default:{dataset}:{partition}`, `apply:default:{route}:{partition}`, `seal:default:{dataset}`, `plan:default:{flow}`.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_run_fans_out_to_all_routes():
    run = rt.start_run("f"); engine.run_until_idle()
    assert a.rows == b.rows == EXPECTED and store.runs.get_run(run).status == "succeeded"

def test_task_args_carry_ids_only():  # I8
    for call in engine.submitted():
        assert set(call.args) <= {"run_id", "flow", "dataset", "partition", "route", "version_id"}
        assert len(json.dumps(call.args)) < 64_000

def test_explicit_partitions_override_the_plan(): ...  # re-run of one day reads only that day
def test_default_plan_uses_the_scheme_recent_window(): ...
def test_custom_planner_is_used(): ...
def test_by_stamp_plans_one_extract_with_partition_star(): ...
def test_multi_slice_run_seals_after_the_last_slice(): ...  # seal_run submitted once
def test_single_slice_run_seals_without_seal_run(): ...
def test_every_task_records_its_code_sha(): ...
def test_failed_extract_fails_the_run_with_the_reason(): ...
def test_replay_applies_an_applied_version_again():  # FR-B8
    rt.replay("a", v1, actor="op", reason="fix"); engine.run_until_idle()
    assert [c.version_id for c in a.calls].count(v1) == 2
    assert [x.to_status for x in ledger.attempts("a", v1)][-1] == "applied"
def test_retry_and_skip_unblock_a_failed_version(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_runtime.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `Runtime` and the plan task.** `feather.plan` picks partitions in this order: the run's explicit list, then `dataset.planner(now)`, then `dataset.partition.recent(now)`; `by_stamp` always gets `["*"]`. It registers slices when a complete dataset has more than one. `feather.extract` passes `stage_only=True` for multi-slice runs, calls `finish_slice`, and submits `feather.seal_run` when the last one finishes. After sealing, it submits `feather.apply` for each (route, version). An `apply` task's idempotency key is `apply:{route}:{version_id}:{replays}:{retries}`: a re-submit of the same state is a no-op, while a retry or a replay gets a new task. The run finishes as `succeeded` or `failed` when its tasks do; compute that from `list_tasks`, in a final step of each task.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: wire the runtime and plan task"`

### Task 12: FlowHarness

**Files:**
- Create: `src/feather_connect/testing/harness.py`
- Modify: `src/feather_connect/testing/__init__.py` (export `FlowHarness`)
- Test: `tests/testing/test_harness.py`

**Interfaces:**
- Consumes: `Runtime`, `LocalEngine`, `MemoryStore`, `MemorySpool`, `memory.Source`, `memory.Sink`.
- Produces: `FlowHarness(flow, *, clock=None)` with `source(dataset_name) -> memory.Source`, `sink(route_name) -> memory.Sink`, `run(*, partitions=None) -> str`, `ledger`, `quarantine`, `versions(dataset_name) -> list[Version]`, `engine`, `store`. It never touches the flow's real connectors.

- [ ] **Step 1: Write the failing test, which is design §5.7 verbatim**

```python
def test_prices_fan_out_and_deletes_propagate():
    h = FlowHarness(flow)  # every connection is swapped for an in-memory connector
    h.source("sap.price").load([
        {"material": "M1", "price_list": "RET", "amount": 499.0},
        {"material": "M2", "price_list": "RET", "amount": 999.0},
    ])
    h.run()
    assert h.sink("zakya").rows == [{"sku": "M1", "rate": 499.0}, {"sku": "M2", "rate": 999.0}]

    h.source("sap.price").load([{"material": "M1", "price_list": "RET", "amount": 549.0}])
    h.run()  # snapshot_diff: M1 updated, M2 deleted
    assert h.sink("magento").deleted_keys == [("M2", "RET")]
    assert h.ledger.all_applied()

def test_harness_does_not_call_real_connectors():
    class Explodes(memory.Sink):
        def apply(self, ctx, batch): raise AssertionError("real connector called")
    ...  # a flow using Explodes runs clean under the harness

def test_harness_exposes_quarantine_and_versions(): ...
```

The test module's `flow` is the §5.2 example with fake `zakya`, `magento` and `gofrugal` sink classes defined in the test, each with `connection=` and a `spec`. `decode_key` makes composite deleted keys appear as tuples, so `memory.Sink.deleted_keys` holds `("M2", "RET")`.

- [ ] **Step 2: Run it and confirm it fails.** Run: `uv run pytest packages/feather-connect/tests/testing/test_harness.py -q`. Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement `FlowHarness`.** Build a copy of the flow in which each dataset's source and each route's sink is replaced by an in-memory one that declares the real one's spec modes. `load` replaces the source's rows, so each `load` is the source's new contents. `run` calls `start_run` and then `engine.run_until_idle()`.

- [ ] **Step 4: Run it and confirm it passes.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add FlowHarness"`

### Task 13: JSON simulation connector

**Files:**
- Create: `src/feather_connect/connectors/simulation.py`
- Test: `tests/connectors/test_simulation.py`

**Interfaces:**
- Consumes: `Source`, `Sink`, `ConnectorSpec`, `TransientError`.
- Produces: `Faults(duplicate=0.0, late=0.0, hard_delete=0.0, key_rewrite=0.0, schema_change=0.0, http_429=0.0, http_5xx=0.0, timeout=0.0)`; `Source(connection="simulation", fixture=None, generator=None, rows_per_partition=100, seed=42, faults=Faults())`; generator field specs `seq()`, `choice(values)`, `date_in(start, end)`, `amount(low, high)`, `name()`; `Sink(connection="simulation", capture_to=None)`, which behaves like `memory.Sink` and, when `capture_to` is a path, writes JSONL. Fixture files are JSON or JSONL.

- [ ] **Step 1: Write the failing tests**

```python
GEN = {"id": sim.seq(), "store": sim.choice([1, 2]), "day": sim.date_in("2026-10-01", "2026-10-07"),
       "amount": sim.amount(10, 100), "customer": sim.name()}

def read_all(s, partition="*"):
    return [r for b in s.read(CTX, ReadRequest(partition, none())) for r in b]

def test_same_seed_gives_same_rows():
    assert read_all(sim.Source(generator=GEN, seed=1)) == read_all(sim.Source(generator=GEN, seed=1))
    assert read_all(sim.Source(generator=GEN, seed=1)) != read_all(sim.Source(generator=GEN, seed=2))

def test_fixture_file_is_read(tmp_path): ...  # JSON and JSONL
def test_duplicates_fault_repeats_rows(): ...
def test_hard_delete_fault_drops_rows_on_a_later_read(): ...
def test_key_rewrite_fault_changes_the_key_field_in_place(): ...
def test_schema_change_fault_adds_and_removes_a_column(): ...
def test_late_fault_holds_rows_for_a_later_read(): ...
def test_http_faults_raise_transient_errors():
    s = sim.Source(generator=GEN, faults=sim.Faults(http_429=1.0))
    with pytest.raises(TransientError) as e:
        read_all(s)
    assert e.value.retry_after is not None
def test_faults_are_repeatable_by_seed(): ...
def test_fan_out_on_the_local_runtime(): ...  # Runtime on LocalEngine, because FlowHarness swaps sources: with Faults(duplicate=0.2), no duplicate rows reach the sink
def test_capture_to_writes_jsonl(tmp_path): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/connectors/test_simulation.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `connectors/simulation.py`.** Use `random.Random(seed)` per source, so runs repeat. Keep a read counter, so late and delete faults can differ between reads. Push-mode event rates are added in Plan 3, Task 4.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add JSON simulation connector"`

### Task 14: Rate budgets

**Files:**
- Create: `src/feather_connect/runtime/budgets.py`
- Modify: `storage/port.py` and `storage/memory.py` (add `BudgetStore`, and `Store.budgets`), `testing/store_contract.py`, `core/context.py`
- Test: `tests/runtime/test_budgets.py`

**Interfaces:**
- Consumes: `Store`, `Clock`, `BudgetDeferred`.
- Produces in `storage/port.py`: `BudgetSpec(name, windows: tuple[Window, ...], reserve: Mapping[str, float] = {})`; `Window(units, seconds)`; `Grant(granted: bool, wait: timedelta)`; `BudgetStore`: `define(spec)`, `acquire(name, units, priority="normal") -> Grant`, `usage(name) -> list[WindowUsage]`. Add `parse_limits(["90/minute", "24000/day"]) -> tuple[Window, ...]`.
- Produces in `runtime/budgets.py`: `BudgetHandle(store, name, priority, sleep=time.sleep)` with `acquire(units=1)`: on no grant, it sleeps if the wait is under 60 s and tries again; otherwise it raises `BudgetDeferred(wait, name)`.

- [ ] **Step 1: Write the failing tests.** Add the store-level ones to `StoreContract`, so Postgres inherits them.

```python
# In StoreContract
def test_budget_never_exceeds_any_window(self, store, clock):
    store.budgets.define(BudgetSpec("z", parse_limits(["10/minute", "12/day"])))
    assert sum(store.budgets.acquire("z", 1).granted for _ in range(20)) == 10
    clock.advance(timedelta(minutes=1))
    assert sum(store.budgets.acquire("z", 1).granted for _ in range(20)) == 2  # the day window bites

def test_low_priority_leaves_the_reserve_for_high(self, store):
    store.budgets.define(BudgetSpec("z", parse_limits(["10/minute"]), reserve={"high": 0.6}))
    assert sum(store.budgets.acquire("z", 1, "low").granted for _ in range(10)) == 4
    assert sum(store.budgets.acquire("z", 1, "high").granted for _ in range(10)) == 6

def test_refused_grant_says_how_long_to_wait(self, store): ...
def test_acquire_is_all_or_nothing_across_windows(self, store): ...

# In tests/runtime/test_budgets.py
def test_short_wait_sleeps_in_process(): ...  # sleep called once, then granted
def test_long_wait_raises_budget_deferred():
    with pytest.raises(BudgetDeferred) as e:
        handle.acquire()
    assert e.value.budget == "z" and e.value.wait > timedelta(seconds=60)
def test_two_flows_share_one_budget():  # H7, on LocalEngine
    # high-priority reader and low-priority writer share 10 units per window; a sink meters via ctx.budget
    assert max_units_in_any_window <= 10 and min_high_in_saturated_window >= 6
def test_budget_exhaustion_defers_and_does_not_fail():  # H8
    assert ledger.get("r", v1).status == "applied" and clock.now() - start >= timedelta(hours=1)
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_budgets.py packages/feather-connect/tests/storage -q`. Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement budgets.** Token-bucket refill per window, using the store's clock. Low priority may use only `(1 - reserve)` of each window, across the union of the higher priorities' reserves. `Context.budget` is a `BudgetHandle` when the connection declares a budget. `Runtime` passes it through `make_context`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Run the whole package with coverage.** Run: `uv run pytest packages/feather-connect/tests --cov=feather_connect --cov-fail-under=85 -q`, then `uv run pyright`, then `uv run ruff check .`. Expected: all clean.

- [ ] **Step 6: Commit.** `git add packages/feather-connect && git commit -m "feat: add rate budgets"`

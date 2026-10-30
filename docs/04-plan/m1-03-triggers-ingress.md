# M1 Plan 3: Triggers, ingress and the sealer

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flows run by themselves on schedules and on webhooks, a webhook reaches a destination in about 5 s at p95, and every tick is recorded, including skipped ones.

**Architecture:** `feather-server` gets a minimal FastAPI app that hosts the webhook endpoint and runs the singleton loops (scheduler, sealer, sweeper, retention), each elected through a Postgres advisory lock. The loops' logic lives in `feather_connect.runtime` as pure functions over the store, so it can be tested with a frozen clock.

**Tech Stack:** FastAPI, uvicorn, cronsim, psycopg advisory locks.

**Spec:** [design v0.2](../02-design/2026-10-30-feather-connect-design.md) §6.3, §6.4, §6.7, §13.5, §18.1. Consumes Plans 1 and 2.

## Global Constraints

- Everything in Plan 1's and Plan 2's Global Constraints still applies.
- **Every trigger writes a run** with a reason, including skipped ticks (I6, FR-E4). Skip reasons are `paused`, `disabled`, `overlap`, `outside_window` and `catch_up_collapsed`.
- Ingress never waits on destinations (I5). The latency budget per stage is in design §6.3: ingress ≤ 200 ms, sealing ≤ 1 s, dispatch ≤ 1 s.
- The sealer seals every 1 s or every 500 events, whichever comes first.
- Webhook signatures: HMAC-SHA256 or the source's own scheme, with a 5-minute timestamp tolerance.
- Settings use the variables in design §18.1.

Paths are relative to the repository root.

## Review Focus

1. The scheduler was down for a day: missed ticks collapse into one run plus one recorded skip, instead of thousands of runs (Task 2).
2. A webhook has a bad signature or a stale timestamp: it gets 401, nothing is stored, and the response doesn't say which check failed (Task 3).
3. A webhook body is larger than the limit: it gets 413 before the body is read into memory (Task 3).
4. An event can't be parsed by `receive()`: it is quarantined with the raw event referenced, and the other events in the batch are sealed (Task 4).
5. A vendor retries the same webhook: an event id, when the source provides one, is stored once, so `append` routes don't get a duplicate (Tasks 3 and 4).

---

### Task 1: Server skeleton and leader-elected loops

**Files:**
- Create: `packages/feather-server/pyproject.toml`, `packages/feather-server/src/feather_server/__init__.py`, `feather_server/settings.py`, `feather_server/app.py`, `feather_server/loops.py`
- Create: `packages/feather-connect/src/feather_connect/runtime/leader.py`
- Test: `packages/feather-server/tests/test_app.py`, `packages/feather-server/tests/test_loops.py`

**Interfaces:**
- Consumes: `load_project`, `connect`, `DbosEngine`, `LocalEngine`, `Runtime`, `sweep_once`, `retention_once`.
- Produces in `runtime/leader.py`: `LeaderLock` protocol with `try_acquire() -> bool` and `release()`; `PostgresLeaderLock(database_url, key: int)`, which holds one session open with `pg_try_advisory_lock`; `AlwaysLeader`.
- Produces in `feather_server/settings.py`: `Settings.from_env()`, with fields `env`, `database_url`, `engine`, `engine_poll_s`, `spool`, `secret_key`, `base_url`, `project`.
- Produces in `feather_server/loops.py`: `LoopRunner(name, fn: Callable[[], None], interval_s, lock: LeaderLock)`, with `start()`, `stop()`, and `last_error`; `default_loops(state) -> list[LoopRunner]`: `scheduler` 1 s, `sealer` 0.25 s, `sweeper` 5 s, `retention` 1 h. `fn` for the first two come from Tasks 2 and 4, so wire them there.
- Produces in `feather_server/app.py`: `create_app(settings=None, *, state=None) -> FastAPI`; `AppState(settings, project, store, spool, engine, runtime)` on `app.state.feather`. The lifespan starts and stops the loops. `GET /healthz` returns 200 always. `GET /readyz` returns 503 until the database answers and the schema is at head.

- [ ] **Step 1: Write the failing tests**

```python
def test_healthz_and_readyz(client):
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").json()["database"] == "ok"

def test_readyz_is_503_when_the_database_is_down(app_with_bad_db): ...

def test_only_one_loop_runner_leads(pg_url):  # postgres
    a, b = counting_runner(pg_url), counting_runner(pg_url)
    a.start(); b.start(); time.sleep(1)
    assert (a.count > 0) != (b.count > 0)

def test_a_new_leader_takes_over(pg_url):
    a, b = counting_runner(pg_url), counting_runner(pg_url)
    a.start(); time.sleep(0.5); b.start(); a.stop(); time.sleep(1)
    assert b.count > 0

def test_a_raising_loop_keeps_running_and_records_the_error():
    r = LoopRunner("x", raises_twice_then_ok, 0.01, AlwaysLeader()); r.start(); wait_until(lambda: r.count >= 3)
    assert "boom" in str(r.last_error)
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the package.** Dependencies: `fastapi`, `uvicorn`, `feather-connect[postgres]`, `feather-engine-dbos`. Loops run on daemon threads, one lock per loop. `create_app` accepts a prepared `state` so tests can inject `MemoryStore` and `LocalEngine`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add server skeleton and leader-elected loops"`

### Task 2: Scheduler

**Files:**
- Create: `packages/feather-connect/src/feather_connect/runtime/scheduler.py`
- Modify: `storage/port.py`, `storage/memory.py`, `storage/postgres/store.py`, `testing/store_contract.py`, and `feather_server/loops.py` under `packages/feather-connect` and `packages/feather-server`
- Test: `packages/feather-connect/tests/runtime/test_scheduler.py`

**Interfaces:**
- Consumes: `Flow.triggers` (`cron`, `every`), `Runtime.start_run`, `RunJournal`, `FrozenClock`.
- Produces in `storage/port.py`: `ScheduleStore`: `last_tick(flow, trigger_id) -> datetime | None`, `set_last_tick(flow, trigger_id, at)`. Available as `store.schedules`.
- Produces: `tick(runtime, *, now) -> list[TickResult]`, `TickResult(flow, trigger_id, due_at, run_id, status, reason)`. `trigger_id` is a stable string such as `cron:*/15 * * * *:Asia/Kolkata`. Add `Runtime.active_runs(flow) -> list[Run]` and `Runtime.cancel_run(run_id, *, actor, reason=None)`.

- [ ] **Step 1: Write the failing tests** (`MemoryStore`, `LocalEngine`, `FrozenClock`)

```python
def test_cron_fires_in_the_flow_timezone():
    # "0 6 * * *" tz Asia/Kolkata: not at 06:00 UTC, but at 00:30 UTC
def test_first_tick_after_start_does_not_backfill(): ...  # no last_tick: records now, starts nothing
def test_overlap_skip_records_a_skipped_run():
    # overlap="skip", previous run still active
    assert run.status == "skipped" and run.reason == "overlap"
def test_overlap_queue_starts_anyway(): ...
def test_overlap_cancel_previous_cancels_the_active_run(): ...
def test_catch_up_latest_collapses_missed_ticks():  # Review Focus 1
    # every 15 minutes, last_tick a day ago
    runs = store.runs.list_runs(flow="f")
    assert sum(r.status == "queued" or r.status == "succeeded" for r in runs) == 1
    assert [r.reason for r in runs if r.status == "skipped"] == ["catch_up_collapsed"]
def test_catch_up_each_runs_every_missed_tick(): ...  # capped at 1,000, with the cap recorded
def test_restart_does_not_double_fire(): ...  # a second tick at the same now starts nothing
def test_every_interval_fires_on_schedule(): ...
def test_dst_gap_and_overlap_fire_once(): ...  # America/New_York, 02:30 daily, across both changes
def test_disabled_flow_records_a_skip(): ...  # an overlay can disable a flow: status skipped, reason disabled
def test_a_failing_flow_does_not_stop_other_flows_ticking(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_scheduler.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `tick`.** Compute due times with `cronsim.CronSim(expr, last_tick_in_tz)`. Add `cronsim` to `feather-connect`. Set `last_tick` in the same call as the run row, so a crash can't double-fire. Register `tick` as the `scheduler` loop.

- [ ] **Step 4: Run them and confirm they pass.** Same command, plus `uv run pytest packages/feather-connect/tests/storage -q`. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add scheduler"`

### Task 3: Webhook ingress and inbox

**Files:**
- Create: `packages/feather-server/src/feather_server/ingress.py`, `packages/feather-connect/src/feather_connect/core/webhooks.py`
- Modify: `core/connector.py` (add `InboundRequest`), `storage/port.py`, `storage/memory.py`, `storage/postgres/store.py`, `testing/store_contract.py`
- Test: `packages/feather-server/tests/test_ingress.py`, `packages/feather-connect/tests/core/test_webhooks.py`

**Interfaces:**
- Consumes: `create_app`, `PushSource`, `Runtime.dataset`.
- Produces in `core/connector.py`: `InboundRequest(method, path, headers: Mapping[str, str], body: bytes, received_at)`. `PushSource` gains an optional `event_id(request) -> str | None`.
- Produces in `core/webhooks.py`: `verify_hmac(secret: str, body: bytes, signature: str, timestamp: str | None, *, now, tolerance=timedelta(minutes=5)) -> None`, which raises `SignatureInvalid` and compares in constant time.
- Produces in `storage/port.py`: `InboxEvent(event_seq, dataset, event_id, request, received_at)`; `InboxStore`: `put(dataset, request, event_id=None) -> bool` (False for a repeat of an event id); `take(dataset, limit) -> list[InboxEvent]`; `oldest_age(dataset, now) -> timedelta | None`; `seal_batch(event_seqs, drafts, routes) -> list[Version]`, which marks events sealed and stages and seals their versions in **one transaction**. Available as `store.inbox`.
- Produces in `feather_server/ingress.py`: router for `POST /ingress/{tenant}/{source}`, where `source` is a dataset name with a `push` strategy. Tenant must be `default` in M1. The body limit defaults to 1 MiB, or `settings.ingress_max_bytes`. Per-source rate limit: 1,000 requests/s by default, from an in-process token bucket.

- [ ] **Step 1: Write the failing tests**

```python
def test_valid_webhook_is_stored_and_returns_202(client, store, signed): ...
def test_ingress_never_touches_a_sink(client, sink, signed): ...  # I5
def test_bad_signature_is_401_and_stores_nothing(client, store):  # Review Focus 2
    r = client.post(URL, content=b"{}", headers={"X-Signature": "wrong", "X-Timestamp": NOW})
    assert r.status_code == 401 and r.json() == {"title": "Unauthorized", "status": 401}
    assert store.inbox.take("d", 10) == []
def test_stale_timestamp_is_401_with_the_same_body(client): ...  # Review Focus 2
def test_oversized_body_is_413_without_reading_it(client):  # Review Focus 3
    # send Content-Length over the limit and a body generator that fails the test if consumed
def test_chunked_body_over_the_limit_is_413(client): ...
def test_repeated_event_id_is_stored_once_and_returns_202(client, store, signed):  # Review Focus 5
def test_unknown_source_is_404(client): ...
def test_non_push_dataset_is_404(client): ...
def test_rate_limit_returns_429_with_retry_after(client): ...
def test_verify_hmac_cases(): ...  # valid; wrong secret; timestamp 6 minutes old; missing timestamp allowed only when configured
```

Add to `StoreContract`: `test_inbox_take_is_in_arrival_order`, `test_seal_batch_is_one_transaction`.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_ingress.py packages/feather-connect/tests/core/test_webhooks.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement ingress.** Check `Content-Length` first, then read the stream with a running count. Call `source.verify` before storing. Errors use `problem+json`. A failed verification returns the same response in every case. Log the reason server-side, without the body.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add webhook ingress and inbox"`

### Task 4: Sealer

**Files:**
- Create: `packages/feather-connect/src/feather_connect/runtime/sealer.py`
- Modify: `packages/feather-connect/src/feather_connect/connectors/simulation.py` (push mode), `packages/feather-server/src/feather_server/loops.py`
- Test: `packages/feather-connect/tests/runtime/test_sealer.py`

**Interfaces:**
- Consumes: `InboxStore`, `PushSource.receive` (returns payload mappings, or `Deleted(payload)` for explicit deletes), `make_key`, `row_hash`, `records_to_table`, `Runtime.submit_apply`, `stable_id`.
- Produces: `seal_once(runtime, *, now, max_events=500, max_age=timedelta(seconds=1)) -> list[Version]`. Adds `simulation.Source.events(rate_per_s, *, bursty=False) -> Iterator[InboundRequest]`, which yields signed requests.

- [ ] **Step 1: Write the failing tests**

```python
def test_seals_after_max_age(): ...          # 3 events, 0.5 s: nothing; 1 s: one version
def test_seals_at_max_events(): ...          # 500 events seal at once, without waiting
def test_groups_events_by_partition(): ...   # by_bucket gives one incomplete version per bucket
def test_keeps_per_key_order():              # FR-B6
    # two updates to one key in one batch: the later one is what the sink ends up with
def test_version_is_incomplete_and_only_explicit_deletes(): ...  # I4
def test_bad_event_is_quarantined_and_the_rest_seal():  # Review Focus 4
    assert [r.key for r in records_of(v)] == ["1", "3"]
    assert "event" in store.quarantine.list_open()[0].error
def test_crash_between_seal_and_submit_is_recovered_by_the_sweeper(): ...
def test_repeated_seal_once_seals_nothing_new(): ...
def test_an_event_id_retry_does_not_reach_an_append_route_twice(): ...  # Review Focus 5
def test_on_event_flow_needs_no_run_per_event(): ...  # versions attach to one daily stream run
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_sealer.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `seal_once`.** Version ids are `stable_id(dataset, partition, str(first_event_seq))`. Write the chunks first (they are idempotent by id), then call `inbox.seal_batch`, then `submit_apply` for each (route, version). A crash between those last two is covered by the sweeper. Register it as the `sealer` loop.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add sealer for push datasets"`

### Task 5: High-frequency pollers

**Files:**
- Modify: `packages/feather-connect/src/feather_connect/runtime/runtime.py`, `runtime/plan.py`, `runtime/extract.py`, `runtime/scheduler.py`
- Test: `packages/feather-connect/tests/runtime/test_pollers.py`

**Interfaces:**
- Consumes: `tick`, `start_run`, `run_extract`.
- Produces: `Runtime.start_stream_tick(flow, *, trigger_id, at) -> str`. It finds or creates the flow's daily run, with trigger `stream`, and submits `feather.plan` with a `tick` argument. `run_extract` gains `tick: str | None = None`, which is added to `stable_id` for the version id. An `every()` interval under 5 minutes is stream mode. Longer intervals behave like cron.

- [ ] **Step 1: Write the failing tests**

```python
def test_poller_uses_one_run_per_day():
    # every("30s") ticking for 2 minutes: 1 run, 4 tasks
def test_a_new_day_opens_a_new_stream_run(): ...  # in the flow's timezone
def test_each_tick_is_recorded_as_a_task(): ...
def test_ticks_produce_distinct_versions(): ...
def test_paused_or_overlapping_tick_is_recorded_as_a_skipped_task(): ...  # with its reason, E4
def test_longer_interval_gets_a_run_per_tick(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_pollers.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement stream ticks.** Overlap for stream ticks is always `skip`, because the next tick reads from the same watermark anyway.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add high-frequency pollers with daily stream runs"`

### Task 6: Pause and resume

**Files:**
- Modify: `storage/port.py`, `storage/memory.py`, `storage/postgres/store.py`, `testing/store_contract.py`, `runtime/runtime.py`, `runtime/apply.py`, `runtime/scheduler.py`, all under `packages/feather-connect/src/feather_connect` and its tests
- Test: `packages/feather-connect/tests/runtime/test_pause.py`

**Interfaces:**
- Consumes: `Ledger.list_deliveries`, `Runtime.submit_apply`.
- Produces in `storage/port.py`: `OverrideStore`: `set(target, key, value, *, actor, reason=None, expires_at=None)`, `get(target, key) -> Any | None` (honours expiry), `list_all() -> list[Override]`, `clear(target, key, *, actor, reason=None)`. Available as `store.overrides`. Targets are `flow:{name}` and `route:{name}`, and the only key used in M1 is `paused`.
- Produces on `Runtime`: `pause_flow(flow, *, actor, reason=None)`, `resume_flow(...)`, `pause_route(route, *, actor, reason=None)`, `resume_route(...)`.

- [ ] **Step 1: Write the failing tests**

```python
def test_paused_flow_records_skipped_ticks(): ...  # reason "paused"
def test_paused_route_holds_deliveries_while_others_continue():
    rt.pause_route("a", actor="op", reason="vendor window"); run()
    assert ledger.get("a", v1).status == "pending" and ledger.get("b", v1).status == "applied"
def test_extraction_continues_while_a_route_is_paused(): ...  # I5
def test_resume_route_applies_the_backlog_in_order(): ...
def test_expired_pause_lifts_by_itself(): ...
def test_manual_run_of_a_paused_flow_is_refused_with_a_reason(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_pause.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement pause and resume.** `run_apply` checks the route's pause inside a step and returns without changing the delivery. `resume_route` submits `apply` for the oldest open delivery of each partition.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add pause and resume per flow and route"`

### Task 7: Latency and burst checks

**Files:**
- Create: `tests/e2e/conftest.py`, `tests/e2e/test_webhook_latency.py`
- Modify: `.github/workflows/ci.yml` (nightly job)

**Interfaces:**
- Consumes: `create_app` under uvicorn, `run_worker`, `simulation.Source.events`. A test sink writes `(key, received_at, applied_at)` to Postgres.
- Produces: fixture `stack(project)`, which runs a real server and worker against the test Postgres. Plans 5 and 6 reuse it.

- [ ] **Step 1: Write the failing tests**

```python
def test_webhook_to_sink_p95_within_5s(stack):  # FR-B10, NFR-01
    # 50 events/s for 60 s, poll 0.2 s
    assert lost == 0 and p95(latencies) <= 5.0

def test_burst_at_10x_loses_nothing_and_recovers(stack):  # H14
    # 50/s, then 500/s for 30 s, then 50/s
    assert lost == 0 and seconds_until_p95_recovers <= 300

def test_scheduled_flow_runs_by_itself(stack): ...  # every("5s"), with no external call
def test_restarting_the_server_keeps_scheduling(stack): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/e2e/test_webhook_latency.py -q -x`. Expected: FAIL (fixture missing).

- [ ] **Step 3: Implement the fixture and fix what the tests expose.** Likely tuning points: sealer interval, worker slots, and `FEATHER_ENGINE_POLL_S`. Record the measured p95 in the commit message. Plan 6, Task 9 repeats the latency check with the real webhook and HTTP connectors.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add tests .github && git commit -m "test: add webhook latency and burst checks"`

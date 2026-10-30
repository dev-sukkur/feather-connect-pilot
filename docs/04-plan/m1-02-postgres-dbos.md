# M1 Plan 2: Postgres storage and the DBOS runtime

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Flows run durably on Postgres and DBOS, and killing a worker with kill -9 mid-apply loses and duplicates nothing.

**Architecture:** Plan 1's `Store`, `Spool` and `Engine` ports get production implementations: `PostgresStore`, `PostgresSpool` and `DbosEngine`. A worker process loads a flow project, registers it in the catalogue and heartbeats. A sweeper recovers dead workers and re-submits orphaned work.

**Tech Stack:** psycopg 3 with pool, Alembic (raw SQL migrations), DBOS Transact 3.4.0 (pinned), PyYAML, Docker for the test Postgres.

**Spec:** [design v0.2](../02-design/2026-10-30-feather-connect-design.md) §5.6, §6.5, §6.8, §6.11, §6.12, §7; [ADR-0003](../adr/0003-durable-engine-selection-by-spike.md), [ADR-0005](../adr/0005-postgres-only-required-database.md). Spike code in `spikes/engine` shows working DBOS calls, but don't import it.

## Global Constraints

- Everything in Plan 1's Global Constraints still applies.
- Postgres ≥ 15 with **no extensions**. Tests run on Postgres 17 at `127.0.0.1:25433` (`deploy/compose.test.yaml`), using `FEATHER_TEST_DATABASE_URL`.
- All our tables live in schema `feather`, and every table has `tenant_id`. DBOS keeps its state in schema `dbos`.
- Migrations are forward-only.
- `feather_connect` imports psycopg and Alembic only under `storage/postgres/`, installed as the extra `feather-connect[postgres]`. DBOS is imported only in `feather-engine-dbos`.
- `dbos==3.4.0` is pinned. Use no DBOS private API, such as `_recover_pending_workflows`.
- `application_version` is fixed at `feather-1` (design §6.12).
- Spool chunks are zstd Parquet, 1–8 MB, in tables partitioned by day.
- Tests that need Postgres are marked `postgres`. Without a reachable database they fail with a message naming the compose file; they don't skip.

Paths are relative to `packages/feather-connect/` unless they start with `packages/`, `deploy/` or `tests/` at the root; the root ones say "root".

## Review Focus

1. A destination stays down longer than spool retention: the day's partition isn't dropped while a delivery on it is still open, and the retention run reports why (Task 9).
2. A partition stays unchanged for longer than retention: its latest snapshot survives, so the next run doesn't see every row as new (Tasks 2 and 9).
3. A worker is wrongly declared dead while still running: the same task runs twice, and the sink sees no duplicate effects (Task 10).
4. Postgres restarts while a task is running: the worker reconnects and the task completes (Task 10).
5. A project's `feather.yaml` names a missing module or an unknown secret provider: `load_project` fails with a message naming the file and key, and never prints a secret value (Task 5).

---

### Task 1: Test database and core migration

**Files:**
- Create (root): `deploy/compose.test.yaml`
- Create: `src/feather_connect/storage/postgres/__init__.py`, `storage/postgres/migrate.py`, `storage/postgres/migrations/env.py`, `storage/postgres/migrations/versions/0001_core.py`
- Modify: `pyproject.toml` (extra `postgres = ["psycopg[binary,pool]", "alembic"]`), root `.github/workflows/ci.yml` (Postgres 17 service on port 25433)
- Test: `tests/storage/postgres/conftest.py`, `tests/storage/postgres/test_migrations.py`

**Interfaces:**
- Produces: `migrate(database_url: str) -> None` (idempotent; also creates schema `feather`). Fixture `pg_url` creates a fresh database for each test session. Table list for `0001`: `tenant`, `run`, `task`, `task_log`, `project`, `flow`, `dataset`, `route`, `connection`, `version`, `run_slice`, `spool_version`, `spool_chunk`, `spool_snapshot`, `record_state`, `partition_status`, `delivery`, `delivery_attempt`, `quarantine`, `inbox`, `override`, `budget`, `budget_window`, `worker`. Columns come from design §7.1.
- Partitioned by day: `spool_chunk`, `inbox`. Partitioned by month: `task_log`, `delivery_attempt`. `version`, `task` and `quarantine` stay plain tables in M1, because they need ids that are unique across partitions, which Postgres can't enforce without the partition column; their retention uses batched deletes.

- [ ] **Step 1: Write the failing tests**

```python
pytestmark = pytest.mark.postgres

def test_migrate_creates_all_tables_and_is_idempotent(pg_url):
    migrate(pg_url); migrate(pg_url)
    assert EXPECTED_TABLES <= tables_in(pg_url, "feather")

def test_every_table_has_tenant_id(pg_url):  # FR-I1
    migrate(pg_url)
    assert columns_missing(pg_url, "feather", "tenant_id", except_={"alembic_version"}) == []

def test_no_extensions_are_needed(pg_url):  # NFR-07
    migrate(pg_url)
    assert extensions(pg_url) == {"plpgsql"}

def test_partial_index_on_open_deliveries_exists(pg_url):
    migrate(pg_url)
    assert "status <> 'applied'" in index_def(pg_url, "delivery_open_idx")
```

- [ ] **Step 2: Start Postgres, run the tests and confirm they fail.** Run: `docker compose -f deploy/compose.test.yaml up -d`, then `uv run pytest packages/feather-connect/tests/storage/postgres/test_migrations.py -q`. Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement the migration.** Write raw SQL through `op.execute`. Tune autovacuum on `delivery` and `budget_window` for frequent updates (design §7.4). Sequence `feather.seal_seq` backs `Version.seal_seq`.

- [ ] **Step 4: Run them and confirm they pass.** Same pytest command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add . && git commit -m "feat: add test Postgres and core schema migration"`

### Task 2: Postgres spool

**Files:**
- Create: `src/feather_connect/storage/postgres/spool.py`
- Test: `tests/storage/postgres/test_spool.py`

**Interfaces:**
- Consumes: `Spool`, `ChunkRef`, and the `StoreContract` chunk tests (Plan 1, Task 7).
- Produces: `PostgresSpool(pool, clock=None)` implementing `Spool`, and `ensure_partitions(days_ahead=7)`; `drop_day(day: date) -> None`. `snapshot` chunks go to the plain table `spool_snapshot`, so they survive day-partition drops. Every other kind goes to `spool_chunk`, under the day recorded for the version in `spool_version` the first time it writes.

- [ ] **Step 1: Write the failing tests**

```python
class TestPostgresSpool(StoreContract): ...  # inherits the chunk tests, fixture spool -> PostgresSpool

def test_chunk_is_zstd_parquet(spool, pg):
    spool.put_chunk("v1", 0, "data", TABLE)
    assert read_codec(pg, "v1") == "ZSTD"

def test_rewrite_across_midnight_stays_on_the_same_day(spool, clock):
    spool.put_chunk("v1", 0, "data", TABLE); clock.advance(timedelta(days=1))
    spool.put_chunk("v1", 0, "data", TABLE2)
    assert spool.get_chunk("v1", 0).num_rows == TABLE2.num_rows and count_chunks("v1") == 1

def test_snapshot_survives_dropping_its_day(spool, clock):  # Review Focus 2
    spool.put_chunk("v1", 0, "snapshot", SNAP); spool.put_chunk("v1", 0, "data", TABLE)
    spool.drop_day(clock.now().date())
    assert spool.get_chunk("v1", 0, "snapshot").num_rows == SNAP.num_rows
    assert spool.list_chunks("v1") == []

def test_missing_future_partition_is_created_on_write(spool, clock):
    clock.advance(timedelta(days=30)); spool.put_chunk("v9", 0, "data", TABLE)
    assert spool.get_chunk("v9", 0).num_rows == TABLE.num_rows
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/storage/postgres/test_spool.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `PostgresSpool`.** Write Parquet to a `BytesIO` with `compression="zstd"`, and store it as `bytea`. See `spikes/engine/spool_bench.py` for the working shape. `put_chunk` upserts on `(day, version_id, chunk_no, kind)`. Create a day partition with `create table if not exists ... partition of`, guarded by an advisory lock.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add Postgres spool"`

### Task 3: Postgres store

**Files:**
- Create: `src/feather_connect/storage/postgres/store.py`, `storage/postgres/connect.py`
- Test: `tests/storage/postgres/test_store.py`

**Interfaces:**
- Consumes: every repository protocol in `storage/port.py`; `StoreContract`.
- Produces: `PostgresStore(pool, clock=None, tenant="default") -> Store`; `connect(database_url, *, clock=None, tenant="default", min_size=1, max_size=10) -> tuple[Store, PostgresSpool]`.

- [ ] **Step 1: Write the failing tests**

```python
class TestPostgresStore(StoreContract): ...  # the whole contract must pass unchanged

def test_seal_is_one_transaction(store, monkeypatch):
    # make inserting the second delivery raise: no delivery exists and the version is still staged

def test_concurrent_seal_assigns_one_sequence(store):
    # 8 threads seal the same version: one seal_seq, one delivery per route

def test_concurrent_transition_applies_once(store):  # I3
    # 8 threads move running -> applied: exactly one succeeds, the rest raise InvalidTransition

def test_tenant_setting_is_set_per_transaction(store, pg):
    # inside a store call, current_setting('feather.tenant_id') == 'default'

def test_oldest_open_uses_the_partial_index(store, pg):
    # EXPLAIN shows delivery_open_idx with 10,000 applied rows present
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/storage/postgres/test_store.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `PostgresStore`.** Each method is one transaction, which starts with `select set_config('feather.tenant_id', %s, true)`. `transition` locks the row with `select ... for update`, then checks the legal moves shared with `MemoryStore`, so put that table in `storage/port.py`. Use `dict_row`. Timestamps come from the injected clock, so tests can freeze time.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add Postgres store"`

### Task 4: Budgets on Postgres

**Files:**
- Modify: `src/feather_connect/storage/postgres/store.py`
- Test: `tests/storage/postgres/test_budgets.py`

**Interfaces:**
- Consumes: `BudgetStore`, `BudgetSpec`, `Grant` (Plan 1, Task 14).
- Produces: `PostgresBudgets`, available as `store.budgets`.

- [ ] **Step 1: Write the failing test**

```python
def test_threads_never_exceed_the_window(store):
    store.budgets.define(BudgetSpec("z", parse_limits(["100/minute"])))
    with ThreadPoolExecutor(8) as pool:
        grants = list(pool.map(lambda _: store.budgets.acquire("z", 1).granted, range(400)))
    assert sum(grants) == 100

def test_all_windows_update_together_or_not_at_all(store): ...  # no window is charged when another refuses
def test_budget_survives_a_new_store_instance(pg_url): ...      # tokens live in Postgres
```

The `StoreContract` budget tests must also pass.

- [ ] **Step 2: Run it and confirm it fails.** Run: `uv run pytest packages/feather-connect/tests/storage/postgres/test_budgets.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement `acquire`.** Design §6.6 calls for one atomic statement: lock the budget's `budget_window` rows in a consistent order with `for update`, refill from `refilled_at`, check every window against the caller's priority limit, and update all or none, in one transaction. Leasing tokens in blocks is out of scope for M1.

- [ ] **Step 4: Run it and confirm it passes.** Run the new test and `uv run pytest packages/feather-connect/tests/storage/postgres/test_store.py -q`. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add Postgres budgets"`

### Task 5: Project loader, overlays and secrets

**Files:**
- Create: `src/feather_connect/project.py`
- Modify: `pyproject.toml` (add `pyyaml`)
- Test: `tests/test_project.py`

**Interfaces:**
- Consumes: `Flow`, `BudgetSpec`, `parse_limits`, `Problem`.
- Produces: `load_project(root: Path, env: str) -> Project`; `Project(name, root, env, flows, connections, budgets, settings, code_sha)`; `ConnectionConfig(name, connector, options, secret_refs, site, budget)`; `resolve_secret(ref: str) -> str`, for `${secret:env:NAME}` and `${secret:file:PATH}`; `effective_config(project) -> list[ConfigValue]`, where `ConfigValue(path, value, layer)` has secrets shown as `***`; `ProjectError`. Layers are as design §5.6: code defaults, `feather.yaml`, `env/<env>.yaml`. Tenant files come in M3, and runtime overrides in Plan 3.

- [ ] **Step 1: Write the failing tests**

```python
def test_later_layers_win(project_dir):
    p = load_project(project_dir, "prod")
    assert p.connections["warehouse"].options["database"] == "prod_bronze"
    assert p.connections["warehouse"].options["threads"] == 2  # from feather.yaml

def test_effective_config_names_the_layer_and_hides_secrets(project_dir, monkeypatch):
    monkeypatch.setenv("TOKEN", "s3cret")
    vals = {v.path: v for v in effective_config(load_project(project_dir, "prod"))}
    assert vals["connections.warehouse.options.database"].layer == "env/prod.yaml"
    assert vals["connections.warehouse.auth.token"].value == "***"

def test_secret_providers(tmp_path, monkeypatch):
    monkeypatch.setenv("A", "x"); (tmp_path / "s").write_text("y\n")
    assert resolve_secret("${secret:env:A}") == "x"
    assert resolve_secret(f"${{secret:file:{tmp_path/'s'}}}") == "y"

def test_unknown_secret_provider_names_the_key(project_dir):  # Review Focus 5
    with pytest.raises(ProjectError, match=r"connections\.bad.*vault"):
        load_project(project_dir, "bad-secret")

def test_missing_flow_module_names_the_file(project_dir):  # Review Focus 5
    with pytest.raises(ProjectError, match=r"feather\.yaml.*flows\.nope"):
        load_project(project_dir, "bad-module")

def test_secret_value_never_appears_in_errors_or_repr(project_dir, monkeypatch): ...
def test_overlay_can_disable_a_route_and_change_a_trigger(project_dir): ...
def test_code_sha_comes_from_env_then_git(project_dir, monkeypatch): ...  # FEATHER_CODE_SHA first
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/test_project.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `project.py`.** Add `root` to `sys.path` while importing flow modules, and collect every `Flow` found in each module's globals. Resolve secrets lazily, only when a connection is used, so `load_project` doesn't need secret values. Vault and cloud providers raise a `ProjectError` saying they arrive in M3.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add project loader, overlays and secrets"`

### Task 6: DbosEngine

**Files:**
- Create: `packages/feather-engine-dbos/pyproject.toml`, `packages/feather-engine-dbos/src/feather_engine_dbos/__init__.py`, `engine.py`, `runtime.py`
- Test: `packages/feather-engine-dbos/tests/test_mapping.py`

**Interfaces:**
- Consumes: `Engine`, `WorkerRuntime`, `TaskCall`, `TaskContext`, `TaskFn`, `RetryPolicy`, `PermanentError`.
- Produces:
  - `DbosEngine(database_url, *, schema="dbos")`, implementing `Engine`. `submit` maps per design §6.8 (no concurrency key goes to `feather-free`, a key to `feather-keyed` with `queue_partition_key`; workflow id is the idempotency key, with reuse policy `return-existing`; port priority 2, 1, 0 becomes DBOS 1, 2, 3). `signal` is `client.send(ref, payload, topic=key)`. `cancel` is `client.cancel_workflow`. `recover_worker(worker_id)` calls `list_workflows(status="PENDING", executor_id=worker_id, load_input=False, load_output=False)`, then `resume_workflow(id, queue_name=w.queue_name)`, and returns the ids. `health()` returns queue depths.
  - `DbosWorkerRuntime(database_url, *, poll_s=0.5, schema="dbos", application_version="feather-1")`, implementing `WorkerRuntime`. `run(tasks, worker_id, labels, slots)` configures DBOS with `executor_id=worker_id`, calls `listen_queues`, `launch` and `register_queue` (`partition_concurrency=1` on keyed, `worker_concurrency=slots`, `polling_interval_sec=poll_s`), and returns a handle with `stop()`.
  - Workflow `feather_task(task, args, retry)`: the loop in `spikes/engine/spike/dbos_engine.py`, with retries using `DBOS.sleep(policy.delay(attempt))`.
  - `_DbosContext` implements `TaskContext`. `step(name, fn)` keeps a per-execution dict of succeeded step names, so a retry attempt returns saved results without creating a new DBOS step. Otherwise it calls `DBOS.run_step(StepOptions(name=name), fn)`.
  - Pool names: a label `pool=x` adds the suffix `@x` to the queue names.

- [ ] **Step 1: Write the failing tests** (no worker running, submit side only)

```python
pytestmark = pytest.mark.postgres

def test_submit_maps_key_priority_and_queue(engine, sys_db):
    ref = engine.submit(TaskCall("t", {"a": 1}, idempotency_key="k1", concurrency_key="ck", priority=2))
    row = workflow_row(sys_db, ref)
    assert (row.queue_name, row.queue_partition_key, row.priority) == ("feather-keyed", "ck", 1)

def test_submit_without_key_uses_free_queue(engine, sys_db): ...
def test_same_idempotency_key_returns_same_ref(engine): ...
def test_pool_label_changes_the_queue(engine, sys_db): ...
def test_args_over_64kb_are_refused(engine):  # I8
    with pytest.raises(ValueError, match="64"):
        engine.submit(TaskCall("t", {"a": "x" * 70_000}, idempotency_key="k"))
def test_no_private_dbos_api_is_used():
    src = Path(feather_engine_dbos.__file__).parent.rglob("*.py")
    assert not any("._recover" in p.read_text() or "DBOS._" in p.read_text() for p in src)
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-engine-dbos/tests -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement the package.** Dependencies: `feather-connect[postgres]` and `dbos==3.4.0`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-engine-dbos pyproject.toml uv.lock && git commit -m "feat: add DbosEngine"`

### Task 7: Engine conformance suite

**Files:**
- Create (root): `tests/engine_conformance/conftest.py`, `tests/engine_conformance/worker_main.py`, `tests/engine_conformance/test_conformance.py`
- Modify (root): `.github/workflows/ci.yml` (nightly job and on changes to `packages/feather-engine-dbos`)

**Interfaces:**
- Consumes: `DbosEngine`, `DbosWorkerRuntime`, `TaskCall`, `RetryPolicy`.
- Produces: fixture `workers` with `start(name, slots=4, env=None) -> float`, `kill(name)` (Windows: `taskkill /F /T /PID`; Linux: SIGKILL), and `stop_all()`. Tests log executions in a table `conformance.exec_log`.

- [ ] **Step 1: Write the failing tests.** These are design §6.8's list. `spikes/engine/tests` has a working version of each, adapted here to the product port.

```python
def test_1_priority_and_one_at_a_time_per_key(...):   # no overlap; priority order 2,2,2,1,1,1,0,0,0
def test_2_kill_mid_step_no_loss_no_duplicates(...):  # effects exactly once; completed chunk not redone
def test_3_deploy_same_version_mid_drain(...):        # 50 of 50 done, none duplicated, under version feather-1
def test_3b_rollback_of_latest_version_resumes_dequeue(...):  # set_latest_application_version works
def test_4_dedup(...):                                # 5 concurrent + 5 sequential submits, one run
def test_5_durable_sleep_across_restart(...):         # 20 s sleep, killed at 5 s, elapsed in [20, 24]
def test_6_retry_timing(...):                         # gaps within [0.5, 1.1] of the policy; permanent: 1 attempt
def test_6b_succeeded_step_not_rerun_on_retry(...):   # step counter stays 1 across attempts
def test_7_schema_isolation(...):                     # DDL on feather finishes < 2 s while a task runs; no locks
def test_8_two_workers_share(...):                    # 200 tasks exactly once; both workers ran some
def test_9_shared_budget(...):                        # ≤ 10 per window; high ≥ 6 in saturated windows
def test_10_dispatch_latency(...):                    # 25 tasks/s for 30 s at poll 0.2: p95 ≤ 1 s, none lost
def test_11_gateway_relay(...):                       # wait_signal resumes within 1 s of signal
def test_12_recover_worker_keeps_key_order(...):      # T2d: key held until recovered; next task waits its turn
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/engine_conformance -q -x`. Expected: FAIL (fixtures missing).

- [ ] **Step 3: Implement the fixtures and worker entry point.** Follow `spikes/engine/tests/conftest.py`. Use host processes by default, and containers with `FEATHER_TEST_WORKERS=docker`.

- [ ] **Step 4: Run them and confirm they pass.** Run: `uv run pytest tests/engine_conformance -q`, with a 15-minute timeout. Expected: all pass. T10's target rate is 25 tasks/s, because the design's 0.5 s default caps a 16-slot worker at 32.

- [ ] **Step 5: Commit.** `git add tests .github && git commit -m "test: add engine conformance suite"`

### Task 8: Worker process and catalogue

**Files:**
- Create: `src/feather_connect/worker.py`
- Modify: `storage/port.py`, `storage/memory.py`, `storage/postgres/store.py`, `testing/store_contract.py` (add `CatalogStore`, `WorkerRegistry`)
- Test (root): `tests/e2e/test_worker_process.py`

**Interfaces:**
- Consumes: `load_project`, `Runtime`, `connect`, `WorkerRuntime`.
- Produces in `storage/port.py`: `CatalogStore`: `sync(project: str, code_sha: str, specs: Mapping[str, Any])`, `flows() -> list[FlowRow]`, `flow(name) -> FlowRow`, `routes() -> list[RouteRow]`. `WorkerRegistry`: `heartbeat(worker_id, labels, code_sha, slots)`, `list_workers() -> list[WorkerRow]`, `stale(older_than: timedelta) -> list[WorkerRow]`, `mark(worker_id, state)`, where `state` is `alive`, `draining` or `dead`.
- Produces in `worker.py`: `run_worker(project_root, env, *, database_url, worker_id=None, slots=8, labels=None, poll_s=0.5)`. `worker_id` defaults to `FEATHER_WORKER_ID`, then the hostname. Heartbeat every 5 s from a daemon thread. Registers specs at startup, with each flow's `spec_dict()`: add that method to `core/flow.py`, returning a JSON-safe description of datasets, routes, triggers and owner. On SIGTERM (or `CTRL_BREAK_EVENT` on Windows) it marks itself `draining`, stops taking work, waits up to 30 s for the current chunk, and exits 0.

- [ ] **Step 1: Write the failing tests**

```python
# In StoreContract
def test_catalog_keeps_specs_per_sha(self, store): ...
def test_stale_lists_workers_past_the_timeout(self, store, clock): ...

# In tests/e2e/test_worker_process.py
def test_worker_registers_catalogue_and_heartbeats(demo_project, pg_url):
    start_worker(demo_project, pg_url, "w1")
    wait_until(lambda: store.catalog.flow("demo").code_sha == "testsha")
    assert store.workers.list_workers()[0].worker_id == "w1"

def test_a_run_completes_through_a_real_worker(...):
    run = runtime_client.start_run("demo")
    wait_until(lambda: store.runs.get_run(run).status == "succeeded", timeout=60)
    assert sim_sink_rows(demo_project) == EXPECTED

def test_sigterm_drains_and_exits_zero(...): ...
def test_workers_with_different_shas_are_visible(...): ...  # both rows are listed with their SHA
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/e2e/test_worker_process.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `worker.py` and the two repositories.** `make_context` resolves a connection's config, builds its `BudgetHandle`, and passes `Project.settings`. Connection config is bound to source and sink objects by their `connection` name.

- [ ] **Step 4: Run them and confirm they pass.** Same command, plus `uv run pytest packages/feather-connect/tests -q`. Expected: all pass.

- [ ] **Step 5: Commit.** `git add . && git commit -m "feat: add worker process and catalogue"`

### Task 9: Sweeper and retention

**Files:**
- Create: `src/feather_connect/runtime/sweeper.py`, `runtime/retention.py`
- Modify: `storage/port.py`, `storage/memory.py`, `storage/postgres/store.py`, `testing/store_contract.py` (add the queries below)
- Test: `tests/runtime/test_sweeper.py`, `tests/runtime/test_retention.py`

**Interfaces:**
- Consumes: `Store`, `Engine.recover_worker`, `Runtime.submit_apply`, `Runtime.submit_seal`.
- Produces: `sweep_once(store, runtime, engine, *, now, dead_after=timedelta(seconds=30), pending_after=timedelta(minutes=1)) -> SweepReport(dead_workers, recovered_tasks, resubmitted_deliveries, resubmitted_seals)`. New store queries: `Ledger.pending_older_than(age) -> list[Delivery]`; `VersionStore.runs_ready_to_seal() -> list[tuple[run_id, dataset]]`.
- Produces: `retention_once(store, spool, *, now, keep_days=14) -> RetentionReport(dropped_days, kept_days: Mapping[date, str], deleted_runs)`. Chunk days older than `keep_days` are dropped only if no version on that day has an open delivery. Older snapshots are deleted, keeping the latest per (dataset, partition). Per design §7.3: runs, tasks and deliveries 180 days; task logs 30 days; version manifests 400 days; inbox 7 days after sealing.

- [ ] **Step 1: Write the failing tests** (on `MemoryStore`, `LocalEngine` and a `FrozenClock`, plus Postgres subclasses of the retention tests)

```python
def test_dead_worker_is_marked_and_recovered(): ...
def test_live_worker_is_left_alone(): ...
def test_worker_that_returns_is_marked_alive_again(): ...
def test_pending_delivery_with_no_active_task_is_resubmitted():  # outbox
    # seal, then "crash" before submit; after pending_after, the sweep submits and the delivery applies
def test_resubmit_is_safe_when_a_task_exists(): ...  # the same idempotency key, so one execution
def test_run_ready_to_seal_is_resubmitted(): ...
def test_blocked_deliveries_are_not_resubmitted(): ...  # failed and held stay as they are

def test_retention_drops_a_day_with_nothing_open(): ...
def test_retention_keeps_a_day_with_an_open_delivery():  # Review Focus 1
    r = retention_once(store, spool, now=T + timedelta(days=30))
    assert r.dropped_days == [] and "open delivery" in r.kept_days[T.date()]
def test_retention_keeps_the_latest_snapshot_of_an_unchanged_partition():  # Review Focus 2
    # after 30 days, the next snapshot_diff run finds no changes
def test_retention_removes_old_runs_and_logs(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_sweeper.py packages/feather-connect/tests/runtime/test_retention.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement both.** Each is a pure function over its arguments. Plan 3, Task 1 runs them on a leader-elected loop. Resubmitting uses the same idempotency key (Plan 1, Task 11), so an existing task isn't duplicated.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add sweeper and retention"`

### Task 10: Chaos and performance checks

**Files:**
- Create (root): `tests/chaos/conftest.py`, `tests/chaos/test_kill_mid_apply.py`, `tests/perf/test_spool_throughput.py`
- Modify (root): `.github/workflows/ci.yml` (nightly job)

**Interfaces:**
- Consumes: `workers` fixture (Task 7), `run_worker`, `sweep_once`, `simulation.Source`. A `SlowSink` test connector writes each key to a Postgres table with `on conflict do nothing`, and counts attempts.

- [ ] **Step 1: Write the failing tests**

```python
def test_kill_9_mid_apply_loses_and_duplicates_nothing(...):  # M1 exit criterion
    # 3 routes, 5 chunks each; SIGKILL the worker while chunk 2 of a route is applying; restart with the same id
    assert sink_rows("a") == sink_rows("b") == sink_rows("c") == EXPECTED_KEYS
    assert ledger_all_applied() and max_writes_per_key() == 1

def test_kill_9_with_no_restart_is_recovered_by_the_sweeper(...):
    # another worker is running; sweep_once declares the first dead and its work finishes

def test_worker_wrongly_declared_dead_causes_no_duplicates(...):  # Review Focus 3
    # call recover_worker on a live worker mid-apply; both executions run, and writes per key stay 1

def test_kill_during_extract_seals_one_clean_version(...):
    # restart rewrites chunks: one sealed version, row counts match

def test_postgres_restart_during_apply(...):  # Review Focus 4
    # docker restart the database mid-apply; the worker reconnects and everything ends applied

# tests/perf/test_spool_throughput.py
def test_extract_to_spool_meets_20k_rows_per_second(...):  # NFR-02
    # 500,000 simulation rows through run_extract on Postgres, one worker
    assert rows_per_second >= 20_000
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/chaos tests/perf -q -x`. Expected: FAIL (fixtures missing).

- [ ] **Step 3: Implement the fixtures and any fixes the tests expose.** Reconnect behaviour may need the pool's `reconnect_failed` settings and retry on `OperationalError` in `PostgresStore`. If the extract path falls short of NFR-02, profile it before changing anything; per-row hashing in Python is the likely cost, and it is allowed to batch.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass. Record the measured rows per second in the commit message.

- [ ] **Step 5: Commit.** `git add tests .github && git commit -m "test: add chaos and spool performance checks"`

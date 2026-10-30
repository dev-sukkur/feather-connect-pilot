# M1 Plan 4: Connectors and templates

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The M1 connector catalogue and five generic templates, each passing one conformance kit.

**Architecture:** A conformance kit defines what every connector must do, as test base classes that connector packages subclass. Each connector is a separate distribution that installs into the `feather_connect.connectors` namespace and registers an entry point. HTTP-based connectors share one client that handles retries, budget metering and log redaction.

**Tech Stack:** httpx, respx, psycopg, fsspec with s3fs, moto, duckdb, pyarrow.

**Spec:** [design v0.2](../02-design/2026-10-30-feather-connect-design.md) §8 (all), §13.3, §6.10. Consumes Plan 1, and Plan 3 for the webhook source.

## Global Constraints

- Everything in Plan 1's Global Constraints still applies.
- Connector packages live in `packages/connectors/feather-<name>/`, with source at `src/feather_connect/connectors/<module>.py` and **no** `__init__.py` in `feather_connect/` or `connectors/` inside those packages.
- Each package registers its classes under the entry point group `feather_connect.connectors`.
- Sinks are idempotent per key. Applying the same batch twice changes nothing.
- Secret config fields are typed `fc.Secret` and never appear in logs, errors or `repr`.
- Every connector and template has a README with a runnable example, tested in CI (NFR-14).
- REST, GraphQL and HTTP support API-key, basic, bearer and OAuth2 (client credentials and refresh token) auth.
- Connector maturity starts at `experimental`, and becomes `beta` when its conformance subclass passes.

Paths are relative to the repository root.

## Review Focus

1. An API's pagination repeats a page or never ends: the read stops with a clear error naming the page, instead of looping forever (Task 5).
2. An OAuth2 access token expires in the middle of a read: the client refreshes once and continues, and the read doesn't fail (Task 5).
3. A record has a column the destination table doesn't have, or a table or column name contains odd characters: names are always quoted, and a missing column fails the version with a message naming the column (Tasks 3 and 8).
4. A CSV has a byte-order mark, a non-UTF-8 encoding, or a short row: the encoding is configurable, the BOM is handled, and bad rows are quarantined without stopping the file (Task 4).
5. A file appears while it is still being written, or a matching path has no files: partial files are skipped until they are stable, and an empty match is treated as an empty read (Task 4).

---

### Task 1: Conformance kit and connector registry

**Files:**
- Create: `packages/feather-connect/src/feather_connect/testing/conformance.py`, `packages/feather-connect/src/feather_connect/core/registry.py`
- Modify: `packages/feather-connect/src/feather_connect/core/connector.py` (add `maturity` to `ConnectorSpec`; add `fc.Secret`)
- Test: `packages/feather-connect/tests/connectors/test_memory_conformance.py`, `tests/connectors/test_simulation_conformance.py`, `tests/core/test_registry.py` under the same package

**Interfaces:**
- Consumes: `Source`, `Sink`, `ReadRequest`, `ApplyBatch`, `BudgetHandle`.
- Produces in `testing/conformance.py`: `class SourceConformance`, needing fixtures `source`, `ctx` and `seed(rows)`. `class SinkConformance`, needing fixtures `sink`, `ctx` and `read_back() -> dict[key, payload]`. Tests in the kit: `test_spec_is_valid`, `test_check_reports_ok`, `test_discover_lists_a_stream`, `test_read_returns_seeded_rows`, `test_resume_continues_from_a_checkpoint` (skipped unless `spec.resumable`), `test_bounded_memory_on_large_read`, `test_apply_twice_changes_nothing`, `test_delete_removes_the_key`, `test_modes_declared_in_spec_work`, `test_retryable_errors_raise_transient`, `test_permanent_errors_raise_permanent`, `test_calls_are_metered_when_a_budget_is_set`.
- Produces in `core/registry.py`: `list_connectors() -> list[ConnectorInfo]`, `describe_connector(name) -> ConnectorInfo`, with `ConnectorInfo(name, kind, spec, config_schema: dict, maturity, package)`.

- [ ] **Step 1: Write the failing tests.** Subclass the kit for `memory` and `simulation`. In `test_registry.py`: `test_builtin_connectors_are_listed`; `test_describe_returns_the_config_json_schema`; `test_third_party_entry_point_is_found`, which installs a tiny test distribution from `tests/fixtures/thirdparty_connector` using `importlib.metadata` monkeypatching (FR-J4).

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/connectors packages/feather-connect/tests/core/test_registry.py -q`. Expected: FAIL with `ImportError`.

- [ ] **Step 3: Implement the kit and registry.** `test_bounded_memory_on_large_read` reads 200,000 rows and uses `tracemalloc` to check peak growth stays under 64 MB. Fix anything the kit finds in `memory` or `simulation`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add connector conformance kit and registry"`

### Task 2: Shared HTTP client

**Files:**
- Create: `packages/feather-connect/src/feather_connect/runtime/http.py`
- Modify: `packages/feather-connect/pyproject.toml` (add `httpx`), `core/context.py` (wire `Context.http`), `runtime/runtime.py` (`make_context`)
- Test: `packages/feather-connect/tests/runtime/test_http.py`

**Interfaces:**
- Consumes: `BudgetHandle`, `TransientError`, `PermanentError`.
- Produces: `FeatherHttp(*, base_url=None, auth=None, budget=None, cost: Mapping[str, int] = {}, retry=HttpRetry(), secrets: Sequence[str] = (), log=None)`, with `request(method, url, *, cost_key="default", **kw) -> httpx.Response` and `close()`. `HttpRetry(attempts=6, first_delay=1.0, max_delay=60.0, jitter=True)`. `Auth` classes: `ApiKey(header, value)`, `Basic(user, password)`, `Bearer(token)`, `OAuth2ClientCredentials(token_url, client_id, client_secret, scope=None)`, `OAuth2RefreshToken(token_url, client_id, client_secret, refresh_token)`. `redact(text, secrets) -> str`.

- [ ] **Step 1: Write the failing tests** (using `respx`)

```python
def test_429_honours_retry_after_seconds_and_http_date(): ...  # sleep called with the header's value
def test_5xx_and_connection_errors_back_off_and_retry(): ...
def test_exhausted_retries_raise_transient_with_retry_after(): ...
def test_4xx_raises_permanent_with_a_redacted_excerpt():
    with pytest.raises(PermanentError) as e: client.request("GET", "/x")
    assert "s3cret" not in str(e.value) and "400" in str(e.value)
def test_every_call_is_metered_with_its_cost(): ...  # cost_key "detail" costs 2 units
def test_budget_deferred_propagates(): ...
def test_secret_values_and_bearer_tokens_never_reach_logs(caplog): ...
def test_oauth2_token_is_fetched_cached_and_refreshed_on_401():  # Review Focus 2
    # first call 401 with an expired token: one refresh, one retry, then success
def test_oauth2_second_401_is_permanent(): ...
def test_timeout_is_transient(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/runtime/test_http.py -q`. Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Implement `runtime/http.py`.** Default timeout 30 s. Backoff uses equal jitter, as `RetryPolicy.delay` does. Redaction also matches common token patterns (`Bearer \S+`, `eyJ[\w-]+\.[\w-]+`).

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/feather-connect && git commit -m "feat: add shared HTTP client with retries, metering and redaction"`

### Task 3: Postgres connector

**Files:**
- Create: `packages/connectors/feather-postgres/pyproject.toml`, `README.md`, `src/feather_connect/connectors/postgres.py`
- Test: `packages/connectors/feather-postgres/tests/test_postgres.py`

**Interfaces:**
- Consumes: `SourceConformance`, `SinkConformance`, `ReadRequest.scheme.bounds`, `Context`.
- Produces: `postgres.Query(connection, *, sql=None, table=None, batch_rows=10_000)`, supporting `by_stamp`, `snapshot_diff` and `replace`. `postgres.Table(connection, *, table, key_columns, partition_columns=())`, supporting `upsert`, `append` and `replace_partition`. Connection config: `dsn` (`fc.Secret`), `schema`.

- [ ] **Step 1: Write the failing tests.** Subclass both conformance classes. Add:

```python
def test_bounds_and_since_are_pushed_down_as_parameters(pg, capture_sql): ...
def test_server_side_cursor_is_used_for_large_reads(pg): ...
def test_upsert_on_composite_key(pg): ...
def test_append_keeps_fc_columns(pg): ...
def test_replace_partition_is_atomic_across_chunks(pg):
    # fail on chunk 2: the old slice is untouched. Succeed: the new slice is complete.
def test_moved_from_removes_the_old_row_in_replace_partition(pg): ...
def test_identifiers_are_quoted(pg):  # Review Focus 3
    # table "order-lines" and column "select" work, and a table name with ; is treated as a name
def test_unknown_column_fails_the_version_naming_it(pg):  # Review Focus 3
    with pytest.raises(PermanentError, match="no_such_col"): sink.apply(ctx, batch_with({"no_such_col": 1}))
def test_constraint_violation_on_one_row_rejects_only_that_row(pg): ...
def test_connection_error_is_transient(): ...
def test_readme_example_runs(pg): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/connectors/feather-postgres/tests -q`. Expected: FAIL.

- [ ] **Step 3: Implement the connector.** Use `psycopg.sql.Identifier` for every name. Upsert uses `insert ... on conflict (keys) do update`, inside a savepoint per row only after a batch fails, so one bad row is isolated without slowing the normal path. `replace_partition` loads into an unlogged staging table for the version, and swaps inside one transaction on `is_last`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/connectors/feather-postgres pyproject.toml uv.lock && git commit -m "feat: add Postgres connector"`

### Task 4: S3-compatible files connector

**Files:**
- Create: `packages/connectors/feather-s3/pyproject.toml`, `README.md`, `src/feather_connect/connectors/s3.py`
- Test: `packages/connectors/feather-s3/tests/test_s3.py`

**Interfaces:**
- Produces: `s3.Files(connection, *, path, format: Literal["csv", "json", "jsonl", "parquet"], encoding="utf-8", delimiter=",", stable_after="60s", batch_rows=10_000)`, supporting `replace` and `by_stamp` (object modified time). `s3.PartitionFiles(connection, *, path_template, format)`, supporting `replace_partition` and `append`, which writes one file per partition. `path` accepts `s3://`, `file://` and `memory://`. Azure Blob and GCS work through fsspec extras and stay `experimental`.

- [ ] **Step 1: Write the failing tests.** Subclass the conformance classes over `memory://`. Add:

```python
@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl", "parquet"])
def test_each_format_reads(fmt, files): ...
def test_csv_with_bom_and_latin1(files):  # Review Focus 4
    assert rows[0]["name"] == "Müller"
def test_short_csv_row_is_quarantined_and_the_file_continues(files):  # Review Focus 4
def test_by_stamp_reads_only_new_objects(files): ...
def test_recently_modified_file_is_skipped_until_stable(files, clock):  # Review Focus 5
def test_no_matching_files_is_an_empty_read(files): ...  # Review Focus 5; the empty-read guard then applies
def test_partition_files_are_replaced_atomically(files): ...  # write to a temp key, then move
def test_works_against_moto_s3(moto_server): ...
def test_credentials_stay_out_of_errors(moto_server): ...
def test_readme_example_runs(files): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/connectors/feather-s3/tests -q`. Expected: FAIL.

- [ ] **Step 3: Implement the connector.** Read through `fsspec.open` streams with `pyarrow.csv`, `pyarrow.json` and `pyarrow.parquet`, batch by batch. The CSV reader uses `utf-8-sig` when the encoding is `utf-8`, and `invalid_row_handler` to route short rows to quarantine. A stamp's value is the object's modified time, added as column `_fc_object_mtime`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/connectors/feather-s3 pyproject.toml uv.lock && git commit -m "feat: add S3-compatible files connector"`

### Task 5: REST connector

**Files:**
- Create: `packages/connectors/feather-http/pyproject.toml`, `README.md`, `src/feather_connect/connectors/http.py`
- Test: `packages/connectors/feather-http/tests/test_rest.py`

**Interfaces:**
- Consumes: `FeatherHttp`, `Auth` classes.
- Produces: `http.Rest(connection, *, path, records_path=None, pagination=None, params=None, stamp_param=None, cost_key="default")`, supporting `by_stamp`, `replace` and `snapshot_diff`. Pagination classes: `Offset(limit_param, offset_param, page_size)`, `Page(page_param, size_param, page_size, start=1)`, `Cursor(cursor_param, next_path)`, `Link()` (RFC 8288). `http.Request(connection, *, path, method="POST", body=None, key_header=None)`, a sink supporting `upsert`, where `body` is a callable or a template, and `key_header` sends the idempotency key. Connection config: `base_url`, `auth`, `budget`.

- [ ] **Step 1: Write the failing tests.** Subclass the conformance classes with `respx`. Add:

```python
@pytest.mark.parametrize("pagination", [OFFSET, PAGE, CURSOR, LINK])
def test_each_pagination_style_reads_every_page(pagination, api): ...
def test_resume_continues_from_the_saved_cursor(api): ...
def test_repeated_page_stops_with_a_clear_error(api):  # Review Focus 1
    with pytest.raises(PermanentError, match=r"page.*repeat"): list(read(api_repeating))
def test_page_limit_stops_a_runaway_cursor(api): ...  # default max_pages=100_000, Review Focus 1
def test_stamp_param_sends_the_watermark(api): ...
def test_records_path_digs_into_the_response(api): ...
def test_non_json_response_is_permanent_with_excerpt(api): ...
def test_token_expiry_mid_read_refreshes_and_continues(api):  # Review Focus 2
def test_sink_sends_the_idempotency_key(api): ...
def test_sink_4xx_on_one_record_rejects_that_record(api): ...
def test_sink_5xx_is_transient(api): ...
def test_readme_example_runs(api): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/connectors/feather-http/tests/test_rest.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement `http.py`.** Use `Context.checkpoint` to save the cursor after each page. Compare a hash of each page's body with the previous page to detect repeats.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/connectors/feather-http pyproject.toml uv.lock && git commit -m "feat: add REST connector"`

### Task 6: GraphQL connector

**Files:**
- Modify: `packages/connectors/feather-http/src/feather_connect/connectors/http.py`, `README.md`
- Create: `packages/connectors/feather-http/src/feather_connect/connectors/graphql.py`
- Test: `packages/connectors/feather-http/tests/test_graphql.py`

**Interfaces:**
- Consumes: `FeatherHttp`, conformance classes.
- Produces: `graphql.Source(connection, *, query, connection_path, page_size=100, variables=None, stamp_variable=None)`, walking `edges/node` and `pageInfo`. `graphql.Mutation(connection, *, mutation, variables: Callable[[ApplyRecord], dict])`, a sink.

- [ ] **Step 1: Write the failing tests.** Subclass the conformance classes with `respx`. Add `test_walks_cursor_connections`, `test_graphql_errors_with_http_200_raise_permanent`, `test_rate_limit_error_code_is_transient`, `test_partial_data_with_errors_fails_the_read`, `test_mutation_user_error_rejects_that_record`, `test_readme_example_runs`.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/connectors/feather-http/tests/test_graphql.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement `graphql.py`.** A GraphQL `errors` array is checked on every 200 response.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/connectors/feather-http && git commit -m "feat: add GraphQL connector"`

### Task 7: Webhook source and outbound HTTP sink

**Files:**
- Modify: `packages/connectors/feather-http/src/feather_connect/connectors/http.py`, `README.md`
- Test: `packages/connectors/feather-http/tests/test_webhook_call.py`

**Interfaces:**
- Consumes: `PushSource`, `InboundRequest`, `verify_hmac` (Plan 3, Task 3).
- Produces: `http.Webhook(connection, *, signature_header="X-Signature", timestamp_header="X-Timestamp", secret_ref, records_path=None, event_id_path=None, verify=None)`, with `verify` allowing a custom scheme. `http.Call(connection, *, method, url, body=None, headers=None, response_map=None, key_header="Idempotency-Key")`, a sink for generic outbound calls: `url` and `body` are templates or callables, and `response_map` records fields from the response into the delivery attempt.

- [ ] **Step 1: Write the failing tests**

```python
def test_valid_signature_passes_and_bad_raises(): ...
def test_custom_verify_scheme_is_used(): ...
def test_receive_splits_a_batch_payload_with_records_path(): ...
def test_event_id_comes_from_the_body_or_header(): ...
def test_call_renders_url_and_body_from_the_record(respx_mock): ...
def test_call_sends_the_idempotency_key(respx_mock): ...
def test_call_classifies_responses(respx_mock): ...  # 2xx ok; 409 ok when already applied; 4xx rejects; 5xx transient
def test_response_map_is_recorded(respx_mock): ...
def test_webhook_to_call_end_to_end_under_the_harness(): ...
def test_readme_example_runs(): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/connectors/feather-http/tests/test_webhook_call.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement both classes.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/connectors/feather-http && git commit -m "feat: add webhook source and outbound HTTP sink"`

### Task 8: DuckDB and MotherDuck connector

**Files:**
- Create: `packages/connectors/feather-duckdb/pyproject.toml`, `README.md`, `src/feather_connect/connectors/duckdb.py`
- Test: `packages/connectors/feather-duckdb/tests/test_duckdb.py`

**Interfaces:**
- Produces: `duckdb.Query(connection, *, sql)`, supporting all strategies except `push`. `duckdb.Table(connection, *, table, key_columns=(), partition_columns=())`, supporting `append`, `upsert` and `replace_partition`. Connection config: `database` (a file path, or `md:<name>`), `token` (`fc.Secret`, for MotherDuck), `threads`. Writes are Arrow-based.

- [ ] **Step 1: Write the failing tests.** Subclass the conformance classes over a temporary file. Add `test_append_keeps_fc_columns`, `test_upsert_on_composite_key`, `test_replace_partition_is_atomic`, `test_new_column_is_added_on_append`, `test_identifiers_are_quoted` (Review Focus 3), `test_unknown_column_on_upsert_names_it` (Review Focus 3), `test_long_write_is_split_into_chunks`, `test_token_never_appears_in_errors`, `test_readme_example_runs`, and `test_motherduck_round_trip`, which is skipped unless `MOTHERDUCK_TOKEN` is set and runs nightly.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/connectors/feather-duckdb/tests -q`. Expected: FAIL.

- [ ] **Step 3: Implement the connector.** Register each batch as an Arrow view and use `insert into ... select`. Upsert uses `insert or replace` when the table has the key as its primary key, and otherwise `merge`. Open one connection per `apply` call.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages/connectors/feather-duckdb pyproject.toml uv.lock && git commit -m "feat: add DuckDB and MotherDuck connector"`

### Task 9: Five generic templates

**Files:**
- Create: `templates/db-to-warehouse/`, `templates/api-to-warehouse/`, `templates/file-drop-to-db/`, `templates/webhook-to-api/`, `templates/fan-out-demo/`, each holding `feather.yaml`, `env/dev.yaml`, `env/qa.yaml`, `env/uat.yaml`, `env/prod.yaml`, `flows/`, `fixtures/`, `tests/`, `README.md`
- Create: `tests/templates/test_templates.py`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: all connectors above, `simulation`, `FlowHarness`, `load_project`.
- Produces: `fan-out-demo` is JSON simulation → Postgres + DuckDB + HTTP, the `feather dev` demo that Plan 6 uses. The `dev` and `qa` overlays use simulation connectors in every template.

- [ ] **Step 1: Write the failing tests**

```python
@pytest.mark.parametrize("name", TEMPLATES)
def test_template_has_every_required_file(name): ...
@pytest.mark.parametrize("name", TEMPLATES)
def test_template_validates_for_every_environment(name):
    for env in ("dev", "qa", "uat", "prod"):
        assert load_project(TEMPLATES_DIR / name, env).flows
@pytest.mark.parametrize("name", TEMPLATES)
def test_template_own_tests_pass(name):
    assert subprocess.run(["uv", "run", "pytest", str(TEMPLATES_DIR / name / "tests"), "-q"]).returncode == 0
def test_fan_out_demo_reaches_three_destinations(): ...  # under FlowHarness
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/templates -q`. Expected: FAIL.

- [ ] **Step 3: Write the templates.** Flow files follow design §5.2–§5.4. Each README says what the flow does, how to run it with `feather run --local --sim`, and what to change for real systems. Fixture packs use the retail vocabulary from design §8.5.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add templates tests .github && git commit -m "feat: add five generic flow templates"`

# M1 Plan 5: Control plane and HTML dashboard

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An operator signs in to a server-rendered HTML dashboard, sees what is failing, and re-runs, retries, skips and replays with a preview, without a CLI or SQL.

**Architecture:** One service layer (`feather_server.services`) holds every operation. The JSON API and the HTML pages are thin adapters over it, so validation, previews and audit are identical. Pages are Jinja2 templates with one hand-written CSS file and vendored HTMX, with no Node toolchain.

**Tech Stack:** FastAPI, Jinja2, HTMX (vendored), argon2-cffi, itsdangerous, Playwright for Python, axe-playwright-python.

**Spec:** [design v0.2](../02-design/2026-10-30-feather-connect-design.md) §9, §10, §13.1, §13.3, §15.1. Consumes Plans 1, 2 and 3.

## Global Constraints

- Everything in Plan 1's and Plan 2's Global Constraints still applies.
- **HTML only.** No single-page-app framework, no TypeScript, no Node toolchain or bundler. Static assets are vendored and served by `feather-server`.
- CSP disallows inline scripts: `default-src 'self'`. HTMX is used only through HTML attributes.
- Read-only views still work as full-page loads without JavaScript.
- Every view is a URL, with filters in the query string.
- Business words first: a partition reads "Store 1509 · 18 Oct 2026".
- Every action goes through the service layer. Mass actions are two-step: `:preview` returns the impact and a `confirmation_token` valid for 10 minutes.
- State-changing calls accept a `reason` and an `Idempotency-Key`. Errors use `problem+json` (RFC 9457), and pagination is cursor-based.
- Until RBAC lands in M2, every local user is an administrator.
- Secrets never reach browsers or logs. Masked by default.
- Contrast and keyboard use meet WCAG 2.1 AA. An operator recovers a failed delivery in ≤ 5 clicks (NFR-11).

Paths are relative to `packages/feather-server/` unless they start with `packages/` or `tests/`.

## Review Focus

1. A confirmation token is reused, or the selection changed after the preview: the action refuses and asks for a new preview, because the impact shown is no longer true (Task 3).
2. Wrong passwords are tried repeatedly: attempts are slowed and the message never says whether the user exists (Task 2).
3. An API token is expired or revoked: it gets 401, even in a session started before it expired (Task 2).
4. A partition grid or failure list has tens of thousands of rows: pages stay paginated and render in under 2 s (Tasks 7 and 9).
5. Record values or error messages contain HTML or `<script>`: they appear as text and never execute (Task 5, with checks in Tasks 8 and 9).

---

### Task 1: Structured logs and correlation ids

**Files:**
- Create: `packages/feather-connect/src/feather_connect/logs.py`
- Modify: `packages/feather-connect/src/feather_connect/worker.py`, `runtime/runtime.py`, `src/feather_server/app.py`
- Test: `packages/feather-connect/tests/test_logs.py`

**Interfaces:**
- Produces: `configure_logging(level="INFO", *, secrets=())`; `bind(**ids)`, a context manager over `contextvars` for `tenant`, `flow`, `run`, `task`, `dataset`, `partition`, `version` and `route`; `TaskLogHandler(store)`, which copies records into `task_log`, capped at 64 KB per task.

- [ ] **Step 1: Write the failing tests.** `test_json_line_carries_bound_ids`; `test_secrets_are_redacted`; `test_task_log_is_capped_with_a_notice`; `test_tasks_bind_their_ids` (run a flow, and each log line in a task has `run` and `task`).

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-connect/tests/test_logs.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement `logs.py`.** Use the standard `logging` module with a JSON formatter and no new dependency. Reuse `redact` from `runtime/http.py`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add structured logs with correlation ids"`

### Task 2: Local users, tokens, sessions and CSRF

**Files:**
- Create: `packages/feather-connect/src/feather_connect/storage/postgres/migrations/versions/0002_auth_audit.py`, `src/feather_server/auth.py`, `src/feather_server/web/templates/login.html`
- Modify: `packages/feather-connect/src/feather_connect/storage/port.py`, `storage/memory.py`, `storage/postgres/store.py`, `testing/store_contract.py`, `src/feather_server/app.py`
- Test: `tests/test_auth.py`

**Interfaces:**
- Produces in `storage/port.py`: `UserStore`: `create_user(username, password_hash) -> str`, `get_user(username) -> UserRow | None`, `create_token(name, token_hash, expires_at) -> str`, `find_token(token_hash) -> TokenRow | None`, `revoke_token(token_id)`. `AuditStore`: `record(actor_type, actor, action, target, request, result)`, `list_audit(limit=100, cursor=None)`. Available as `store.users` and `store.audit`.
- Produces in `auth.py`: `Actor(kind: "human" | "system" | "token", name)`; `hash_password`, `verify_password`; `issue_token(store, name, ttl) -> str` (returns `fc_<random>` once; only its SHA-256 is stored); dependency `current_actor(request) -> Actor`, which accepts a session cookie or `Authorization: Bearer`; `csrf_token(request) -> str`, `require_csrf(request)`. Routes: `GET`, `POST /login`, `POST /logout`. Session cookie: `secure` (except on localhost), `httponly`, `samesite=lax`, signed with `FEATHER_SECRET_KEY`.

- [ ] **Step 1: Write the failing tests**

```python
def test_login_sets_a_secure_http_only_cookie(client): ...
def test_wrong_password_and_unknown_user_look_the_same(client):  # Review Focus 2
    a = client.post("/login", data={"username": "op", "password": "bad"})
    b = client.post("/login", data={"username": "nobody", "password": "bad"})
    assert a.status_code == b.status_code == 401 and a.text == b.text
def test_repeated_failures_are_slowed(client): ...  # Review Focus 2; 429 after 5 failures per user and IP
def test_token_works_and_only_its_hash_is_stored(client, store): ...
def test_expired_and_revoked_tokens_are_401(client): ...  # Review Focus 3
def test_form_post_without_csrf_is_403(client): ...
def test_api_bearer_call_needs_no_csrf(client): ...
def test_unauthenticated_page_redirects_to_login(client): ...
def test_unauthenticated_api_is_401_problem_json(client): ...
def test_healthz_and_ingress_need_no_login(client): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_auth.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement auth.** Dependencies: `argon2-cffi`, `itsdangerous`, `python-multipart`, `jinja2`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add local users, tokens, sessions and CSRF"`

### Task 3: Service layer

**Files:**
- Create: `src/feather_server/services/__init__.py`, `services/actions.py`, `services/reads.py`, `services/previews.py`
- Modify: `packages/feather-connect/src/feather_connect/runtime/runtime.py`, `runtime/plan.py` (backfill concurrency), `storage/port.py`, `storage/memory.py`, `storage/postgres/store.py`, `testing/store_contract.py`
- Test: `tests/test_actions.py`, `tests/test_reads.py`

**Interfaces:**
- Consumes: `Runtime` (`start_run`, `retry`, `skip`, `replay`, `cancel_run`, `pause_flow`, `resume_flow`, `pause_route`, `resume_route`), `Actor`, `store.audit`.
- Produces in `services/previews.py`: `Preview(token, action, impact: Impact)`, `Impact(partitions, deliveries, estimated_budget_units, samples)`. The token is signed, expires after 10 minutes, is single-use (tracked in `store.overrides`, or a small `preview_token` table added to migration `0002`), and embeds a hash of the selection.
- Produces in `services/actions.py`: `class Actions(state)`: `run_flow(actor, flow, *, reason=None) -> str`; `preview_rerun(actor, dataset, partitions) -> Preview`; `rerun(actor, token, *, reason=None) -> str`; `preview_backfill(actor, flow, *, start: date, end: date, max_concurrent=4) -> Preview`; `backfill(actor, token, *, reason=None) -> str`; `cancel_run(actor, run_id, *, reason=None)`; `preview_deliveries(actor, action: "retry" | "skip" | "replay", selection: DeliverySelection) -> Preview`; `apply_deliveries(actor, token, *, reason=None) -> list[str]`; `dismiss_quarantine(actor, route, version_id, key, *, reason=None)`; `pause_flow`, `resume_flow`, `pause_route`, `resume_route` (`actor`, name, `reason=None`); `check_connection(actor, name) -> CheckResult`. Each writes an audit row.
- Produces in `services/reads.py`: `class Reads(state)`: `overview() -> Overview`; `flows() -> list[FlowSummary]`; `flow_detail(name) -> FlowDetail`; `partition_grid(dataset, *, since=None, cursor=None, limit=100) -> Grid`; `runs(...)`; `run_detail(run_id)`; `task_logs(task_id)`; `failures_grouped(route=None, cursor=None) -> list[FailureGroup]`; `deliveries(...)`; `quarantine(...)`; `connections() -> list[ConnectionStatus]`.
- `Runtime.start_run` gains `max_concurrent: int | None = None`. The plan submits that many slices, and each finished slice submits the next.

- [ ] **Step 1: Write the failing tests** (on `MemoryStore` and `LocalEngine`)

```python
def test_rerun_preview_shows_impact_and_changes_nothing(actions): ...
def test_rerun_with_the_token_runs_those_partitions(actions): ...
def test_token_is_single_use_and_expires(actions, clock):  # Review Focus 1
def test_changed_selection_refuses_the_token(actions):  # Review Focus 1
    # a delivery in the selection moves on after the preview
    with pytest.raises(PreviewStale): actions.apply_deliveries(actor, p.token)
def test_bulk_retry_skip_and_replay_affect_only_the_selection(actions): ...
def test_every_action_writes_an_audit_row_with_actor_and_reason(actions, store): ...
def test_backfill_limits_concurrent_slices(actions, engine): ...  # FR-D4; at most max_concurrent extracts at once
def test_backfill_preview_estimates_budget_units(actions): ...
def test_cancel_stops_queued_tasks_and_records_the_reason(actions): ...
def test_failures_group_by_error_class_and_message_per_route(reads): ...
def test_overview_shows_failures_stale_datasets_budgets_and_workers(reads): ...
def test_partition_grid_cells_show_each_route_status(reads): ...
def test_flow_detail_has_effective_config_with_layers_and_no_secret(reads): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_actions.py packages/feather-server/tests/test_reads.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the service layer.** Reads needing joins get new `store.reads` queries, in `StoreContract`, so they work on both stores. M1 derives `partition_status` by query. `estimated_budget_units` is rows × the connector's declared cost, or `None` when unknown.

- [ ] **Step 4: Run them and confirm they pass.** Same command, plus `uv run pytest packages/feather-connect/tests/storage -q`. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add service layer with previews and audit"`

### Task 4: JSON API

**Files:**
- Create: `src/feather_server/api/__init__.py`, `api/routes.py`, `api/errors.py`, `api/paging.py`
- Modify: `src/feather_server/app.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `Actions`, `Reads`, `current_actor`.
- Produces: the M1 endpoints in design §9.2, under `/api/v1`: catalogue `GET /flows`, `/flows/{flow}`, `/datasets/{ds}`, `/routes/{route}`; runs `GET /runs`, `/runs/{id}`, `/runs/{id}/tasks`, `/tasks/{id}/logs`, `POST /flows/{flow}:run`, `/runs/{id}:cancel`, `/flows/{flow}:pause`, `:resume`; partitions `GET /datasets/{ds}/partitions`, `POST /datasets/{ds}/partitions:rerun`, `/flows/{flow}:backfill`; versions `GET /versions/{id}`, `/versions/{id}/records`; deliveries `GET /routes/{route}/deliveries`, `POST /deliveries:retry`, `:skip`, `:replay`; budgets `GET /budgets`, `/budgets/{name}/usage`; connections `GET /connections`, `POST /connections/{name}:check`; `GET /summary`. Each mass action also has a `:preview` form.

- [ ] **Step 1: Write the failing tests**

```python
def test_openapi_is_3_1_and_lists_every_endpoint(client): ...
def test_errors_are_problem_json(client): ...
def test_pagination_uses_a_cursor(client): ...
def test_mass_action_needs_a_preview_token(client): ...  # POST without a token is 400 with a link to :preview
def test_preview_then_confirm_retries_deliveries(client): ...
def test_idempotency_key_repeats_return_the_first_result(client): ...
def test_version_records_are_masked_and_paginated(client): ...
def test_reason_reaches_the_audit_row(client, store): ...
def test_no_secret_in_any_connection_response(client): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_api.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement thin routers.** No logic beyond parsing, calling the service and formatting. Store `Idempotency-Key` results for 24 hours. PII masking arrives in M2, so `/versions/{id}/records` returns values with a `masked: false` flag.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add JSON API"`

### Task 5: Page shell and overview

**Files:**
- Create: `src/feather_server/web/__init__.py`, `web/routes.py`, `web/templates/base.html`, `web/templates/overview.html`, `web/templates/_macros.html`, `web/static/app.css`, `web/static/htmx.min.js`, `web/charts.py`
- Modify: `src/feather_server/app.py`
- Test: `tests/test_web_overview.py`

**Interfaces:**
- Consumes: `Reads.overview`, `current_actor`, `csrf_token`.
- Produces: `render(request, template, **ctx) -> HTMLResponse`. Security headers middleware: CSP `default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; frame-ancestors 'none'`. Macros `status_badge(status)` (icon, text and colour), `time_ago(dt)`, `partition_label(dataset, key)`, `pagination(cursor)`, `confirm_form(preview)`. `web/charts.py`: `sparkline(values) -> str` and `meter(value, limit) -> str`, returning inline SVG. `base.html` has the nav, and light and dark tokens driven by `prefers-color-scheme`. Overview refreshes through `hx-get` every 10 s.

- [ ] **Step 1: Write the failing tests**

```python
def test_overview_shows_failing_flows_and_links_to_them(client): ...
def test_overview_shows_stale_datasets_budgets_workers_and_engine_health(client): ...
def test_every_page_sends_the_csp_header_and_has_no_inline_script(client):
    html = client.get("/").text
    assert "'unsafe-inline'" not in client.get("/").headers["content-security-policy"]
    assert "<script>" not in html and " onclick=" not in html
def test_user_text_is_escaped(client, store):  # Review Focus 5
    # a failed delivery's error is "<script>alert(1)</script>"
    assert "<script>alert" not in client.get("/").text and "&lt;script&gt;alert" in client.get("/").text
def test_page_works_without_htmx(client): ...  # the full page has the data, and links are plain anchors
def test_htmx_request_gets_a_fragment(client): ...
def test_status_is_never_colour_alone(client): ...  # badges have text
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_web_overview.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the shell.** Vendor HTMX 2.0.x with its licence file, and pin it by checksum in a test. Keep Jinja2 autoescape on, and use no `|safe` on anything derived from data; the SVG helpers build their markup from numbers only.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add dashboard shell and overview"`

### Task 6: Flows and flow detail

**Files:**
- Create: `web/templates/flows.html`, `web/templates/flow_detail.html`, `web/flow_graph.py`
- Modify: `web/routes.py` (all under `src/feather_server/`)
- Test: `tests/test_web_flows.py`

**Interfaces:**
- Consumes: `Reads.flows`, `Reads.flow_detail`, `Actions.run_flow`, `pause_flow`, `resume_flow`, `preview_backfill`, `backfill`.
- Produces: routes `GET /flows`, `GET /flows/{flow}`, `POST /flows/{flow}/run`, `/pause`, `/resume`, `/backfill/preview`, `/backfill/confirm`. `flow_graph(detail) -> str`, an inline SVG of datasets → routes → destinations.

- [ ] **Step 1: Write the failing tests.** `test_flows_list_shows_status_last_run_freshness_owner_tags_sha`; `test_filters_live_in_the_query_string`; `test_detail_shows_graph_config_layers_runs_versions`; `test_config_never_shows_a_secret`; `test_run_now_starts_a_run_and_shows_a_link`; `test_pause_and_resume_need_csrf_and_record_a_reason`; `test_backfill_preview_shows_the_impact_before_confirming`; `test_two_shas_for_one_flow_show_a_warning`.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_web_flows.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the pages.** The graph is computed in Python and rendered as SVG with `<title>` elements, so it needs no JavaScript.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add flows and flow detail pages"`

### Task 7: Partition grid

**Files:**
- Create: `web/templates/partitions.html`
- Modify: `web/routes.py`, `src/feather_server/services/actions.py`, `packages/feather-connect/src/feather_connect/runtime/extract.py` (dry run)
- Test: `tests/test_web_partitions.py`

**Interfaces:**
- Consumes: `Reads.partition_grid`, `Actions.preview_rerun`, `rerun`, `preview_deliveries`, `apply_deliveries`.
- Produces: routes `GET /datasets/{ds}/partitions`, `POST /datasets/{ds}/partitions/rerun/preview`, `/confirm`, `POST /datasets/{ds}/partitions/{partition}/verify`, `POST /routes/{route}/partitions/{partition}/replay/preview`, `/confirm`. `run_extract(..., dry_run: bool = False) -> VerifyResult(upserts, deletes)`, which writes nothing. `Actions.verify_partition(actor, dataset, partition) -> VerifyResult`.

- [ ] **Step 1: Write the failing tests**

```python
def test_grid_shows_a_cell_per_partition_and_route_with_business_labels(client): ...
def test_cell_links_drill_down_to_deliveries(client): ...
def test_rerun_selected_partitions_with_preview(client): ...
def test_verify_reports_differences_and_changes_nothing(client): ...
def test_replay_to_one_route_leaves_others_alone(client): ...
def test_50_000_partitions_page_renders_quickly(client, seed_grid):  # Review Focus 4
    r = timed(client.get, "/datasets/d/partitions")
    assert r.seconds < 2 and "next" in r.text
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_web_partitions.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the page.** A table with sticky headers, cursor pagination by partition, and checkboxes in a plain form. Each cell's status is shown with an icon and text.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add partition grid"`

### Task 8: Runs and tasks

**Files:**
- Create: `web/templates/runs.html`, `web/templates/run_detail.html`
- Modify: `web/routes.py`
- Test: `tests/test_web_runs.py`

**Interfaces:**
- Consumes: `Reads.runs`, `run_detail`, `task_logs`, `Actions.cancel_run`, `Actions.run_flow`.
- Produces: routes `GET /runs`, `GET /runs/{id}`, `GET /tasks/{id}/logs`, `POST /runs/{id}/cancel`, `POST /runs/{id}/rerun`.

- [ ] **Step 1: Write the failing tests.** `test_runs_list_filters_by_flow_and_status`; `test_skipped_runs_show_their_reason` (FR-E4); `test_detail_has_timeline_inputs_attempts_and_sha`; `test_logs_are_shown_escaped_and_paged` (Review Focus 5); `test_active_run_refreshes_through_htmx`; `test_cancel_asks_for_confirmation_and_records_the_reason`; `test_rerun_with_new_inputs_starts_a_new_run`.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_web_runs.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the pages.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add runs and tasks pages"`

### Task 9: Deliveries and failures

**Files:**
- Create: `web/templates/failures.html`, `web/templates/deliveries.html`, `web/templates/quarantine.html`
- Modify: `web/routes.py`
- Test: `tests/test_web_failures.py`

**Interfaces:**
- Consumes: `Reads.failures_grouped`, `deliveries`, `quarantine`, `Actions.preview_deliveries`, `apply_deliveries`, `dismiss_quarantine`.
- Produces: routes `GET /failures`, `GET /routes/{route}/deliveries`, `GET /quarantine`, `POST /deliveries/preview`, `POST /deliveries/confirm`, `POST /quarantine/dismiss`.

- [ ] **Step 1: Write the failing tests**

```python
def test_failures_are_grouped_by_error_class_and_message_per_route(client): ...
def test_group_shows_a_blocked_partition_and_what_it_blocks(client): ...
def test_bulk_retry_previews_then_confirms(client): ...
def test_skip_needs_a_reason_field_filled_when_protected(client): ...
def test_replay_from_deliveries_page(client): ...
def test_quarantine_shows_reason_and_can_be_dismissed(client): ...
def test_error_messages_are_escaped(client): ...  # Review Focus 5
def test_large_failure_list_is_paginated_and_fast(client): ...  # Review Focus 4
def test_recover_a_failed_delivery_in_five_clicks_or_fewer(client):  # NFR-11
    # overview -> failures -> group -> retry preview -> confirm: count the requests
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_web_failures.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the pages.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add deliveries, failures and quarantine pages"`

### Task 10: Connections

**Files:**
- Create: `web/templates/connections.html`
- Modify: `web/routes.py`, `src/feather_server/services/actions.py`
- Test: `tests/test_web_connections.py`

**Interfaces:**
- Consumes: `Reads.connections`, `Actions.check_connection`, `CheckResult`, `Project.connections`.
- Produces: routes `GET /connections`, `POST /connections/{name}/check`. The check runs on the server with a 15 s timeout and stores `last_check_at`, `last_check_ok` and `last_check_message`.

- [ ] **Step 1: Write the failing tests.** `test_lists_status_last_check_and_site`; `test_check_runs_and_shows_the_result`; `test_failed_check_shows_a_redacted_message`; `test_check_times_out_with_a_clear_message`; `test_no_secret_on_the_page`.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-server/tests/test_web_connections.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the page and action.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add connections page"`

### Task 11: Browser and accessibility checks

**Files:**
- Create (root): `tests/dashboard/conftest.py`, `tests/dashboard/test_operator_journeys.py`, `tests/dashboard/test_accessibility.py`
- Modify (root): `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: the `stack` fixture (Plan 3, Task 7), a seeded project using simulation connectors.
- Produces: a `page` fixture that signs in.

- [ ] **Step 1: Write the failing tests**

```python
def test_rerun_from_the_dashboard(page, stack):  # M1 exit criterion
    # partition grid -> select a day -> preview -> confirm -> the run finishes and the cell updates
def test_replay_from_the_dashboard(page, stack):  # M1 exit criterion
    # a route's delivery -> replay -> preview -> confirm -> the sink received it again
def test_retry_after_a_failure(page, stack): ...
def test_read_only_pages_work_with_javascript_disabled(browser, stack): ...
def test_no_console_errors_or_csp_violations(page, stack): ...
def test_keyboard_only_retry(page, stack): ...

@pytest.mark.parametrize("path", ["/", "/flows", "/flows/demo", "/datasets/demo.items/partitions",
                                  "/runs", "/failures", "/connections"])
@pytest.mark.parametrize("scheme", ["light", "dark"])
def test_axe_finds_no_wcag_aa_violations(page, stack, path, scheme): ...  # NFR-13
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run playwright install chromium`, then `uv run pytest tests/dashboard -q -x`. Expected: FAIL.

- [ ] **Step 3: Implement the fixtures and fix what they expose.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add tests packages .github && git commit -m "test: add dashboard journeys and accessibility checks"`

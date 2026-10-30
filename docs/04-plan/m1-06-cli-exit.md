# M1 Plan 6: CLI, packaging and the M1 exit

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One `feather` command for developers, operators and AI agents; `feather dev` shows the demo in the dashboard within 5 minutes; and every M1 exit criterion is proved by a test.

**Architecture:** `feather-cli` is a thin Typer app. Local commands (`new`, `validate`, `run --local`, `test`) use the SDK directly. Remote commands (`status`, `runs`, `logs`, `backfill`, `replay`) call the JSON API through one client. Every command takes `--env` and `--json`.

**Tech Stack:** Typer, httpx, Docker and Docker Compose, Playwright (for the timing check).

**Spec:** [design v0.2](../02-design/2026-10-30-feather-connect-design.md) §5.1, §5.8, §14.1, §16, §18, §19. Consumes Plans 1–5.

## Global Constraints

- Everything in the earlier plans' Global Constraints still applies.
- The command is named `feather`, never `fc`, which is a bash and zsh builtin.
- Every command accepts `--env` and `--json`. With `--json`, stdout holds exactly one JSON document, and the exit code is 0 on success and non-zero on failure.
- A Compose environment uses ≤ 4 containers (NFR-06). Postgres is the only required stateful dependency (NFR-07).
- From a clean install, `feather dev` shows a demo flow in the dashboard in ≤ 5 minutes.
- Images build for linux/amd64 and linux/arm64 (NFR-12), with no secrets in them.
- Image names use `ghcr.io/<org>/...`, with `<org>` read from one variable until discovery Q9 is answered.

Paths are relative to the repository root.

## Review Focus

1. `feather dev` runs when Docker isn't running, or when port 8080 or the Postgres port is taken: it stops with a message saying what to do, instead of a stack trace (Task 7).
2. `feather dev` is stopped with Ctrl+C and started again: it comes back cleanly, with its data kept and no leftover processes (Task 7).
3. `feather run --local` is used on a flow whose connections need real credentials: without `--sim` it says which connection is missing what, and with `--sim` it needs none (Task 3).
4. `feather new flow` targets a name that already exists, or an invalid name: it refuses without overwriting, and leaves no partial files (Task 2).
5. A remote command can't reach the server or is refused: the error says which URL and why, and with `--json` it is still valid JSON (Task 4).

---

### Task 1: CLI skeleton, `validate` and `describe`

**Files:**
- Create: `packages/feather-cli/pyproject.toml`, `packages/feather-cli/src/feather_cli/__init__.py`, `main.py`, `output.py`, `commands/validate.py`, `commands/describe.py`
- Test: `packages/feather-cli/tests/test_validate.py`, `tests/test_describe.py` in the same package

**Interfaces:**
- Consumes: `load_project`, `Flow.validate`, `Problem`, `list_connectors`, `describe_connector`.
- Produces: console script `feather`. `output.emit(data, *, json_mode, human: Callable)`, and `output.fail(message, *, code=1, hint=None, json_mode)`. `feather validate [--env dev] [--json]` prints problems with fix hints. Exit 0 with warnings only, 1 with any error. `feather describe connector <name> --json` prints the config schema and capabilities.

- [ ] **Step 1: Write the failing tests**

```python
def test_validate_clean_project_exits_zero(cli, project): ...
def test_validate_reports_problems_as_json_with_hints(cli, bad_project):
    r = cli("validate", "--json"); doc = json.loads(r.stdout)
    assert r.exit_code == 1 and doc["problems"][0]["code"] == "mode_needs_complete" and doc["problems"][0]["hint"]
def test_validate_checks_budgets_partitions_and_config_references(cli, bad_project): ...
def test_warnings_do_not_fail_validate(cli, warn_project): ...
def test_describe_connector_returns_schema_and_capabilities(cli): ...
def test_json_mode_keeps_stdout_clean(cli, project): ...  # logs go to stderr
def test_help_lists_every_command(cli): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-cli/tests -q`. Expected: FAIL.

- [ ] **Step 3: Implement the skeleton.** Add project-level checks in `feather_connect/project.py`: `validate_project(project) -> list[Problem]`, which covers budget references, connection references and connector availability, on top of `Flow.validate`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add feather CLI with validate and describe"`

### Task 2: `feather new` and `AGENTS.md`

**Files:**
- Create: `packages/feather-cli/src/feather_cli/commands/new.py`, `packages/feather-cli/src/feather_cli/scaffolds/` (connector skeleton, `AGENTS.md`, project skeleton)
- Modify: `packages/feather-cli/pyproject.toml` (ship `templates/` as package data)
- Test: `packages/feather-cli/tests/test_new.py`

**Interfaces:**
- Produces: `feather new project <name>`, `feather new flow <name> [--template t]`, `feather new connector <name>`. Generated projects include `AGENTS.md`, with the rules in design §14.1: transforms are pure; budgets attach to connections; never put data in task arguments; every flow needs a test. It also gives the commands to run.

- [ ] **Step 1: Write the failing tests**

```python
def test_new_project_has_the_design_layout(cli, tmp_path): ...  # §5.1 tree, including AGENTS.md
def test_new_flow_from_each_template_validates_and_its_tests_pass(cli, tmp_path, template): ...
def test_new_connector_passes_the_conformance_kit_stubs(cli, tmp_path): ...
def test_existing_name_is_refused_and_nothing_changes(cli, tmp_path):  # Review Focus 4
def test_invalid_name_is_refused_with_no_partial_files(cli, tmp_path):  # Review Focus 4
def test_unknown_template_lists_the_available_ones(cli, tmp_path): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-cli/tests/test_new.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement `new`.** Write into a temporary folder, and move it into place only when complete.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add feather new with scaffolds and AGENTS.md"`

### Task 3: `feather run` and `feather test`

**Files:**
- Create: `packages/feather-cli/src/feather_cli/commands/run.py`, `commands/test.py`
- Test: `packages/feather-cli/tests/test_run.py`

**Interfaces:**
- Consumes: `FlowHarness`, `Runtime`, `LocalEngine`, `load_project`, `simulation`.
- Produces: `feather run <flow> [--local] [--sim] [--partition k=v]... [--json]`. `--local` runs in-process with `LocalEngine` and in-memory storage against real connections. `--sim` swaps every connection for simulation connectors, using the project's `fixtures/`. Without `--local` it starts a run on the server (Task 4's client). JSON output: run id, status, per-route counts, quarantine counts. `feather test [--json]` runs the project's pytest suite, including the conformance kit for its connectors.

- [ ] **Step 1: Write the failing tests**

```python
def test_run_local_sim_reports_per_route_counts(cli, project):
    doc = json.loads(cli("run", "demo", "--local", "--sim", "--json").stdout)
    assert doc["status"] == "succeeded" and doc["routes"]["warehouse"]["applied"] > 0
def test_run_local_without_sim_names_the_missing_credential(cli, project):  # Review Focus 3
    r = cli("run", "demo", "--local")
    assert r.exit_code != 0 and "warehouse" in r.stderr and "MOTHERDUCK_TOKEN" in r.stderr
def test_partition_option_runs_one_slice(cli, project): ...
def test_failed_local_run_exits_non_zero_with_the_reason(cli, project): ...
def test_feather_test_passes_on_a_new_project_and_returns_json(cli, project): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-cli/tests/test_run.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement both commands.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add feather run and feather test"`

### Task 4: Remote commands

**Files:**
- Create: `packages/feather-cli/src/feather_cli/client.py`, `commands/remote.py`
- Test: `packages/feather-cli/tests/test_remote.py`

**Interfaces:**
- Consumes: the JSON API (Plan 5, Task 4).
- Produces: `ApiClient(base_url, token)`, the one generated-style client that the MCP server also uses in M2. Server URL and token come from `--server`, then `FEATHER_SERVER` and `FEATHER_TOKEN`, then `env/<env>.yaml`. Commands: `feather status`, `feather runs [--flow] [--status]`, `feather logs <run>`, `feather backfill <flow> --from D --to D [--dry-run] [--yes]`, `feather replay <route> (--version id | --partition k=v) [--dry-run] [--yes]`. Mass actions show the preview, and proceed only on `--yes` or a typed confirmation.

- [ ] **Step 1: Write the failing tests** (a test server with `create_app`, `MemoryStore` and `LocalEngine`)

```python
def test_status_shows_failures_and_workers(cli, server): ...
def test_runs_filter_and_json(cli, server): ...
def test_logs_print_a_run_task_logs(cli, server): ...
def test_backfill_dry_run_prints_the_impact_and_changes_nothing(cli, server): ...
def test_backfill_yes_starts_the_run(cli, server): ...
def test_replay_by_version_and_by_partition(cli, server): ...
def test_unreachable_server_names_the_url(cli):  # Review Focus 5
    r = cli("status", "--server", "http://127.0.0.1:9", "--json")
    assert r.exit_code != 0 and "127.0.0.1:9" in json.loads(r.stdout)["error"]
def test_rejected_token_says_so(cli, server): ...  # Review Focus 5
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-cli/tests/test_remote.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the client and commands.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add remote CLI commands"`

### Task 5: `feather server` and `feather worker`

**Files:**
- Create: `packages/feather-cli/src/feather_cli/commands/processes.py`
- Test: `packages/feather-cli/tests/test_processes.py`

**Interfaces:**
- Consumes: `create_app`, `migrate`, `run_worker`, `issue_token`, `store.users`.
- Produces: `feather server [--host 0.0.0.0] [--port 8080]`; `feather server migrate`; `feather server create-user <username>` (reads the password from a prompt or stdin, never an argument); `feather server create-token <name> --ttl 90d` (prints the token once); `feather worker [--slots 8] [--labels k=v]`. Settings use the variables in design §18.1.

- [ ] **Step 1: Write the failing tests.** `test_server_starts_and_readyz_turns_ok` (postgres); `test_migrate_is_idempotent`; `test_create_user_reads_the_password_from_stdin_and_the_user_can_log_in`; `test_create_token_prints_once_and_stores_only_the_hash`; `test_worker_starts_and_appears_on_the_server`; `test_missing_database_url_explains_what_to_set`.

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest packages/feather-cli/tests/test_processes.py -q`. Expected: FAIL.

- [ ] **Step 3: Implement the commands.**

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages && git commit -m "feat: add server and worker commands"`

### Task 6: Images and Compose

**Files:**
- Create: `deploy/Dockerfile.server`, `deploy/Dockerfile.worker`, `deploy/compose.yaml`, `deploy/.env.example`, `deploy/README.md`
- Modify: `.github/workflows/ci.yml` (build images), `.github/workflows/release.yml`
- Test: `tests/deploy/test_compose.py`

**Interfaces:**
- Consumes: `feather server`, `feather worker`, `feather server migrate`.
- Produces: services `postgres` (`postgres:17`), `migrate` (a one-shot job), `feather-server` and `feather-worker`: at most 4 containers. Profiles `objects`, `sso` and `observability` are placeholders in M1 and add nothing yet. `Dockerfile.worker` is built `FROM` by the user's flow image. Ports bind to `127.0.0.1` by default. `.env.example` holds no real secrets, and `compose.yaml` refuses to start without `FEATHER_SECRET_KEY`.

- [ ] **Step 1: Write the failing tests** (nightly and on changes to `deploy/`)

```python
def test_compose_starts_healthy_and_serves_the_dashboard(compose): ...
def test_compose_has_at_most_four_long_running_containers(compose): ...  # NFR-06
def test_only_postgres_is_stateful(compose): ...  # NFR-07: one named volume
def test_no_secret_in_the_images(): ...  # docker history has no key values
def test_restart_keeps_data(compose): ...
def test_two_workers_scale_with_replicas(compose): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/deploy -q`. Expected: FAIL.

- [ ] **Step 3: Write the images and Compose file.** Use multi-stage builds with `uv sync --frozen`, run as a non-root user, and add healthchecks. `release.yml` builds for both architectures and pushes on tags, with `<org>` from a variable. Signing and the SBOM come with the release work in M3.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add deploy tests .github && git commit -m "feat: add images and Compose deployment"`

### Task 7: `feather dev`

**Files:**
- Create: `packages/feather-cli/src/feather_cli/commands/dev.py`, `examples/demo/` (copied from `templates/fan-out-demo`)
- Test: `tests/dev/test_feather_dev.py`

**Interfaces:**
- Consumes: `migrate`, `create_app`, `run_worker`, `load_project`, `examples/demo`.
- Produces: `feather dev [--project PATH] [--port 8080] [--no-browser]`, following design §18.2: use `FEATHER_DATABASE_URL` if set, otherwise start a named Postgres container `feather-dev-postgres` with a named volume; run migrations; start the server and one worker in-process with auto-reload on flow changes; load the demo project on the JSON simulation connector; create a local user and print its generated password; open the dashboard at `http://localhost:8080`. Ctrl+C stops the processes and leaves the container's data.

- [ ] **Step 1: Write the failing tests**

```python
def test_dev_shows_the_demo_flow_within_five_minutes(dev_clean):  # M1 exit criterion, NFR-06
    # clean machine state: no container, no volume, image cache allowed. Timed through the dashboard.
    assert seconds_until_demo_flow_visible <= 300

def test_demo_runs_and_reaches_destinations(dev): ...
def test_editing_a_flow_reloads_it(dev): ...
def test_docker_not_running_explains_what_to_do(fake_no_docker):  # Review Focus 1
    assert r.exit_code != 0 and "Docker" in r.stderr and "FEATHER_DATABASE_URL" in r.stderr
def test_port_taken_names_the_port_and_the_flag(dev_port_taken): ...  # Review Focus 1
def test_ctrl_c_then_restart_keeps_data_and_leaves_no_processes(dev):  # Review Focus 2
def test_works_with_an_existing_database_url(pg_url): ...
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/dev -q`. Expected: FAIL.

- [ ] **Step 3: Implement `feather dev`.** Check ports and Docker before starting anything. Use a free local Postgres port, chosen once and stored in the container's labels. Windows and Linux must both work. Reload restarts the in-process worker when `flows/` or `env/` change, using `watchfiles`.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add packages examples tests && git commit -m "feat: add feather dev"`

### Task 8: Demo end to end

**Files:**
- Create: `tests/e2e/test_demo_fanout.py`
- Modify: `examples/demo/` as needed, root `README.md` (quick start)

**Interfaces:**
- Consumes: the `stack` fixture, `examples/demo`.

- [ ] **Step 1: Write the failing tests**

```python
def test_demo_fans_out_to_postgres_duckdb_and_http(stack, http_echo, duckdb_file):  # M1 exit criterion
    run = stack.start_run("demo-fan-out"); stack.wait_for(run)
    assert pg_rows() == duck_rows() == http_echo.received_keys() == EXPECTED_KEYS

def test_a_down_route_does_not_hold_the_others_and_catches_up(stack, http_echo):  # FR-B1, H6
    http_echo.go_down(); run_and_wait()
    assert pg_rows() == EXPECTED and http_echo.received_keys() == set()
    http_echo.come_up(); retry_failed(); wait_all_applied()
    assert http_echo.received_keys() == EXPECTED

def test_new_data_produces_only_the_changes(stack, http_echo): ...
def test_quick_start_in_the_readme_works(): ...  # the commands in the README's code block run in order
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/e2e/test_demo_fanout.py -q`. Expected: FAIL.

- [ ] **Step 3: Fix what the tests expose and write the quick start.** Update the root README's status line, and add the quick start: install, `feather dev`, and what to look at.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Commit.** `git add tests examples README.md && git commit -m "test: prove the demo fans out to three destinations"`

### Task 9: Hard cases and the M1 exit

**Files:**
- Create: `tests/hard_cases/conftest.py`, `tests/hard_cases/test_hard_cases.py`, `tests/e2e/test_webhook_to_http_latency.py`, `docs/06-release/m1-exit-report.md`
- Modify: `.github/workflows/ci.yml`, `docs/README.md`

**Interfaces:**
- Consumes: the `stack` fixture, `feather-postgres`, `feather-http`, and the Plan 2 chaos suite.
- Produces: `tests/hard_cases/`, running the design's §16 scenarios on the full stack. M2 extends it to H5, H7–H13 and H15.

- [ ] **Step 1: Write the failing tests**

```python
def test_h1_record_moves_partitions_leaves_no_stale_copy(stack): ...  # upsert and replace_partition routes
def test_h2_snapshot_diff_on_a_stampless_table(stack): ...
def test_h3_source_rewrites_a_key_in_place(stack): ...
def test_h4_late_rows_inside_the_lookback_captured_once(stack): ...
def test_h6_destination_down_during_the_read_window(stack): ...
def test_h14_webhook_burst(stack): ...  # 10x normal, no loss, p95 recovers within 5 minutes

# tests/e2e/test_webhook_to_http_latency.py
def test_webhook_to_http_p95_within_5s(stack, http_echo):  # M1 exit criterion, real connectors
    # 50 events/s for 60 s through feather-http's Webhook and Call
    assert lost == 0 and p95 <= 5.0
```

- [ ] **Step 2: Run them and confirm they fail.** Run: `uv run pytest tests/hard_cases tests/e2e/test_webhook_to_http_latency.py -q`. Expected: FAIL.

- [ ] **Step 3: Fix what the tests expose.** Anything that needs more than a local fix is reported in the exit report, with the plan task it reopens.

- [ ] **Step 4: Run them and confirm they pass.** Same command. Expected: all pass.

- [ ] **Step 5: Run the whole M1 gate.** Run: `uv run ruff check .`, `uv run pyright`, `uv run pytest packages --cov --cov-fail-under=70`, `uv run pytest tests -q`. Expected: all clean, and SDK core coverage ≥ 85%.

- [ ] **Step 6: Write the exit report.** `docs/06-release/m1-exit-report.md` has one row per exit criterion in design §19: pass or fail, the test that proves it, and the measured number. List known gaps and the state of open questions. Update the build row in `docs/README.md`.

- [ ] **Step 7: Commit.** `git add tests docs .github && git commit -m "test: prove the M1 exit criteria and add the exit report"`

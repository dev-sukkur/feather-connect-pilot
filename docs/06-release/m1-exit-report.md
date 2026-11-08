# M1 exit report and review guide

This is where M1 stands against its exit criteria (design §19), how to run every check that proves them, a map of the
code for a reviewer, and what is still open. Branch `stage-5-exit`; the measured numbers come from the full M1 gate
run on 2026-11-08 (Windows 11, Docker Desktop), after the two fixes in §4.

## 1. Exit criteria

| Exit criterion (design §19) | Proved by | Result | Measured |
|---|---|---|---|
| The demo fans JSON simulation data out to Postgres, DuckDB and HTTP | `tests/e2e/test_demo_fanout.py::test_demo_fans_out_to_postgres_duckdb_and_http` (real stack); `tests/demo/test_demo_project.py` (in process) | Passed in the M1 gate (2026-11-08) | all orders at all three: yes (3 passed) |
| Webhook → HTTP p95 ≤ 5 s | `tests/e2e/test_webhook_to_http_latency.py` (feather-http `Webhook` and `Call`, from the generator's send to the echo's receipt); `tests/e2e/test_webhook_latency.py::test_webhook_to_sink_p95_within_5s` (test connectors, receipt to write) | Passed in the M1 gate (2026-11-08) | 3,000 events at 50/s: p95 1.856 s, p50 1.263 s, lost 0, repeats 0 on the fsync-off test Postgres; p95 2.004 s (p50 1.342 s, lost 0) on a durable fsync-on one (S19). Receipt to write: p95 1.709 s |
| Re-run and replay work from the dashboard | `tests/dashboard/test_operator_journeys.py::test_rerun_from_the_dashboard`, `::test_replay_from_the_dashboard` (Chromium) | Passed | — |
| kill -9 mid-apply loses and duplicates nothing | `tests/chaos/test_kill_mid_apply.py::test_kill_9_mid_apply_loses_and_duplicates_nothing`, and the rest of `tests/chaos` | Passed in the M1 gate (2026-11-08) (12 chaos tests) | lost 0, duplicates 0 |
| `feather dev` ≤ 5 minutes | `tests/dev/test_feather_dev.py::test_dev_shows_the_demo_flow_within_five_minutes` | Passed in the M1 gate (2026-11-08) | 67 s to the demo flow on the dashboard |
| H1–H4 and H6 pass | `tests/hard_cases/test_hard_cases.py` (full stack, real Postgres and HTTP connectors); `packages/feather-connect/tests/runtime/test_extract.py` and `test_apply.py` (`LocalEngine`) | See §4 | — |
| H14 passes | `tests/e2e/test_webhook_latency.py::test_burst_at_10x_loses_nothing_and_recovers` | Passed in the M1 gate (2026-11-08) | 18,229 events, lost 0, duplicates 0; p95 never left the target, recovered after 0.0 s |
| NFR-13 (WCAG 2.1 AA) | `tests/dashboard/test_accessibility.py` (axe-core on 7 pages, light and dark) | Passed | 0 violations |
| NFR-06, 07, 15 (Compose) | `tests/deploy/test_compose.py` | 7 of 7 passed | — |

## 2. Running the checks

### Prerequisites

| What | Why | Notes |
|---|---|---|
| uv, and Python 3.13 (uv installs it) | everything | `uv sync --all-extras` at the root installs every workspace package and the dev tools |
| Docker Desktop, running | the test Postgres, `feather dev`, chaos, the Compose tests, the S19 database | keep 5 GB free on the Docker disk: `tests/deploy` builds two 700 MB images and removes them at the end |
| The test Postgres | every test marked `postgres` | `docker compose -f deploy/compose.test.yaml up -d`: Postgres 17 on `127.0.0.1:25433`, fsync off, in memory. `FEATHER_TEST_DATABASE_URL` overrides `postgresql://feather:feather@127.0.0.1:25433/feather_test` |
| Chromium for Playwright | `tests/dashboard` | `uv run playwright install chromium` (about 170 MB; on Linux CI, `--with-deps`) |

Ports the tests take, apart from free ones they pick themselves:

| Port | Used by |
|---|---|
| 25433 | the test Postgres (`deploy/compose.test.yaml`) |
| 25434 | the chaos suite's own Postgres container, and the S19 durable Postgres (`feather-latency-postgres`); not at the same time |
| 29000 | the Compose test stack (`FEATHER_COMPOSE_TEST_PORT`) |
| 8080, 29100, 29101 | `feather dev` and the demo's helpers, when a developer runs them |

On the development machine 5432, 55432, 7077 and 8888 belong to other projects; nothing here uses them.

### The M1 gate

Run from the repository root, in this order. Each should end as the right-hand column says.

| Command | Expected |
|---|---|
| `uv sync --all-extras` | resolves from `uv.lock` |
| `uv run ruff check .` | `All checks passed!` |
| `uv run ruff format --check .` | no file would be reformatted |
| `uv run pyright` | `0 errors` (the add-on packages' imports carry ignores, see §5) |
| `uv run pytest packages --cov --cov-fail-under=70` | all pass; total coverage ≥ 70 % |
| `uv run pytest packages/feather-connect/tests --cov=feather_connect --cov-report=term --cov-fail-under=85` | SDK core coverage ≥ 85 % (85.06 %; all packages 96.32 %) |
| `uv run pytest tests -q` | all pass, apart from the `perf` tests, which `tests/e2e` leaves out unless a file is named |

`uv run pytest tests -q` covers chaos, dashboard, demo, deploy (Docker), dev (Docker), e2e, engine conformance,
hard cases and templates. It takes about 34 minutes. The exit numbers come from the runs named by file:

```text
uv run pytest tests/e2e/test_webhook_latency.py -s                  # p95 receipt to write, H14 burst, schedules
uv run pytest tests/e2e/test_webhook_to_http_latency.py -s          # p95 on the real HTTP connectors
uv run pytest tests/chaos -s                                        # kill -9 mid-apply and mid-push
uv run pytest tests/dev/test_feather_dev.py -k five_minutes -s      # feather dev to the demo
uv run pytest tests/hard_cases tests/dashboard -q
uv run pytest tests/deploy -q                                       # images and Compose; FEATHER_COMPOSE_KEEP_IMAGES=1 keeps the images
```

The latency on a durable database (S19): a second Postgres with fsync on, on 25434, removed afterwards.

```text
docker run -d --name feather-latency-postgres -p 127.0.0.1:25434:5432 -e POSTGRES_USER=feather \
    -e POSTGRES_PASSWORD=feather -e POSTGRES_DB=feather_test postgres:17
FEATHER_TEST_DATABASE_URL=postgresql://feather:feather@127.0.0.1:25434/feather_test \
    uv run pytest tests/e2e/test_webhook_to_http_latency.py -s
docker rm -fv feather-latency-postgres    # -v removes its data volume too
```

In CI (`.github/workflows/ci.yml`): `check` (lint, types, unit tests on Ubuntu and Windows), `postgres` (the tests
marked `postgres`, with the 85 % coverage gate), `dashboard` (Chromium), `engine-conformance`, `chaos` (nightly and
on product changes: chaos, perf, hard cases and both latency tests) and `deploy` (images and Compose).

## 3. A map of the M1 code

| Package | What it is | Start reading at |
|---|---|---|
| `packages/feather-connect` | the SDK and the runtime: datasets, flows, routes, strategies, partitions, versions, the ledger, the scheduler, the sealer and sweeper, the stores and the spool | `src/feather_connect/__init__.py` (the public API), `core/`, `runtime/runtime.py` |
| `packages/feather-engine-dbos` | the durable engine behind the `Engine` port (ADR-0002, ADR-0003) | `engine.py` |
| `packages/feather-server` | the JSON API, the HTML dashboard, auth, ingress for webhooks, the server's loops | `app.py`, `services/actions.py`, `services/reads.py` |
| `packages/feather-cli` | `feather`: dev, new, validate, describe, run, test, the remote commands, server, worker | `main.py`, `commands/` |
| `packages/connectors/*` | Postgres, DuckDB/MotherDuck, S3, HTTP (REST, GraphQL, Call, Webhook) | each `src/feather_connect/connectors/<name>.py` and its README |
| `packages/templates/*` | the generic templates (library functions first, `feather new --template` copies them) and the retail ones | `generic.py` |
| `deploy/` | the images, `compose.yaml`, and the test Postgres | `compose.yaml` |
| `examples/demo` | the M1 exit demo | its README |

Key modules of the runtime, in the order data moves:

| Module | Does |
|---|---|
| `runtime/plan.py` | turns a trigger into a run and its slices |
| `runtime/extract.py` | reads a source into the spool and works out what changed (by_stamp, snapshot_diff, replace) |
| `runtime/seal.py`, `runtime/sealer.py` | seals versions and writes their deliveries in one transaction; batches push events by count and bytes |
| `runtime/apply.py` | delivers a version's chunks to a sink, with the ledger's idempotency keys |
| `runtime/budgets.py`, `runtime/http.py` | budget windows, and the shared HTTP client (retries, metering, OAuth2 tokens shared per grant) |
| `runtime/scheduler.py`, `runtime/leader.py` | ticks, with compare-and-set so one leader fires |
| `runtime/sweeper.py`, `runtime/retention.py` | recovers dead workers' tasks; deletes what has expired |
| `storage/port.py` | the store's contract; `storage/memory.py` and `storage/postgres/store.py` implement it, and `testing/store_contract.py` tests both |
| `storage/postgres/migrations/versions/` | the schema, 0001 to 0012 |

Where each criterion is proved is the table in §1; the requirement-to-task map is in `docs/04-plan/README.md`.

## 4. Stage 5 results

### The full M1 gate (2026-11-08)

Every command in §2 was run on this branch. Two tests failed on the first pass; both are fixed (below) and the two
tests pass again.

| Step | Result |
|---|---|
| ruff, format check, pyright | clean; pyright 0 errors |
| `pytest packages --cov` | 1,780 passed, 24 skipped, 3 xfailed, 1 failed (fixed: `1b48445`); coverage 96.32 % |
| feather-connect coverage | 1,221 passed, 10 skipped, 2 xfailed; coverage 85.06 % |
| `pytest tests -q` | 129 passed, 6 skipped, 1 failed (fixed: `ae50501`), 33.5 minutes |
| `tests/e2e/test_webhook_latency.py -s` | 4 passed; p95 1.709 s receipt to write; H14 burst 18,229 events, lost 0 |
| `tests/e2e/test_webhook_to_http_latency.py -s` | passed; p50 1.263 s, p95 1.856 s, max 2.204 s; 3,000 of 3,000, none repeated |
| the same on a durable fsync-on Postgres (S19, port 25434) | passed; p50 1.342 s, p95 2.004 s, max 2.451 s; container and volume removed afterwards |
| `tests/chaos -s` | 12 passed |
| `tests/dev/test_feather_dev.py -k five_minutes` | passed; demo visible after 67 s |
| `tests/e2e/test_demo_fanout.py` (rerun alone) | 3 passed |

The two failures:

- `test_run_local_sim_reports_per_route_counts` (`1b48445`, test bug): a manual run of a `by_day` dataset reads
  yesterday and today, and the test's rows had fixed dates, so it began failing once those dates aged out. The rows
  are now dated from the clock.
- `test_two_workers_scale_with_replicas` (`ae50501`, product bug): the default worker id is the hostname, which a
  restarted container keeps. After `compose restart` the row stayed `draining`, and the restarted worker's heartbeats
  left it so. A starting worker now marks its row `alive`; new test
  `test_a_worker_started_again_under_its_id_is_alive`. `tests/deploy/test_compose.py` 7 passed after it.

CI on Linux showed two more, both fixed and checked in a Linux container:

- `tests/dev` (`0e2cc2b`, product bug): `feather dev`'s port check bound without `SO_REUSEADDR`, so a helper's port
  still holding TIME_WAIT connections from the last run looked taken for a minute. The check now sets it on POSIX, as
  servers do; new test `packages/feather-cli/tests/test_dev_ports.py`.
- `test_webhook_to_http_p95_within_5s` on the CI runner, p95 82 s (`348ddf5`, test bug): the test's echo server wrote
  head and body separately with Nagle on, so each answer waited out Linux's 40 ms delayed ACK (about 20 calls a
  second). With `TCP_NODELAY` the Linux p95 is 2.99 s; the 5 s target is unchanged.

Seen while rerunning: when `pytest-timeout` stops a run of `tests/e2e/test_worker_process.py`, the worker
subprocesses it started outlive pytest and keep their database connections, so the next runs can time out connecting.
Clean runs pass (7 of 7, three times).

### Stage 5 focused runs

| Check | Result |
|---|---|
| `tests/deploy` | 7 passed |
| `tests/dashboard` | 27 passed |
| `tests/hard_cases` | 5 passed (after the I4 fix below; H1 failed before it) |
| `tests/e2e/test_webhook_to_http_latency.py`, test Postgres | passed: 3,000 of 3,000 delivered, none lost or repeated; numbers go in §1 from the reviewer's run |
| the same, durable Postgres (S19) | passed, as above; container and volume removed afterwards |
| feather-connect, Postgres marker | 243 passed (start of Stage 5), plus the S6 and M6 additions; feather-postgres 28 passed |

## 5. Open

| Item | State | Reopens |
|---|---|---|
| pyright and the add-on packages | `feather_connect` is a regular package extended with `pkgutil.extend_path`, which pyright does not follow, so imports of `feather_connect.connectors.http` and the templates carry ignores and type as `Unknown` there. `executionEnvironments` and a stub in `typings/` were tried and do not help. Ways out: a partial `feather_connect-stubs` package kept current by a test, or moving the add-ons under a namespace with no `__init__.py` | Plan 4 |
| OAuth2 tokens | shared per grant in a process, not across processes | M2 |
| Helpers of `feather dev` on macOS | a feather killed with -9 leaves its helpers (Windows: job object; Linux: PR_SET_PDEATHSIG) | Plan 6, Task 7 |
| Watermarks | `since_watermark` stops at the first version below the lookback; a lookback made longer later does not reach versions read before | design §6.2 |
| Idempotency-Key in the API | check then set: two identical requests at the same instant both run | Plan 5, Task 4 |
| `feather new --template` | rewrites feather.yaml and env files with PyYAML, so their comments are lost | Plan 6, Task 2 |
| Scheduler tick | a crash between the tick and the run loses that tick (documented) | Plan 3 |
| Stage 2 M13 "reason column cut off" | not reproduced at 360 px | — |
| Worker test subprocesses | outlive a run that `pytest-timeout` stops (see §4) | Plan 3 |

## 6. End-of-stage review

A review of `git diff origin/main..stage-5-exit`. Each Critical or Important finding was fixed with a failing test
first. There were no Critical findings.

| # | Severity | Finding | Fix |
|---|---|---|---|
| I1 | Important | ingress's rate limiter kept one bucket per caller address and never dropped any, so many addresses grow memory without a bound | `b1b9042`: at most 10,000 buckets; refilled ones are dropped first, then the oldest half |
| I2 | Important | the remote commands sent the env file's API token to any `--server`, including one that is not the env file's | `9df0eb8`: the token goes only to the env file's own server |
| I3 | Important | an append route to a Postgres table that lacks `_fc_version`, `_fc_run` or `_fc_extracted_at` failed on every delivery (found by H4) | `202fe35`: the Postgres sink adds those columns for append routes |
| I4 | Important | two first applies to a new schema at once raced on `create table if not exists _feather_applied`, and one failed for good on pg_type's unique index (found by H1) | `4ba0e7d`: `_prepare` takes a transaction advisory lock per schema |
| m1 | Minor | the server image ships the demo's `erp-token.txt` | open: harmless demo value, but the image should not carry it |
| m2 | Minor | `latest_watermark` takes `SEAL_LOCK` on each tick for a dataset that was never sealed | open: cheap, but a read that need not lock |
| m3 | Minor | `feather server` binds 0.0.0.0 by default (it warns) | open: design §13 to decide |
| m4 | Minor | the webhook-to-HTTP latency test inherited pytest's stdin, which is at its end, so the load stopped at once | fixed in `7d7e424`: it passes a pipe |

Checked and found sound: every API route needs an actor; the sealer (S11, S20); `sweep_stream_runs` ordering (seals are
resubmitted first); `feather new`; the images and Compose files.

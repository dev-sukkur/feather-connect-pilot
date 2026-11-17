# Traceability

Where each requirement in `specs/` comes from in the pilot repository (dev-sukkur/feather-connect-pilot, as of
2026-11-11), and how sure it is.

| Pilot status | Meaning | What a rebuild does |
|---|---|---|
| `verified` | Built, and a pilot test proves it | Port the test first, then build to it |
| `built` | Built; no passing pilot test proves it | Write the test from the scenario, then build |
| `partial` | Built in part; the note says which part | Build all of it |
| `corrected` | The pilot does this wrong; the requirement states the right behaviour | Build it right; the note names the defect |

IDs (FR-, NFR-, UC-, I1–I10, H1–H17, F, S, B, rulings, design sections, ADRs, milestones, exit reports) are defined in
the pilot's `docs/`, starting at `docs/04-plan/milestones-and-scope.md` §10. Paths: `core/…`, `runtime/…`,
`storage/…`, `packages/…`, `testing/…` and other bare module paths are under
`packages/feather-connect/src/feather_connect/`; `feather_server/…`, `feather_cli/…` and `feather_engine_dbos/…` are
under their package's `src/`; `<package>/tests/…` is under `packages/` (connectors under `packages/connectors/`);
`tests/…` is the repository's top-level `tests/`.

## access-control

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Local users | verified | FR-K1, design §13.1, `feather_server/auth.py`, `feather-server/tests/test_auth.py`, `feather-cli/tests/test_processes.py` |
| Sign-in is throttled | verified | NFR-10, `feather_server/auth.py` (`MAX_FAILURES`, `MAX_ADDRESS_FAILURES`), `feather-server/tests/test_auth.py` |
| Sessions | verified | design §10.1, `feather_server/auth.py` (`SESSION_TTL`), `feather-server/tests/test_auth.py` |
| API tokens | verified | FR-K1, design §13.1, `feather_server/auth.py`, `feather-server/tests/test_auth.py`, `feather-server/tests/test_api.py` |
| Two roles | verified | FR-K2 (M1.1 part), flow packages §11, `feather_server/services/access.py`, `feather-server/tests/test_roles.py` |
| Publish-only tokens | verified | flow packages §4.3, `feather_server/services/access.py` (`PUBLISH_SCOPE`), `feather-server/tests/test_registry.py` |
| Roles are checked in one place | verified | I9, design §9.1, `feather_server/services/access.py` (`require`), `feather-server/tests/test_roles.py`, `feather-server/tests/test_api.py` |
| Every action is audited | verified | FR-K4, I9, design §13.6, `storage/port.py` (`AuditStore`), `feather-server/tests/test_actions.py`, `feather-server/tests/test_roles.py` |
| Cross-site request forgery | verified | design §10.1, §13.7, `feather_server/auth.py`, `feather-server/tests/test_auth.py` |
| Content security policy | verified | design §10.1, §13.7, `feather_server/auth.py` (`CSP`), `feather-server/tests/test_web_shell.py` |
| Managing users and tokens | verified | flow packages §14 task 0, `feather_server/services/accounts.py`, `feather-server/tests/test_roles.py` |

## change-detection

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Incremental reads by change stamp | verified | FR-B2, design §4.3, §6.2, `core/strategies.py`, `runtime/extract.py`, `feather-connect/tests/runtime/test_extract.py` |
| A longer lookback reaches back | corrected | M1 exit report §5 "Watermarks": `since_watermark` stops at the first version below the old lookback. |
| Whole-slice diff for stampless sources | verified | FR-B2, H2, design §4.3, `tests/hard_cases/test_hard_cases.py::test_h2_snapshot_diff_on_a_stampless_table` |
| Full reload | verified | FR-B2, design §4.3, `core/strategies.py`, `feather-connect/tests/core/test_strategies.py` |
| Custom strategies | verified | FR-A2, design §4.3, `core/strategies.py` (`Strategy`) |
| Unchanged records are dropped | verified | H4, design §4.3, `tests/hard_cases/test_hard_cases.py::test_h4_late_rows_inside_the_lookback_captured_once` |
| A rewritten key keeps one current row | verified | H3, `tests/hard_cases/test_hard_cases.py::test_h3_source_rewrites_a_key_in_place` |
| Deletes across slices are decided for the whole run | verified | I10, H1, design §4.3, `runtime/seal.py`, `feather-connect/tests/runtime/test_seal.py` |
| Staged versions of failed runs are cleaned up | verified | F13, S12, `runtime/sweeper.py`, `runtime/retention.py` |
| Unreadable rows are quarantined at the source | verified | FR-C3, B5, design §4.7, `core/connector.py` (`Unreadable`), `runtime/extract.py` |
| Reads can resume | verified | B4, design §8.1, `core/context.py` (`checkpoint`), `runtime/extract.py` |

## connector-catalogue

| Requirement | Pilot | Trace and notes |
|---|---|---|
| JSON simulation connector | verified | UC-15, FR-J2, design §8.5, `connectors/simulation.py`, `feather-connect/tests/connectors/test_simulation.py`, `feather-connect/tests/connectors/test_simulation_conformance.py` |
| Postgres connector | verified | UC-01, FR-J2, design §8.3, I3 (M1 review), `feather-postgres/tests/test_postgres.py` |
| Object storage connector | verified | UC-05, FR-J2, design §8.3, B3, B5, ruling 8, `feather-s3/tests/test_s3.py` |
| REST source | verified | UC-02, FR-J2, design §8.3, `connectors/http.py`, `feather-http/tests/test_rest.py`, `feather-http/tests/test_conformance.py` |
| GraphQL source and sink | verified | FR-J2, design §8.3, `feather-http/tests/test_graphql.py` |
| HTTP call sink | verified | UC-04, FR-J2, rulings 5 and 7, S21, `connectors/http.py`, `feather-http/tests/test_call.py` |
| Webhook source | verified | FR-J2, ruling 6, design §8.3, `feather-http/tests/test_webhook_call.py` |
| DuckDB and MotherDuck connector | verified | FR-J2, design §8.3–8.4, `feather-duckdb/tests/test_duckdb.py` |
| Zoho Zakya connector | verified | FR-J2 (M1.1 part), design §8.4, milestones §4 "Zakya connector", `feather-zakya/tests/`, `tests/demo/test_zakya_demo.py`. The run against a live Zakya organisation is deferred; five points stay open until then (milestones §4). |
| Connectors are documented and tested | built | NFR-14, `packages/connectors/*/README.md`. Each connector has a README; the pilot has no docs site or docs CI. |

## connector-sdk

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Source contract | verified | FR-J1, design §8.1, `core/connector.py` (`Source`, `ReadRequest`), `feather-connect/tests/core/test_connector.py` |
| Sink contract | verified | FR-J1, FR-E3, design §8.1, `core/connector.py` (`Sink`, `ApplyBatch`, `ApplyResult`) |
| Push source contract | verified | FR-K6, design §8.1, §13.5, `core/connector.py` (`PushSource`), `feather-connect/tests/test_project.py` |
| Connectors declare a spec | verified | FR-J1, design §8.2, `core/connector.py` (`ConnectorSpec`, `secret_fields`), `feather-cli/tests/test_describe.py` |
| Third-party connectors need no fork | partial | FR-J4, B1, design §8.2, M1 exit report §5 "pyright and the add-on packages": the pilot's path-extension namespace is not followed by pyright; the rebuild uses a true namespace package. |
| The context | verified | design §8.1, `core/context.py`, `feather-connect/tests/core/test_context.py` |
| The shared HTTP client | verified | FR-E2, rulings 2, 3 and 4, design §8.1, `runtime/http.py`, `feather-connect/tests/runtime/test_http.py` |
| HTTP authentication | verified | ruling 3, design §8.3, `runtime/http.py`, `feather-connect/tests/runtime/test_http.py` |
| OAuth2 tokens are shared across processes | corrected | M1 exit report §5 "OAuth2 tokens": shared per grant in a process, not across processes. ADR-0013 rule 7. |
| The conformance kit | verified | FR-J3, design §8.7, `testing/conformance.py`, `feather-connect/tests/connectors/test_memory_conformance.py`, `feather-http/tests/test_conformance.py`, `feather-zakya/tests/test_conformance.py` |
| Maturity levels | verified | FR-J3, ruling 10, design §8.7, `core/connector.py` (`Maturity`) |
| In-memory connectors | verified | FR-J2, design §8.3, `feather-connect/tests/connectors/test_memory.py` |

## dashboard

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Server-rendered HTML with no front-end stack | verified | C7, ADR-0001, design §10.1, milestones §4.3, `feather_server/web/`, `tests/dashboard/test_operator_journeys.py::test_no_console_errors_or_csp_violations` |
| Read-only views work without JavaScript | verified | design §10.1, `tests/dashboard/test_operator_journeys.py::test_read_only_pages_work_with_javascript_disabled`, `::test_a_tasks_records_without_javascript` |
| The app shell | verified | milestones §4.3, `feather_server/web/templates/base.html`, `tests/dashboard/test_shell.py` |
| One pattern for list pages | verified | milestones §4.3, `tests/dashboard/test_shell.py::test_the_column_chooser_hides_a_column_and_remembers`, `::test_a_flow_row_opens_to_its_routes_and_runs_and_the_search_asks_the_server` |
| Pages | verified | FR-F1–F6, design §10.2, flow packages §10, `feather_server/web/routes.py`, `feather_server/web/package_pages.py`, `feather-server/tests/test_web_*.py` |
| Acting from the dashboard | verified | FR-G1, FR-G2, FR-G5, G3, design §10.2, `tests/dashboard/test_operator_journeys.py` |
| Safe actions | verified | FR-G2, design §10.3, `feather-server/tests/test_web_failures.py`, `tests/dashboard/test_operator_journeys.py::test_retry_after_a_failure` |
| Recovery in five clicks | built | NFR-11, G3, design §10. The journeys are tested; the click count has not been measured in a usability test. |
| From a failure to its cause | verified | UC-10, FR-F6, `tests/dashboard/test_operator_journeys.py::test_from_a_delivery_to_what_was_read`, `::test_trace_a_record_from_a_failed_delivery` |
| Every view is a URL | corrected | design §10.3, M1.1 exit report §4 "Links to packaged names": the pilot's links carry `%2F`. |
| Business words first | partial | design §10.3, `feather-server/tests/test_web_partitions.py`. Datasets from packages show keys only in the pilot. |
| Live pages | verified | design §10.1, `feather_server/web/templates/_overview_live.html`, `tests/dashboard/test_shell.py::test_a_row_opened_after_the_session_ends_goes_to_sign_in` |
| Accessibility | verified | NFR-13, milestones §4.3, `tests/dashboard/test_accessibility.py`, `tests/dashboard/test_operator_journeys.py::test_keyboard_only_retry`, `::test_keyboard_only_switch_of_a_packaged_flow` |
| Narrow screens | verified | milestones §4.3, `tests/dashboard/test_operator_journeys.py::test_pages_fit_a_phone_without_scrolling_sideways`, `::test_the_sidebar_opens_on_a_narrow_screen` |

## data-model

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Records are keyed by business identity | verified | I7, FR-A2, design §4.1, `core/records.py`, `feather-connect/tests/core/test_records.py` |
| Row hashes are stable | verified | design §4.3, `core/records.py`, `feather-connect/tests/core/test_records.py` |
| Partitions name business slices | verified | FR-B3, I7, design §4.4, `core/partitions.py`, `feather-connect/tests/core/test_partitions.py` |
| Partitions have business labels | partial | design §4.4, §10.3, `core/partitions.py`. The pilot shows only keys for datasets that come from packages (M1.1 exit report §4); the rebuild shows labels for all. |
| Versions are immutable batches | verified | I1, FR-B4, design §4.1, §6.2, `storage/port.py` (`VersionDraft`), `testing/store_contract.py` |
| Only complete versions imply deletes | verified | I4, design §4.2, `core/strategies.py`, `feather-connect/tests/core/test_strategies.py` |
| An empty complete read is refused | verified | design §4.3, `core/dataset.py`, `runtime/extract.py` |
| A moved record is an update | verified | I7, H1, design §4.4, `tests/hard_cases/test_hard_cases.py::test_h1_record_moves_partitions_leaves_no_stale_copy` |
| Record state is kept on request | verified | design §4.8, `storage/port.py` (`RecordStateStore`), `runtime/extract.py` |

## delivery

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Routes progress independently | verified | FR-B1, design §5.2, `tests/e2e/test_demo_fanout.py`, `tests/demo/test_demo_project.py` |
| Extraction never waits for delivery | verified | I5, FR-B5, H6, design §6.1, `tests/hard_cases/test_hard_cases.py::test_h6_destination_down_during_the_read_window` |
| Versions apply in seal order per partition | verified | I2, FR-B6, design §6.4, `runtime/apply.py`, `feather-connect/tests/runtime/test_apply.py` |
| A delivery is applied at most once | verified | I3, FR-E3, NFR-04, design §6.5, `tests/chaos/test_kill_mid_apply.py::test_kill_9_mid_apply_loses_and_duplicates_nothing` |
| Delivery states | verified | design §6.5, `storage/port.py` (`TRANSITIONS`), `testing/store_contract.py` |
| Every transition is recorded | verified | FR-E4, I9, design §6.5, `storage/port.py` (`Attempt`), `testing/store_contract.py` |
| Route modes | verified | design §4.5, ruling 1, B6, I3 (M1 review), `core/connector.py` (`RouteMode`), `feather-postgres/tests/test_postgres.py`, `feather-duckdb/tests/test_duckdb.py` |
| Sinks mark what they wrote | verified | ruling 1, I4 (M1 review), `feather-postgres/tests/test_postgres.py` |
| Errors are classified | verified | FR-E2, F8, design §6.10, `core/errors.py`, `runtime/apply.py`, `feather-connect/tests/test_logs.py` |
| A rejected record does not stop its version | verified | FR-C3, FR-G2, design §4.7, §6.4, `storage/port.py` (`QuarantineStore`), `feather-server/tests/test_actions.py` |
| A newer version resolves an older rejection | built | H10, design §6.4, `storage/port.py` (`QuarantineStore.supersede`). H10 is not in the pilot's hard-case suite. |
| Retry and skip | verified | FR-G2, design §6.5, `feather_server/services/actions.py`, `feather-server/tests/test_actions.py` |
| Replay | verified | FR-B8, F1, F2, F3, `feather_server/services/actions.py`, `tests/dashboard/test_operator_journeys.py::test_replay_from_the_dashboard` |
| Routes can be paused | verified | FR-D5, design §5.6, `feather-connect/tests/runtime/test_pause.py` |
| Destination answers are kept | verified | rulings 5 and 11, `core/connector.py` (`RecordResponse`), `storage/port.py` (`ResponseStore`), `feather-http/tests/test_call.py` |
| Pending deliveries are never orphaned | verified | F7, design §6.1, `runtime/sweeper.py`, `feather-connect/tests/runtime/test_sweeper.py` |

## deployment

| Requirement | Pilot | Trace and notes |
|---|---|---|
| An environment is a deployment | verified | FR-H1, G5, ADR-0006, design §11, `deploy/compose.yaml` |
| Compose topology | built | NFR-06, NFR-07, NFR-15, C3, ADR-0010, design §18.1, `deploy/compose.yaml`, `tests/deploy/test_compose.py`. The pilot's deploy test has not been run since the Compose file lost its project mount (M1.1 exit report §4). |
| Images | corrected | NFR-12, design §17, `deploy/Dockerfile.server`, `deploy/Dockerfile.worker`, `tests/deploy/test_compose.py::test_no_secret_in_the_images`. M1 review m1: the pilot's server image carries the demo's `erp-token.txt`. |
| Settings come from the environment | verified | design §18.1, `feather_server/settings.py`, `feather-server/tests/test_app.py` |
| The server binds safely by default | corrected | M1 review m3: the pilot binds 0.0.0.0 by default, with a warning. Design §13. |
| Migrations and upgrades | built | NFR-09, design §17, §18.5, `feather_cli/commands/processes.py` (`migrate`), `feather-connect/tests/storage/postgres/test_migrations.py`. No pilot test upgrades a running stack. |
| Administration from the server's command line | verified | design §13.1, `feather_cli/commands/processes.py`, `feather-cli/tests/test_processes.py`, `feather-cli/tests/test_dev_user.py` |
| Recovery | partial | NFR-05, design §7.5, `tests/chaos/test_kill_mid_apply.py::test_postgres_restart_during_apply`. The restart half is tested; no restore drill has been run. |
| Quality gates | partial | NFR-08, design §16, `.github/workflows/ci.yml`, M1 exit report §2. Coverage was 85.06% and 96.32% at the M1 gate. The pilot's last pull request merged before CI ended (M1.1 exit report §2); the last clause is the rule that was broken. |

## durable-execution

| Requirement | Pilot | Trace and notes |
|---|---|---|
| The engine sits behind a port | verified | ADR-0002, ADR-0003, design §6.8, `engine/port.py`, `feather-engine-dbos/tests/test_mapping.py` |
| Task arguments carry ids only | verified | I8, design §2 principle 5, `engine/port.py` (`TaskCall`) |
| What a task body may rely on | verified | design §6.8, `tests/engine_conformance/test_conformance.py::test_5_durable_sleep_across_restart`, `::test_6b_succeeded_step_not_rerun_on_retry` |
| Idempotent submission | verified | design §6.8, `tests/engine_conformance/test_conformance.py::test_4_dedup` |
| One task at a time per concurrency key | verified | FR-E7, design §6.2, `tests/engine_conformance/test_conformance.py::test_1_priority_and_one_at_a_time_per_key`, `feather-connect/tests/runtime/test_slice_concurrency.py` |
| Retries with backoff and jitter | verified | FR-E2, design §6.8, §6.10, `engine/port.py` (`RetryPolicy`), `tests/engine_conformance/test_conformance.py::test_6_retry_timing`, `feather-connect/tests/runtime/test_budgets.py::test_a_deferred_extract_does_not_use_up_an_attempt` |
| Killing any process loses no acknowledged work | verified | FR-E1, NFR-04, design §6.10, `tests/chaos/test_kill_mid_apply.py`, `tests/chaos/test_kill_mid_push.py`, `tests/engine_conformance/test_conformance.py::test_2_kill_mid_step_no_loss_no_duplicates`. The pilot's chaos suite passed at the M1 gate and needs rework for package hosts (M1.1 exit report §4). |
| Worker liveness | verified | design §6.12, F5, `worker.py`, `runtime/sweeper.py`, `feather-connect/tests/runtime/test_sweeper.py`, `tests/chaos/test_kill_mid_apply.py::test_kill_9_with_no_restart_is_recovered_by_the_sweeper` |
| Graceful drain | verified | design §6.11, `worker.py` (`DRAIN_S`), `tests/e2e/test_worker_process.py` |
| The sweeper | verified | F6, F7, F11, F13, design §6.1, `runtime/sweeper.py`, `feather-connect/tests/runtime/test_sweeper.py` |
| One leader for singleton loops | verified | design §3, §6.7, `storage/postgres/leader.py`, `feather-server/tests/test_loops.py` |
| Two engines, one contract | verified | design §6.8, `engine/local.py`, `feather-connect/tests/engine/test_local_engine.py`, `tests/engine_conformance/` |
| Engine conformance suite | verified | design §6.8, §6.12, `tests/engine_conformance/test_conformance.py` (tests 1–12, less the gateway relay, which is backlog) |
| Engine operating rules stay inside the adapter | verified | design §6.12, spike T2, T3, T10, `feather_engine_dbos/runtime.py`, `tests/engine_conformance/test_conformance.py::test_3_deploy_same_version_mid_drain` |

## environment-config

| Requirement | Pilot | Trace and notes |
|---|---|---|
| A project is configured by files | verified | FR-H1, design §5.1, §5.6, `project.py`, `feather-connect/tests/test_project.py` |
| An overlay can swap a connection for a simulation | verified | B2, ruling 9, design §11, `project.py`, `tests/demo/test_demo_project.py` |
| The environment has configuration of its own | verified | flow packages §8, `feather_server/services/environment.py`, `feather-server/tests/test_environment.py`, `feather-connect/tests/test_environment.py` |
| Plain settings are visible and audited | verified | milestones §4 "Connections in the database", `feather_server/services/environment.py`, `feather-server/tests/test_web_connections.py` |
| Secrets are references or encrypted values | verified | FR-K3, design §5.6, §13.3, ruling 3, `project.py` (`resolve_secret`), `feather-connect/tests/test_project.py` |
| Encrypted secrets in the database | verified | milestones §4 "Connections in the database", `vault.py`, `feather-server/tests/test_environment.py` |
| Secrets are write-only | verified | milestones §4, `feather_server/services/environment.py`, `feather_cli/commands/environment.py` (`secret_app`), `feather-server/tests/test_environment.py` |
| A secret opens only for its own connection | verified | `feather_server/services/environment.py` (`ENDPOINT_KEYS`), `feather-server/tests/test_environment.py` |
| Key rotation | verified | `vault.py` (`Keyring`), `feather_cli/commands/environment.py` (`rotate-key`, `new-key`), `feather-server/tests/test_environment.py` |
| Effective configuration shows its source | built | FR-F2, design §5.6, flow packages §8, `feather-server/tests/test_web_connections.py` |

## flow-authoring

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Flows are plain Python | verified | FR-A1, NFR-08, ADR-0007, design §5, `feather_connect/__init__.py`, `feather-connect/tests/test_smoke.py` |
| A dataset declares its source, key, partitions and strategy | verified | FR-A2, design §4, `core/dataset.py`, `feather-connect/tests/core/test_flow.py` |
| A flow declares what it reads, where it delivers and what starts it | verified | design §4.1, §5.2, `core/flow.py` (`Flow`, `spec_dict`), `feather-connect/tests/core/test_flow.py` |
| Routes are declared with `to` | verified | FR-B1, design §5.2, `core/flow.py` (`to`, `RouteSpec`) |
| Triggers | verified | FR-D1, FR-D2, design §5.2, `core/triggers.py`, `core/strategies.py` (`parse_duration`) |
| Record maps | verified | FR-C1, design §5.5, `core/transforms.py`, `feather-connect/tests/runtime/test_apply.py` |
| Batch maps | verified | FR-C1, design §5.5, `core/transforms.py` (`batch_map`) |
| Transforms are pure | built | design §5.5, `core/context.py` (`now`), `storage/port.py` (`Attempt.code_sha`). The rule is a convention; nothing enforces purity. |
| Static validation | verified | FR-A5, S17, S18, `core/flow.py` (`validate`), `feather-connect/tests/core/test_flow.py`, `feather-cli/tests/test_validate.py` |
| Validation codes | verified | FR-A5, S17, S18, `core/flow.py`, `feather-connect/tests/core/test_flow.py` |
| Route and dataset names are unique across a project | corrected | milestones §3.6: `feather validate` misses duplicate route names across flows. |
| A test harness that needs no infrastructure | verified | FR-A6, design §5.7, `testing/harness.py`, `feather-connect/tests/testing/test_harness.py` |
| Projects carry conventions for AI assistants | verified | FR-L2, design §14.1, `feather_cli/scaffolds/`, `feather-cli/tests/test_new.py` |

## flow-packages

| Requirement | Pilot | Trace and notes |
|---|---|---|
| A flow package is a Python wheel | verified | ADR-0011, flow packages §4.1–4.2, `packages/manifest.schema.json`, `packages/build.py`, `feather-connect/tests/packages/test_build.py` |
| Builds are reproducible and locked | verified | flow packages §14 task 1, `packages/build.py`, `feather-connect/tests/packages/test_build.py`, `feather-cli/tests/test_package.py` |
| A version means one set of bytes | verified | flow packages §4.2–4.3, `storage/port.py` (`DuplicateFile`, `DuplicateVersion`), `feather-server/tests/test_registry.py`, `feather-server/tests/test_packages.py` |
| The built-in registry | verified | flow packages §4.3, `feather_server/registry.py`, `feather-server/tests/test_registry.py` |
| Other registries | verified | flow packages §4.3, `feather_server/services/registries.py`, `feather-cli/tests/test_package.py`, `feather-server/tests/test_packages.py` |
| Install checks | verified | flow packages §5, `feather_server/services/packages.py` (`CHECKS`), `feather-server/tests/test_packages.py` |
| An installed package is stored whole | verified | flow packages §5, §9, `storage/port.py` (`PackageStore`), `feather-server/tests/test_packages.py` |
| Names carry the package prefix | verified | flow packages §8, `packages/link.py`, `feather-connect/tests/test_project.py`, `tests/e2e/test_package_hosts.py` |
| Packaged names are addressable without escaping | corrected | M1.1 exit report §4 "Links to packaged names": the dashboard encodes the slash as `%2F`. Plan §5.2 R1. |
| Promotion by checksum | verified | FR-H2, UC-14, flow packages §6, §16, `feather_cli/commands/package.py` (`promote`), `feather-server/tests/test_packages.py` |
| Optional signing | verified | flow packages §11, §16, `packages/signing.py`, `feather-server/tests/test_packages.py` |
| Uninstall | verified | flow packages §6, §16, `storage/port.py` (`InUse`), `feather-server/tests/test_packages.py` |
| Package commands | verified | flow packages §10, `feather_cli/commands/package.py`, `feather_cli/commands/registry.py`, `feather-cli/tests/test_package.py` |
| A package can't bring a webhook receiver as code | verified | flow packages §7.2, §12, `packages/check.py` (`own_push_sources`), `packages/prepare.py`, `feather-connect/tests/packages/test_check.py` |

## flow-templates

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Templates are library functions first | verified | FR-A7, ruling 12, design §8.6, `packages/templates/feather-generic/`, `tests/templates/test_templates.py::test_template_flow_calls_the_library` |
| Generic templates | verified | FR-A7, UC-01, UC-04, UC-05, design §8.6, `templates/`, `tests/templates/test_templates.py` |
| What every template ships with | verified | design §8.6, NFR-14, `tests/templates/test_templates.py::test_template_has_every_required_file`, `::test_template_validates_for_every_environment`, `::test_template_own_tests_pass` |
| Copying a template keeps the project's files intact | corrected | M1 exit report §5 "`feather new --template`": the pilot rewrites the YAML files and loses their comments. |
| A retail template for Zakya invoices | verified | milestones §3.4, `packages/templates/feather-retail/tests/test_retail.py`, `tests/demo/test_zakya_demo.py::test_each_invoice_reaches_the_receiver_once_and_a_changed_one_again` |

## local-dev

| Requirement | Pilot | Trace and notes |
|---|---|---|
| One command to a working environment | verified | G4, NFR-06, ADR-0010, design §18.2, `feather_cli/commands/dev.py`, `tests/dev/test_feather_dev.py::test_dev_shows_the_demo_flow_within_five_minutes` (67 s at the M1 gate), `::test_ctrl_c_then_restart_keeps_data_and_leaves_no_processes` |
| Reload on save | verified | flow packages §13, F10, `feather_cli/commands/dev.py`, `tests/dev/test_feather_dev.py::test_editing_a_flow_reloads_it`, `::test_a_reload_leaves_no_connections_behind`, `feather-cli/tests/test_dev_watch.py` |
| Clear failures at start-up | verified | design §18.2, `tests/dev/test_feather_dev.py`, `feather-cli/tests/test_dev_ports.py` |
| Helper processes | partial | `tests/dev/test_dev_helpers.py`, M1 exit report §5 "Helpers of `feather dev` on macOS": a killed `feather dev` leaves its helpers running on macOS. |
| Scaffolding | verified | FR-A3, design §5.8, `feather_cli/commands/new.py`, `feather-cli/tests/test_new.py` |
| Validate a project | verified | FR-A5, G2, design §5.8, §14.1, `feather_cli/commands/validate.py`, `feather-cli/tests/test_validate.py`, `tests/demo/test_demo_project.py::test_the_demo_project_is_sound` |
| Run a flow in process | corrected | FR-A4, design §5.8, `feather_cli/commands/run.py`, `feather-cli/tests/test_run.py`, `tests/demo/test_quick_start.py`. Milestones §3.6: `feather run --sim` fails with signed webhook sources in the pilot. |
| Test a project | verified | FR-A6, design §5.8, `feather_cli/commands/test.py` |
| Describe what is installed | verified | FR-L2, design §14.1, `feather_cli/commands/describe.py`, `feather-cli/tests/test_describe.py` |
| Every command speaks JSON | verified | FR-L2, design §5.8, `feather_cli/main.py`, `tests/dev/test_feather_dev.py::test_port_taken_with_json_is_still_one_json_document` |
| Development on three operating systems | partial | NFR-12, `.github/workflows/ci.yml`. CI covers Ubuntu and Windows; macOS is not in CI. |

## observability

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Structured logs with correlation ids | verified | FR-M1, design §15.1, `logs.py`, `feather-connect/tests/test_logs.py` |
| Logs never hold secrets | verified | FR-K3, F8, design §13.3, `logs.py` (`redact`), `feather-connect/tests/test_logs.py`, `feather-server/tests/test_ingress.py` |
| Task logs are kept for the dashboard | verified | FR-F4, design §15.1, `storage/port.py` (`RunJournal.log`, `task_logs`), `feather-server/tests/test_web_runs.py`, `feather-cli/tests/test_remote.py` |
| The task journal | verified | FR-F4, design §7.1, `storage/port.py` (`TaskRow`), `feather-connect/tests/runtime/test_task_scans.py` |
| A task's records | verified | FR-F6 (M1.1 part), milestones §4, `feather_server/services/task_records.py`, `feather-server/tests/test_task_records.py` |
| Record trace by business key | verified | FR-F6 (M1.1 part), UC-10, `feather_server/services/trace.py`, `feather-server/tests/test_trace.py` |
| Health and readiness | verified | design §9.2, `feather_server/app.py`, `feather-server/tests/test_app.py` |
| A summary of the environment | verified | FR-F1, design §9.2, `feather_server/services/reads.py`, `feather-server/tests/test_reads.py` |
| Worker code versions are visible | verified | FR-H2, design §6.11, `storage/port.py` (`WorkerRow`, `TaskRow.code_sha`) |

## operations

| Requirement | Pilot | Trace and notes |
|---|---|---|
| One service layer, several adapters | verified | design §2 principle 6, §9.1, `feather_server/services/actions.py`, `feather-server/tests/test_actions.py` |
| Run, re-run, backfill and cancel | verified | FR-G1, FR-D4, UC-08, design §9.2, `feather_server/services/actions.py`, `tests/dashboard/test_operator_journeys.py::test_rerun_from_the_dashboard` |
| Mass actions are previewed, then confirmed | verified | FR-G2, design §9.3, `feather_server/services/previews.py`, `feather-server/tests/test_actions.py`, `feather-server/tests/test_api.py` |
| Selecting deliveries | verified | FR-G2, FR-F5, `feather_server/services/actions.py` (`DeliverySelection`), `feather-server/tests/test_actions.py` |
| Reasons | partial | FR-H3, design §9.3, `feather_server/services/actions.py` (`protected`). The pilot requires a reason only for skip; wider production protections are backlog. |
| Verify a partition | partial | design §10.2, `runtime/extract.py` (`verify`), `feather-server/tests/test_actions.py`. Not available for datasets from packages in the pilot (M1.1 exit report §4); the rebuild runs it on the package's host. |
| Test a connection | verified | FR-G5, design §9.2, `feather_server/services/actions.py` (`check_connection`), `feather-server/tests/test_web_connections.py` |
| JSON API conventions | verified | design §9.2–9.3, `feather_server/api/routes.py`, `feather_server/api/errors.py`, `feather_server/api/paging.py`, `feather-server/tests/test_api.py` |
| API resources | verified | design §9.2, `feather_server/api/routes.py`, `feather-server/tests/test_api.py` |
| Requests can be retried safely | corrected | design §9.3, M1 exit report §5 "Idempotency-Key in the API": the pilot checks and then sets, so two identical requests at the same instant both run. `feather_server/api/routes.py`. |
| Remote commands | verified | FR-L2, design §5.8, I2 (M1 review), `feather_cli/commands/remote.py`, `feather-cli/tests/test_remote.py` |
| Held events | verified | S1, `feather_server/services/actions.py` (`reprocess_event`, `release_event`), `feather-server/tests/test_actions.py` |

## package-hosts

| Requirement | Pilot | Trace and notes |
|---|---|---|
| The server never loads package code | verified | ADR-0011, flow packages §7.2, `feather_server/catalog_view.py`, `runtime/catalogue.py`, `feather-server/tests/test_packages.py` |
| Workers are supervisors | verified | flow packages §7.1, `supervisor.py`, `feather-connect/tests/test_supervisor.py`, `tests/e2e/test_package_hosts.py` |
| A version is prepared before it can be activated | verified | flow packages §7.1, §16, `packages/prepare.py` (`PREPARE_STEPS`), `storage/port.py` (`VersionNotReady`), `feather-connect/tests/test_supervisor.py` |
| One dependency environment per lock | verified | flow packages §7.1, §14 task 8, `packages/envs.py`, `feather-connect/tests/packages/test_envs.py` |
| The load check runs in a throwaway process | verified | flow packages §5 step 4, §11, `packages/check.py`, `feather-connect/tests/packages/test_check.py` |
| Activate and deactivate per environment | verified | flow packages §6, `storage/port.py` (`FlowActivation`), `feather-server/tests/test_packages.py`, `tests/e2e/test_package_hosts.py` |
| Switching versions loses and doubles nothing | verified | flow packages §6, §7.1, `host.py`, `tests/chaos/test_package_switch.py::test_kill_9_of_a_host_during_a_switch_loses_and_duplicates_nothing` |
| Hosts are supervised | verified | flow packages §7.1, §11, `supervisor.py`, `packages/tree.py`, `tests/chaos/test_package_switch.py::test_kill_9_of_the_supervisor_leaves_no_host_behind_and_each_task_runs_once` |
| A host holds no secret of the worker's | verified | flow packages §11, M1.1 exit report §2, `supervisor.py`, `host.py`, `feather-connect/tests/test_host.py`, `feather-connect/tests/test_supervisor.py` |
| A database role for hosts | verified | flow packages §11, `storage/postgres/host_role.py`, `feather-connect/tests/storage/postgres/test_store.py` |
| Packaged datasets are fully operable | corrected | flow packages §12, M1.1 exit report §4 "Packaged datasets in the dashboard": the pilot shows keys only, and offers no partition check, backfill or write cost for packaged flows. |
| Flow commands | verified | flow packages §10, `feather_cli/commands/environment.py` (`flow_app`), `feather-server/tests/test_packages.py` |

## push-ingestion

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Ingress verifies, stores and answers | verified | I5, FR-D2, design §6.3, `feather_server/ingress.py`, `feather-server/tests/test_ingress.py`, `tests/e2e/test_webhook_to_sink.py` |
| Checks go from cheap to dear | verified | FR-K6, S4, S16, design §13.5, `feather_server/ingress.py`, `feather-server/tests/test_ingress.py` |
| Unsigned callers can't use up a source's rate | verified | S5, I1 (M1 review), `feather_server/ingress.py` (`RateLimiter`), `feather-server/tests/test_ingress.py` |
| Built-in signature schemes | verified | FR-K6, flow packages §7.2, milestones §4 "Webhooks in packages", `packages/webhooks.py`, `core/webhooks.py`, `feather-connect/tests/packages/test_webhooks.py`, `feather-connect/tests/core/test_webhooks.py` |
| Packages declare their webhooks | verified | flow packages §7.2, milestones §4 "Webhooks in packages", `packages/webhooks.py`, `feather-connect/tests/packages/test_webhooks.py` |
| An event is kept once | verified | S10, flow packages §7.2, `storage/port.py` (`WebhookEventStore`), `feather-server/tests/test_ingress.py` |
| The sealer batches events into versions | verified | design §6.3, S20, `runtime/sealer.py`, `feather-connect/tests/runtime/test_sealer.py` |
| Per-key order is kept | verified | FR-B6, S3, design §6.4, `runtime/sealer.py`, `feather-connect/tests/runtime/test_sealer.py` |
| One bad event does not block the rest | verified | S1, S2, design §6.4, `runtime/sealer.py`, `storage/port.py` (`InboxStore.hold`, `reprocess`, `release`), `feather-connect/tests/runtime/test_sealer.py` |
| Push datasets are sealed where their code is | verified | flow packages §7.1, `feather_server/package_push.py`, `host.py`, `tests/e2e/test_package_hosts.py` |
| Push latency | verified | FR-B10, NFR-01, design §6.3, `tests/e2e/test_webhook_to_http_latency.py`, `tests/e2e/test_webhook_latency.py`. Measured at the M1 gate: p95 1.856 s, and 2.004 s on a durable Postgres, 3,000 of 3,000 events. |
| Bursts recover | verified | H14, `tests/e2e/test_webhook_latency.py`. Measured at the M1 gate: 18,229 events, none lost. |
| Push work survives crashes | verified | S7, S11, `tests/chaos/test_kill_mid_push.py` |

## rate-budgets

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Budgets have several windows | verified | FR-E6, design §6.6, `storage/port.py` (`BudgetSpec`, `parse_limits`), `feather-connect/tests/runtime/test_budgets.py::test_parse_limits_reads_units_per_period` |
| Acquiring is all or nothing | verified | design §6.6, H7, `feather-connect/tests/storage/postgres/test_budgets.py`, `tests/engine_conformance/test_conformance.py::test_9_shared_budget` |
| Budgets attach to connections | verified | FR-E6, design §5.4, §6.6, `runtime/http.py`, `feather-connect/tests/runtime/test_http.py` |
| Priority reserve | verified | FR-E6, H7, design §6.6, `feather-connect/tests/runtime/test_budgets.py::test_two_flows_share_one_budget` |
| Exhaustion is a wait, not a failure | verified | H8, F12, S23, design §6.6, §6.10, `feather-connect/tests/runtime/test_budgets.py::test_budget_exhaustion_defers_and_does_not_fail`, `tests/chaos/test_overlap_and_budgets.py` |
| A request that can never fit fails with the reason | verified | F12, `feather-connect/tests/runtime/test_budgets.py::test_a_read_that_needs_more_than_the_budget_holds_fails_with_the_reason`, `tests/chaos/test_overlap_and_budgets.py::test_a_read_bigger_than_its_budget_fails_for_good_and_names_the_budget` |
| Usage can be read | verified | FR-F1, design §9.2, `storage/port.py` (`BudgetStore.usage`), `feather-server/tests/test_api.py` |
| Redefining a budget | verified | `storage/port.py` (`BudgetStore.define`), `testing/store_contract.py` |

## scheduling

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Every trigger writes a run | verified | I6, FR-E4, design §6.7, `runtime/scheduler.py`, `feather-connect/tests/runtime/test_scheduler.py` |
| Cron and interval triggers | verified | FR-D1, design §6.7, `feather-connect/tests/runtime/test_scheduler.py::test_cron_fires_in_the_flow_timezone`, `::test_cron_fires_once_a_day_over_both_dst_changes`, `::test_first_tick_after_start_does_not_backfill` |
| A tick fires exactly once | corrected | design §6.7, M1 exit report §5 "Scheduler tick": the pilot stores the last tick before the run, so a crash in between loses that tick. `feather-connect/tests/runtime/test_scheduler.py::test_restart_does_not_double_fire` proves the no-repeat half. |
| Overlap policy | verified | design §6.7, `feather-connect/tests/runtime/test_scheduler.py::test_overlap_skip_records_a_skipped_run`, `::test_overlap_queue_starts_anyway`, `::test_overlap_cancel_previous_cancels_the_active_run` |
| Catch-up after downtime | verified | design §6.7, `feather-connect/tests/runtime/test_scheduler.py::test_catch_up_latest_collapses_missed_ticks`, `::test_catch_up_each_is_capped_at_1000_and_the_cap_is_recorded` |
| High-frequency pollers share a daily run | verified | design §6.3, `core/triggers.py` (`STREAM_BELOW`), `feather-connect/tests/runtime/test_pollers.py` |
| Stream runs are closed and cleaned up | verified | S12, `runtime/runtime.py` (`sweep_stream_runs`), `feather-connect/tests/runtime/test_pollers.py` |
| Manual runs | verified | FR-D1, FR-G1, `feather-connect/tests/runtime/test_pause.py::test_manual_run_of_a_paused_flow_is_refused_with_a_reason`, `feather-server/tests/test_actions.py` |
| Pause and resume a flow | verified | FR-D5, design §5.6, `storage/port.py` (`OverrideStore`), `feather-connect/tests/runtime/test_pause.py` |
| Planning | verified | FR-B3, design §4.4, §6.2, `runtime/plan.py`, `feather-connect/tests/runtime/test_runtime.py` |
| Cancelling a run | verified | FR-G1, `feather-connect/tests/runtime/test_sweeper.py::test_a_cancelled_run_is_not_sealed`, `feather-server/tests/test_actions.py` |

## storage

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Postgres is the only required database | verified | NFR-07, C6, ADR-0005, design §7.1, §7.5, `tests/engine_conformance/test_conformance.py::test_7_schema_isolation` |
| One store contract, two implementations | verified | ADR-0002, `storage/port.py`, `testing/store_contract.py`, `feather-connect/tests/storage/test_memory_store.py`, `feather-connect/tests/storage/postgres/test_store.py`. The pilot keeps the contract in one file; the rebuild splits it by aggregate (plan §5.2 R2). |
| Sealing is one transaction | verified | I1, F4, design §6.1, `storage/port.py` (`VersionStore.seal`, `claim`, `StaleWriter`), `feather-connect/tests/runtime/test_double_run.py` |
| Spool chunks | verified | design §7.2, B3, ruling 8, `storage/spool.py`, `feather-connect/tests/storage/postgres/test_spool.py` |
| Spool throughput | verified | NFR-02, spike SP4, `tests/perf/test_spool_throughput.py` |
| The key index | verified | FR-F6, ruling 11, `storage/port.py` (`KeySighting`, `key_history`), `feather-server/tests/test_trace.py` |
| Retention | verified | design §7.3, F14, `runtime/retention.py`, `feather-connect/tests/runtime/test_retention.py` |
| Growth is managed by partitions | verified | R8, S14, design §7.4, `storage/postgres/spool.py`, `feather-connect/tests/storage/postgres/test_store.py` |
| Database connections are bounded | verified | F9, F10, `storage/postgres/connect.py`, `feather-cli/tests/test_dev_watch.py` |
| Forward-only migrations with one baseline | partial | NFR-09, design §17, `storage/postgres/migrate.py`. The pilot has 26 migrations from its own history; the rebuild starts with one. |

## tenancy

| Requirement | Pilot | Trace and notes |
|---|---|---|
| Every record carries a tenant | built | FR-I1, ADR-0006, ADR-0013 rule 1, `feather-connect/tests/storage/postgres/test_migrations.py::test_every_table_has_tenant_id` proves the column; no pilot test checks the keys and indexes. |
| All data access goes through a tenant-bound store | built | ADR-0013 rules 2 and 6, `storage/port.py` (`Store`). Rule 6 asks for tests with a second tenant; the pilot's store contract has none, so the rebuild adds them. |
| Names are unique per tenant | built | ADR-0013 rule 3; no two-tenant test was found in the pilot's store contract. |
| Work carries its tenant | built | ADR-0013 rules 4 and 7, `core/context.py` (`tenant`), `runtime/http.py` |
| Roles are held per tenant and user | verified | ADR-0013 rule 5, `feather-server/tests/test_roles.py` |
| A vendor account's budget may span tenants | verified | ADR-0013 rule 8, `storage/port.py` (`BudgetSpec.account`), `feather-connect/tests/storage/postgres/test_budgets.py` |

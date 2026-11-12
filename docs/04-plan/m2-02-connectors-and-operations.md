# M2 plan 2: SAP through CPI, approvals and operations

The rest of [M2](milestones-and-scope.md#5-m2-business-connectors-and-operations-8-weeks) after
[plan 1 (readiness)](m2-01-readiness.md), as the sponsor scoped it on 12 Nov 2026: the connectors are Zakya (done in
M1.1), Postgres and SAP through SAP's Cloud Platform Integration (CPI). Everything else connector-shaped is M6. It goes
on the same branch as plan 1, as one PR, with CI at the end.

## 1. What is there already

| Piece | State |
|---|---|
| Postgres | `feather-postgres`: `postgres.Query` (by_stamp, snapshot_diff, replace) and `postgres.Table` (upsert, append, replace_partition), beta. Nothing to build but the chain that uses it |
| Delivery states | `held` and `compensated` are in the ledger's transitions (design §6.5); nothing sets them yet |
| Budgets | Windows, `reserve` per priority and `acquire(..., priority)` exist (M1); no page |
| Roles | `admin` and `operator` (M1.1) |
| Audit | Every action is audited (`AuditStore`); no page to read it |
| Hard cases | H1–H4, H6, H13, H14, H16, H17 pass |

## 2. Stages

| # | Stage | What | Proves |
|---|---|---|---|
| A1 | Approvals | `fc.approval.when(pred)` and `fc.approval.always()` on a route. Before a chunk is applied, records that need approval hold the delivery `held` with the reason and the count; an `approver` approves (→ `pending`, applied as is) or rejects (→ `skipped`), with a reason, audited. API, CLI `feather holds`, the dashboard's Holds page | A held delivery applies nothing until approved; a rejected one never applies; both audited |
| A2 | Side-effect routes | A sink may implement `compensate(ctx, batch)`; its routes are side-effect routes. An operator compensates an applied delivery (→ `compensated`), audited. The duplicate guard is the destination's idempotency key, which `http.Call` and the CPI sink already send | H9: a worker killed mid-apply posts nothing twice; compensate reverses on request |
| C1 | SAP through CPI | `feather-sap-cpi`: `sap_cpi.Source`, a GET of an iFlow endpoint that returns JSON pages (by_stamp with a `since` parameter, or replace); `sap_cpi.Post`, a side-effect sink that sends each record to an iFlow endpoint with an idempotency header and implements `compensate` with a reversal endpoint. OAuth2 client credentials against the BTP token URL; the CSRF token fetched when the iFlow asks for one. A simulator, `sims/cpi.py`, as Zakya's | Conformance kit; token refresh; CSRF; idempotent resend; reversal |
| C2 | The reference chain | `examples/zakya-demo`: Zakya invoices → Postgres staging (`postgres.Table`), then staging → SAP through CPI once each run is in staging (`fc.on_ready(..., delivered_to="staging")`), with approval above an amount | The M2 exit criterion on `feather dev` |
| D1 | Schema policy | A version's schema (field → type) is recorded with it. Applying a version whose schema dropped a field, or changed a type, against the last applied one on the route holds the delivery `held` with reason `schema_change`; added fields pass. `schema="strict"` holds on any change, `"permissive"` on none, per dataset or route | H12 |
| D2 | A route added to a live flow | Its first delivery is bootstrapped: for each partition, the newest sealed version is delivered first, in partition order, then it follows live | H11 |
| D3 | Reconcile | `fc.reconcile(every=..., lookback=...)` on a by_stamp dataset: a scheduled snapshot read of the lookback that finds rows the stamp missed (back-dated into a closed period) | H5 |
| O1 | Budgets page | Each budget's windows, use and reserve, deferred tasks; priorities already work | H7, H8 |
| O2 | RBAC | Roles `viewer` and `developer` beside `operator` and `admin`, by design §13.2; grants `approver` and `pii-reader` | Each capability refused to a role without it, on API, HTML and CLI |
| O3 | Audit page | The audit log, filtered by actor, action and target | — |
| O4 | PII masking | `fc.Dataset(..., pii=["email", "phone"])`: those fields are masked in samples, trace and diff on the dashboard, API and CLI; a `pii-reader` may unmask, audited | Masked by default; unmasking audited |
| O5 | Version diff | Two versions of a partition: keys added, removed and changed, with the changed fields; masked as O4 | — |
| O6 | Alerts | Rules by design §15.4 that M2 has the data for (`run_failed`, `delivery_failed`, `budget_high`, `hold_waiting`, `schedule_skipped`, `dataset_stale` where a flow declares `max_lag`), evaluated by a server loop; channels: webhook, Slack, SMTP; deduplicated; acknowledge and snooze from the Alerts page | Each rule fires once and clears; a snoozed one is quiet |
| H | The rest of the hard cases | H5, H7–H12, H15 on the full stack | H1–H15 pass |

Order: A1, A2, C1, C2 (the exit criterion), then D1–D3, O1–O6, H. Docs (Operating Guide sections, plan status) go with
each stage.

## 3. Decisions taken while planning

- **The duplicate guard is the destination's idempotency key**, not a table of ours: a resent chunk carries the same
  key, which CPI's idempotent process call (or the iFlow's own dedupe on the header) answers once. The ledger already
  records each record's answer (`ResponseStore`), so a resend after a crash finds what was posted.
- **Approval holds the delivery, not single records.** A delivery whose chunk has any record that needs approval waits
  as a whole: a posting is all or nothing for the business, and partial holds would break the per-partition order.
- **Masking happens at the service layer**, so the HTML, API and CLI share it, as every read does.
- **Alerts keep their state in tables** (`alert_rule`, `alert`), not in logs, so they can be queried, acknowledged and
  retried.

## 4. Where it stands

All stages are built on `feat/m2-readiness` (PR 30), 12 Nov 2026.

| # | State |
|---|---|
| A1, A2 | Done: holds, approve and reject (API, CLI `feather holds`, Holds page); `compensate` on side-effect routes (`feather deliveries compensate`) |
| C1 | Done: `feather-sap-cpi` (`sap_cpi.Source`, `sap_cpi.Post`), OAuth2 client credentials, CSRF, idempotent resend, reversal; `sims/cpi.py` |
| C2 | Done: `examples/zakya-demo` Zakya → staging → SAP with approval above 5 lakh; `tests/demo` runs the chain on Postgres |
| D1 | Done: `schema_policy` default, strict, permissive or a function, per dataset or route. A field is typed from the version's rows; a held delivery's reason starts `schema_change` |
| D2 | Done: a new route's first delivery in a partition is the whole slice; by_stamp reads from the start for it, the other routes get only what changed |
| D3 | Done: `fc.reconcile(every=, lookback=)`, fired before ordinary ticks, the newest due tick only |
| O1 | Done: Budgets page and API; waits longer than a minute are listed (shorter ones are handled in the task). The page also lists budgets a package host defined |
| O2 | Done: viewer, operator, developer, admin; grants approver and pii-reader; `settings.protected` makes a developer's capabilities an admin's |
| O3 | Done: Audit page and `GET /api/v1/audit`, filtered by actor, action and the start of a target |
| O4 | Done: `pii=[...]` masks fields in records, versions, traces and diffs; `unmask` needs pii-reader and is audited. Keys are not masked |
| O5 | Done: `/versions/{id}/diff`, `feather diff`, `feather trace`; nets the versions between two of a partition |
| O6 | Done: six rules, webhook, Slack and SMTP channels, ack and snooze, rules per environment (`alert_rule`, `alert`, migration 0032) |
| H | H5, H7, H8, H10–H12, H15 added to `tests/hard_cases` and pass on the full stack. H9: `tests/chaos/test_kill_mid_apply.py` (no duplicate) and the demo chain's compensate. H7 and H8 use hour windows and check the wait, the reserve and the cap without waiting the hour out |

A deployment (Railway) comes after M3, as the sponsor said on 12 Nov 2026; M2's exit criterion runs in the reference organisation's qa/uat.

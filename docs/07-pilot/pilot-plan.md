# The pilot plan (M3.1 P1)

Feather Connect runs for a while on our own Railway deployment, privately, before any v1.0.0. This plan sets what
runs, for how long, who does what, what counts as success, and what decides v1.

**Status: proposed.** Each answer below is the recommended one, written so the pilot can start without waiting. The
sponsor confirms or changes them, and the table in section 9 records each decision with its date. The answers come
from the questions in the M3.1 plan's section 5 (`docs/04-plan/m3.1-01-flows-ai-pilot.md`).

## 1. What runs

The pilot has two phases, so that operating Feather is shaken out before real systems depend on it.

| Phase | Weeks | Flows | Systems |
|---|---|---|---|
| A: shake-out | 1–2 | The demo (`examples/demo`) on a schedule; the reference chain (`examples/zakya-demo`) against its simulators; UC-06's effective price as a derived dataset | Railway Postgres for real; Zakya, SAP CPI and the POS as the simulators in the repository |
| B: real systems | 3–6 | The reference chain: Zakya invoices to Postgres staging, then staging to SAP once each run is in staging, invoices above 5 lakh held for approval. UC-06's effective price from the SAP price list, with a night read window on SAP | Zakya's test organisation (read only); SAP CPI's test tenant (postings); Railway Postgres as staging. MotherDuck only if an account is ready |

- **Why test systems in phase B.** SAP postings are side effects. A mistake in a test tenant costs nothing; in
  production it costs a reversal. Production systems come after the pilot, with v1.
- **Phase B waits for access.** It starts when the Zakya test organisation and the SAP CPI test tenant can be
  reached from Railway (the open M3 Q1 item). Until then, phase A goes on.
- **What each phase must exercise.** Between them: a scheduled flow, a webhook, a SQL transform with a reference, a
  derived dataset recomputed on a cron, an availability window, an approval hold, a budget, a failed delivery
  retried and one skipped, a replay, and a worker restarted during a run (a redeploy does that).

## 2. How long, and who

- **Length.** Six weeks: two of phase A, four of phase B. A week is added for each week phase B starts late, up to
  four.
- **Roles.**

  | Role | Who | Does |
  |---|---|---|
  | Sponsor | the sponsor | Decides section 9; reads the weekly summary; signs the exit report |
  | Operator | one named person, with a deputy | Watches alerts in working hours; retries, skips, approves; writes findings |
  | Developer | the flows' author | Fixes flows and configuration; answers the operator within a working day |
  | Agent | Claude, through the read-only MCP with a viewer token | Investigates failures and writes up the cause; changes nothing |

- **Alerts.** Two channels: a Teams channel for the operator and the developer (the `teams` channel kind), and email
  to the operator as the fallback. `worker_missing` and `delivery_failed` go to both; budget and stale-dataset
  warnings go to Teams only. Out of working hours nothing is watched: the pilot accepts that a failure at night waits
  until morning, and the success criteria count working hours only.
- **The server's own health.** An uptime check on `https://<domain>/readyz` every minute (the Railway guide's step
  10), alerting the same Teams channel and email.

## 3. Success

The pilot succeeds when, over phase B, all of these hold:

| # | Criterion | Measured by |
|---|---|---|
| S1 | No delivery lost or duplicated | A weekly reconciliation: each route's applied records against its target (row counts and keys for Postgres staging; SAP document numbers against applied invoices), and `max writes per key = 1` |
| S2 | Every failure is seen through an alert before anyone reports it | Each finding of kind *failure* records whether an alert came first |
| S3 | A failed delivery is fixed, retried or skipped within 30 minutes of its alert, in working hours | The alert's `fired_at` against the retry or skip in the audit log |
| S4 | The investigation walk-through takes at most 5 clicks from the overview to the record trace | Done once a week by the operator, on a real failure if there was one |
| S5 | The server is up 99% of working hours | The uptime check's report |
| S6 | At least three real failures are investigated through MCP, and the agent's cause is the right one | The findings, with the agent's write-up attached |
| S7 | No secret, password or PII value appears in a log, an alert, an MCP answer or a finding | A search of the logs and findings at the end, and spot checks weekly |

S1 to S3 are the ones that decide v1 (section 5). S4 to S7 are for the exit report; a miss there is a finding.

## 4. Findings

- **Where.** A GitHub issue in this repository for each finding, labelled `pilot`, and one of `bug`, `ux`,
  `docs`, `ops` or `idea`. A failure adds the run, the delivery and the alert. The ledger
  (`.superpowers/sdd/README/progress.md`) gets a one-line weekly summary, not the findings themselves.
- **Severity.** *High*: data lost or duplicated, a wrong posting, a secret exposed, or the operator unable to fix
  something. *Medium*: a failure without an alert, a fix that took the developer, a misleading screen. *Low*:
  everything else.
- **Rhythm.** A 30-minute review each week: new findings, S1's reconciliation, what changes. A high finding is fixed
  before the next week's flows run, or the affected flow is paused.

## 5. What decides v1

v1.0.0 is tagged when all of these hold:

1. S1 to S3 held for the last four weeks of phase B.
2. Every high finding fixed; every medium one fixed or accepted in writing by the sponsor.
3. The decisions v1 still needs are taken: the package and organisation names (Q9), whether the docs site is
   published, the written acceptance of the security findings (section 6), and the repository's visibility.
4. The release checklist (`docs/06-release/release-checklist.md`) passes, as it is.

If the pilot ends without them, the exit report says what is missing, and the sponsor chooses between a longer pilot
and a narrower v1.

## 6. Decisions before real data

These come before phase B, because phase B reads and writes real business data.

| # | Decision | Recommended | Why |
|---|---|---|---|
| D1 | The repository's visibility | **Private** before phase B | It is public today. Flow code for real systems names endpoints and fields; private keeps them out of view. Nothing in the pilot needs it public |
| D2 | Security findings X2-3 and X2-5 to X2-10 | **Accept** for the pilot, in writing | Each is low, with its reason in `docs/06-release/security-review.md`. None changes with real data in a test tenant |
| D3 | X2-11, no multi-factor sign-in | **Accept for phase A; for phase B, put an access proxy in front of the domain** (for example Cloudflare Access with the organisation's sign-in), with `/mcp`, `/api` and `/ingress` allowed through on their own tokens | On Railway the dashboard is on the internet, which X2-11's acceptance assumed it wouldn't be. A proxy brings MFA without waiting for SSO (M9) |
| D4 | Backups | **Railway's daily snapshots for the pilot**, with one restore tried in week 1. **WAL archiving before production systems**, which is after the pilot | Phase B posts to a test tenant, where a lost day costs a re-run. Idempotency keys keep a re-run from posting twice where SAP CPI honours them. A day's loss of the ledger is not acceptable once production postings depend on it |
| D5 | Who may hold the admin role | **The developer and the sponsor**; the operator is an operator with the `approver` grant | Admin can make tokens and users. The operator needs to retry, skip and approve, and nothing more |

## 7. Week by week

| Week | What |
|---|---|
| 0 | Railway set up (the guide's steps 1–11); the first admin; alert channels; the uptime check; a usage limit; a viewer token for the agent; D1–D5 decided |
| 1 | Phase A: the demo on a schedule; a restore tried into a second environment; the operator walks a failed delivery end to end; `worker_missing` seen by scaling the worker to 0 |
| 2 | Phase A: the reference chain and UC-06 against the simulators; a window on the simulated SAP connection; the first MCP investigation |
| 3 | Phase B starts if access is there: Zakya read only, staging; SAP postings held for approval at first, all of them, until the first ten are checked by hand |
| 4–6 | Phase B as normal: the approval threshold back to 5 lakh; weekly reconciliation; findings fixed as they come |
| 7 | The exit report: S1–S7 with evidence, the findings and what became of them, and the recommendation on v1 |

## 8. Risks

| Risk | What we do |
|---|---|
| Test-system access comes late | Phase A goes on; the pilot extends, up to four weeks (section 2) |
| A failure at night goes unseen until morning | Accepted for the pilot; S3 counts working hours. Before production, an on-call rota or a pager channel |
| Railway's region or an outage | The uptime check tells us; the pilot pauses and resumes. Nothing is lost: work waits in the queue and the ledger |
| The agent says something wrong with confidence | Its write-up is checked by the operator before anyone acts on it; S6 counts only causes that were right |
| Cost runs over | The usage limit and its email alert (the guide's step 11) |

## 9. Decisions

| # | Question | Proposed | Decided | By, on |
|---|---|---|---|---|
| 1 | Flows and systems | Section 1 | | |
| 2 | Length and roles | Section 2: six weeks; one operator with a deputy | | |
| 3 | Alert channels | Teams and email | | |
| 4 | Success criteria | Section 3 | | |
| 5 | Where findings go | GitHub issues labelled `pilot` | | |
| 6 | What decides v1 | Section 5 | | |
| D1–D5 | Before real data | Section 6 | | |

# M3 plan 1: test environments, hardening and v1.0

All of [M3](milestones-and-scope.md#6-m3-enterprise-and-v10-8-weeks), as scoped on 12 Nov 2026: light tenancy
([ADR-0013](../02-design/adr/0013-light-tenancy-and-non-prod-layout.md)), the protections of protected environments, a
Compose layout for the test environments, hardening with published results, and the v1.0.0 release. The sponsor cut the
fleet view and SSO on 12 Nov 2026 (section 4). M3 ends with the reference chain in the reference organisation's qa/uat and the
cut-over to prod. The Railway deployment comes after M3.

Requirements: FR-H3 (production protections) and the NFRs as the release checklist checks them. UC-13 and full tenancy
are M9, not planned. FR-F8 (fleet view), FR-H4 (several environments on one dashboard) and FR-K1's SSO half went to M9
too.

## 1. What is there already

| Piece | State |
|---|---|
| Tenancy | `tenant_id` on every table and in every key, a store bound to one tenant, `/ingress/{tenant}/{source}`, only `default` accepted (M1). Account budgets in tenant `*` rows (M1.1) |
| Protected environments | `settings.protected`: a developer's capabilities need an admin (M2 O2); a skip needs a reason; `run_failed` alerts on. Mass actions already go preview → token → confirm |
| Health | `/healthz`, `/readyz`, `GET /api/v1/summary` |
| Auth | Local users (argon2), hashed tokens with role and scope, roles and grants (M2) |
| Compose | `deploy/compose.yaml`: Postgres, a one-shot `migrate`, server, workers: four containers (NFR-06). Profiles `objects`, `sso` and `observability` reserved, empty |
| Perf | `tests/perf/test_spool_throughput.py` (NFR-02) and `tests/e2e/test_webhook_latency.py` (NFR-01). Both were red on shared CI runners at times |
| Chaos | `tests/chaos`: kill mid-apply, kill mid-push, overlap and budgets, package switch (NFR-04) |
| CI | Lint, pyright, tests with an 85% coverage gate, Postgres, engine conformance, chaos, dashboard with axe. No security scans |
| Release | `release.yml` builds multi-arch images to GHCR on a tag. No SBOM, no signing, no PyPI, no release-please |
| Governance | `LICENSE` (MIT). No `SECURITY.md`, `CONTRIBUTING.md`, Code of Conduct or CODEOWNERS |
| Docs | The design, plans and the Operating Guide (HTML) in the repository. No docs site |
| After a restore | The sweeper re-submits runs; nothing marks a version with missing chunks `lost` |
| Open items | M1 exit report §5–§6 and milestones §3.6: `feather server` binds 0.0.0.0, the demo's `erp-token.txt` in the server image, the `Idempotency-Key` race, a lost scheduler tick, and the rest listed there |

## 2. Stages

| # | Stage | What | Proves | Estimate |
|---|---|---|---|---|
| T1 | Tenancy checks | Check every table, query and process-wide cache against ADR-0013's eight rules, fix what breaks one (two found while planning, both rule 7: the leader lock is keyed by the loop's name alone, and DBOS queue names carry no tenant), and add the check to the review list. Tests that create a second tenant in the store and prove no read, write, budget or spool file crosses over (rule 6) | The rules hold in code; a second tenant sees nothing of the first | 0.5–1 day |
| E3 | Protected environments | In a protected environment: a reason for every state-changing call (API, HTML, CLI), not only skip; destructive actions (a mass skip, compensate, purge) wait as a hold for an `approver` other than the one who asked; tokens made there default to `viewer` (least privilege). The banner shows the environment is protected. Agent tokens that may only request come with MCP in M8 | FR-H3: each action refused without a reason; a destructive one waits for a second person | 1 day |
| E4 | Test environments (layout B) | `deploy/compose.nonprod.yaml`: one Postgres server with two databases, `feather_qa` and `feather_uat`, and a server and a worker for each, with its own `.env`, secrets and port. No dev environment: developers use `feather dev`. The test environments are used now and then, not watched all the time. The Deploying README section on it, and on keeping production apart | ADR-0013 decision 3: two test environments on one machine, neither reaching the other's data | 0.5 day |
| P1 | Performance | Make NFR-01 and NFR-02 reliable on CI (a dedicated job, a warm-up, the number recorded, a regression of more than 20% fails); add NFR-03, 5 million records a day on the *small* deployment, as a sustained run (nightly, not per PR). Results go to `docs/06-release/perf-results.md` with the machine they ran on | NFR-01–03 met and published | 1–1.5 days |
| P2 | Chaos and recovery | Add a Postgres restart and network delay to `tests/chaos`, and the NFR-05 checks: a server restart costs a running task at most one retry. A restore drill on Postgres PITR (a scripted run against a scratch database, timed for RTO ≤ 1 h), written as a runbook. Results to `docs/06-release/chaos-results.md` | NFR-04 and NFR-05; the drill is repeatable for the release checklist | 1 day |
| X1 | Security scans | CI on every PR: Bandit (SAST), pip-audit on `uv.lock`, Trivy on both images; release: an SBOM (CycloneDX) and keyless signing of the images (cosign). `SECURITY.md`, CODEOWNERS on auth, access, secrets and migrations | NFR-10's "no known critical or high vulnerabilities", SBOM and signed images | 0.5–1 day |
| X2 | Security review | A review of the control plane against design §13 and OWASP ASVS level 2: authentication, sessions, access control on every route (a test that lists every route and the capability it checks), input handling, secrets and redaction, webhooks, audit. Fix the findings, and the M1 open items that are security ones (the 0.0.0.0 default, the demo token in the image). Written up in `docs/06-release/security-review.md` | Every finding closed or accepted by the sponsor in writing | 1–1.5 days |
| L1 | Docs site | MkDocs Material, published to GitHub Pages from `main`: getting started, concepts, the Operating Guide, connector and template reference (each with a runnable example tested in CI, NFR-14), the API reference from OpenAPI, the CLI reference from `--help`, runbooks (design §18.5: failed deliveries, an exhausted budget, Postgres growth, a restore drill, an engine upgrade, secret rotation) | NFR-14; the docs CI fails when an example breaks | 1 day |
| L2 | Release checklist and v1.0.0 | `docs/06-release/release-checklist.md`, one line per NFR with its evidence (§3 below), the restore drill, the security review, the upgrade from the last release. release-please, PyPI by trusted publishing, `CONTRIBUTING.md` with DCO, a Code of Conduct. Then tag v1.0.0 and write the M3 exit report | The checklist complete; v1.0.0 on PyPI and GHCR | 0.5–1 day |
| Q1 | The reference chain in qa/uat | Zakya → Postgres staging → SAP through CPI, with approvals, in the reference organisation's qa and uat, deployed by layout B, against a Zakya test organisation and a SAP test client (ADR-0013). Run by us once access is given | The M3 exit criterion | 0.5 day once access is given |
| Q2 | Prod cut-over | The reference organisation's in-scope flows move to prod, on its own deployment, by the reference organisation's people with our help | The M3 exit criterion | Theirs |

Build total: about 7–9.5 days. The development (T1, E3, E4) is about 2–2.5 days; the hardening code (P1, P2, X1 and
X2's fixes, with the M1 open items) about 4–5; the docs site and the release about 1.5–2. The M3 window is ~8 weeks.

Order: T1, E3, E4 (the first PR), then P1, P2, X1, X2, L1, L2 (the second PR, ending in v1.0.0), then Q1 and Q2.
Docs (Operating Guide sections, this plan's §4) go with each stage. The full suite runs once per PR, with CI at the end
of each.

## 3. The release checklist

Each NFR (discovery §8), and how v1.0.0 shows it is met.

| NFR | Evidence |
|---|---|
| 01 Latency | P1: webhook to destination p95 ≤ 30 s (≤ 5 s target) at 50 events/s, published |
| 02 Throughput | P1: ≥ 20,000 rows/s per worker, published |
| 03 Capacity | P1: ≥ 5 million records/day on the *small* deployment, published |
| 04 Durability | P2: the chaos suite, nightly and at release |
| 05 Recovery | P2: one retry at most after a server restart; the restore drill within RTO 1 h, RPO 5 min |
| 06 Runnability | A clean install to the demo in the dashboard in ≤ 5 min, timed by hand at release; Compose ≤ 4 containers |
| 07 Footprint | Architecture review: Postgres ≥ 15 without extensions is the only state |
| 08 Maintainability | CI gates: pyright, ruff, coverage ≥ 85% (the gate today covers SDK and server together) |
| 09 Compatibility | Semver from 1.0 by release-please; migrations forward-only (Alembic, expand then contract) |
| 10 Security | X1 and X2 |
| 11 Usability | A timed walk-through: an operator finds, understands and retries or replays a failed delivery in ≤ 5 clicks |
| 12 Portability | `release.yml` builds linux/amd64 and arm64; the CI matrix runs Linux and Windows |
| 13 Accessibility | axe checks on the core screens, in CI already, extended to the M2 and M3 pages |
| 14 Documentation | L1's docs CI |
| 15 Cost | Architecture review: one VM, no paid licence |

## 4. Decisions

Decided by the sponsor, 12 Nov 2026:

- **No fleet view in v1** (E1, E2 cut). The test environments are used to test, not watched all the time, so one
  dashboard per environment is enough. FR-F8 and FR-H4 go to M9.
- **No dev environment.** Developers use `feather dev`; the shared test environments are qa and uat, or one of them.
- **Test environments by layout B, a database each**, not Salesforce-style tenants. With two environments the machine
  is most of the cost. A tenant per process saves only a database, and depends on our tenant filtering with no
  row-level security behind it; one server and one set of workers for both is most of full multi-tenancy, now M9. T1's rule 7 fixes keep that
  step small for later.
- **No SSO in v1 for now** (S1 cut). Local users and tokens, as today. FR-K1's SSO half goes to M9.
- **No restore cleanup stage** (R1 cut). With everything in Postgres, one restore brings chunks, ledger and engine
  state back to the same moment, and deliveries sent again after it are deduplicated by the destination (idempotency
  keys, upserts). P2's restore drill checks it. Marking versions with missing chunks `lost` comes with the
  object-storage spool in M4.

Proposed here, for the sponsor's review:

- **Two PRs, then the exit.** The development (T1, E3, E4) in one; hardening and the release in the second, ending in
  the v1.0.0 tag. Q1 and Q2 need no code PR of their own unless they find a bug.
- **Destructive actions in a protected environment are two-person**: a hold an `approver` other than the asker decides,
  reusing M2's holds.
- **The `objects` and `observability` profiles are not in v1.** The object-storage spool and Prometheus and
  OpenTelemetry export are M4, as the scope says; the `sso` profile waits with SSO.
- **Perf numbers are published with the machine they ran on**, since shared CI runners vary; the nightly job fails on a
  regression of more than 20%, not on one slow run.

Questions for the sponsor, also in the ledger:

1. When will qa/uat access be given, with a Zakya test organisation and a SAP CPI test tenant?
2. The GitHub organisation for the images and PyPI names (discovery Q9), needed before v1.0.0.
3. Is GitHub Pages the place for the docs site?

## 5. Where it stands

Planned, 12 Nov 2026. Nothing built yet.

# M3 plan 1: environments, SSO, hardening and v1.0

All of [M3](milestones-and-scope.md#6-m3-enterprise-and-v10-8-weeks), as scoped on 12 Nov 2026: light tenancy
([ADR-0013](../02-design/adr/0013-light-tenancy-and-non-prod-layout.md)), environment peers and the fleet view, the
protections of protected environments, a Compose layout for the test environments, OIDC SSO, hardening with published
results, and the v1.0.0 release. M3 ends with the reference chain in the reference organisation's qa/uat and the
cut-over to prod. The Railway deployment comes after M3.

Requirements: UC-13 is M5 now (ADR-0013 moved full tenancy there); FR-F8 (fleet view), FR-H3 (production protections),
FR-H4 (several environments on one dashboard), FR-K1's SSO half, and the NFRs as the release checklist checks them.

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
| T1 | Tenancy checks | Check every table, query and process-wide cache against ADR-0013's eight rules, fix what breaks one, and add the check to the review list. Tests that create a second tenant in the store and prove no read, write, budget or spool file crosses over (rule 6) | The rules hold in code; a second tenant sees nothing of the first | 0.5–1 day |
| E1 | Environment peers | `feather.environment_peer` (tenant, name, URL, the read-only token encrypted as database secrets are, M1.1). Admin API `/api/v1/environment-peers` (list, add, remove; audited), `feather env peers add/list/remove`. The token is a `viewer` token of the other environment; ours is never sent to the browser | An admin adds a peer; a non-admin can't; the token never leaves the server | 0.5 day |
| E2 | Fleet page | `/fleet`: the server fetches each peer's `/api/v1/summary` (short timeout, in parallel, cached a few seconds) and renders one card per environment: name, protected or not, flows failing, deliveries waiting and held, budgets near their cap, open alerts, the version it runs. A peer that doesn't answer shows as unreachable, not as an error page. Each card links to that environment's own dashboard. No cross-environment actions | FR-F8, FR-H4: one page across dev, qa, uat and prod; prod can only be changed from prod | 1 day |
| E3 | Protected environments | In a protected environment: a reason for every state-changing call (API, HTML, CLI), not only skip; destructive actions (a mass skip, compensate, purge) wait as a hold for an `approver` other than the one who asked; tokens made there default to `viewer` (least privilege). The banner shows the environment is protected. Agent tokens that may only request come with MCP in M4 | FR-H3: each action refused without a reason; a destructive one waits for a second person | 1 day |
| E4 | Layout B | `deploy/compose.nonprod.yaml`: one Postgres with `feather_dev`, `feather_qa` and `feather_uat`, a server and a worker per environment, each with its own `.env` and secrets, peered with each other. The Deploying README section on it, and on keeping production apart | ADR-0013 decision 3: three environments on one machine, none reaching another's data | 0.5 day |
| S1 | OIDC SSO | Sign-in through an OIDC provider (authorization code with PKCE): Google Workspace, Entra ID or Keycloak by configuration (issuer, client ID, client secret as a reference). A user is made or matched on first sign-in; a claim (`groups` by default) maps to a role and grants per environment, in `settings.sso`. Local users stay, and an admin can turn password sign-in off. Sessions end with the provider's token's lifetime; sign-out. The `sso` Compose profile runs Keycloak with a test realm | Sign-in, group to role mapping, a user with no mapped group refused, a role change taking effect at the next sign-in, tested against Keycloak in CI | 1.5–2 days |
| R1 | After a restore | What design §7.5 asks: after a restore, versions whose chunks are missing are marked `lost` and their partitions read again; deliveries stuck in `running` go back to `pending`. `feather server check-restore` runs it on demand, and the server runs it at start-up | A version with a missing chunk becomes `lost` and is read again; nothing is delivered twice | 0.5 day |
| P1 | Performance | Make NFR-01 and NFR-02 reliable on CI (a dedicated job, a warm-up, the number recorded, a regression of more than 20% fails); add NFR-03, 5 million records a day on the *small* deployment, as a sustained run (nightly, not per PR). Results go to `docs/06-release/perf-results.md` with the machine they ran on | NFR-01–03 met and published | 1–1.5 days |
| P2 | Chaos and recovery | Add a Postgres restart and network delay to `tests/chaos`, and the NFR-05 checks: a server restart costs a running task at most one retry. A restore drill on Postgres PITR (a scripted run against a scratch database, timed for RTO ≤ 1 h), written as a runbook. Results to `docs/06-release/chaos-results.md` | NFR-04 and NFR-05; the drill is repeatable for the release checklist | 1 day |
| X1 | Security scans | CI on every PR: Bandit (SAST), pip-audit on `uv.lock`, Trivy on both images; release: an SBOM (CycloneDX) and keyless signing of the images (cosign). `SECURITY.md`, CODEOWNERS on auth, access, secrets and migrations | NFR-10's "no known critical or high vulnerabilities", SBOM and signed images | 0.5–1 day |
| X2 | Security review | A review of the control plane against design §13 and OWASP ASVS level 2: authentication, sessions, access control on every route (a test that lists every route and the capability it checks), input handling, secrets and redaction, webhooks, audit. Fix the findings, and the M1 open items that are security ones (the 0.0.0.0 default, the demo token in the image). Written up in `docs/06-release/security-review.md` | Every finding closed or accepted by the sponsor in writing | 1–1.5 days |
| L1 | Docs site | MkDocs Material, published to GitHub Pages from `main`: getting started, concepts, the Operating Guide, connector and template reference (each with a runnable example tested in CI, NFR-14), the API reference from OpenAPI, the CLI reference from `--help`, runbooks (design §18.5: failed deliveries, an exhausted budget, Postgres growth, a restore drill, an engine upgrade, secret rotation) | NFR-14; the docs CI fails when an example breaks | 1 day |
| L2 | Release checklist and v1.0.0 | `docs/06-release/release-checklist.md`, one line per NFR with its evidence (§3 below), the restore drill, the security review, the upgrade from the last release. release-please, PyPI by trusted publishing, `CONTRIBUTING.md` with DCO, a Code of Conduct. Then tag v1.0.0 and write the M3 exit report | The checklist complete; v1.0.0 on PyPI and GHCR | 0.5–1 day |
| Q1 | The reference chain in qa/uat | Zakya → Postgres staging → SAP through CPI, with approvals, in the reference organisation's qa and uat, deployed by layout B, against a Zakya test organisation and a SAP test client (ADR-0013). Run by us once access is given | The M3 exit criterion | 0.5 day once access is given |
| Q2 | Prod cut-over | The reference organisation's in-scope flows move to prod, on its own deployment, by the reference organisation's people with our help | The M3 exit criterion | Theirs |

Build total: about 10.5–13.5 days. That is more than the rough 6–9 days given on 12 Nov 2026, which counted the
restore handling, the security review's fixes and the release work too lightly. The M3 window is ~8 weeks.

Order: T1, E1–E4, S1, R1 (the first PR), then P1, P2, X1, X2, L1, L2 (the second PR, ending in v1.0.0), then Q1 and Q2.
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

## 4. Decisions to take while planning

Proposed here, for the sponsor's review:

- **Two PRs, then the exit.** Environments, SSO and restore handling in one; hardening and the release in the second,
  ending in the v1.0.0 tag. Q1 and Q2 need no code PR of their own unless they find a bug.
- **The fleet view reads, never acts.** Peers are reached server-side with a `viewer` token of the other environment;
  the browser never holds it. Each environment keeps its own login and roles (design §10.4).
- **SSO maps groups to roles per environment**, so a group can be `developer` in dev and `viewer` in prod. Local users
  stay as the break-glass path, which an admin can turn off.
- **Destructive actions in a protected environment are two-person**: a hold an `approver` other than the asker decides,
  reusing M2's holds.
- **The `objects` and `observability` profiles are not in v1.** The object-storage spool and Prometheus and
  OpenTelemetry export are M4, as the scope says; only the `sso` profile gets content in M3.
- **Perf numbers are published with the machine they ran on**, since shared CI runners vary; the nightly job fails on a
  regression of more than 20%, not on one slow run.

Questions for the sponsor, also in the ledger:

1. Which OIDC provider does the reference organisation use (Google Workspace, Entra ID, another)?
2. When will qa/uat access be given, with a Zakya test organisation and a SAP CPI test tenant?
3. The GitHub organisation for the images and PyPI names (discovery Q9), needed before v1.0.0.
4. Is GitHub Pages the place for the docs site?

## 5. Where it stands

Planned, 12 Nov 2026. Nothing built yet.

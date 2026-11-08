# ADR-0013: Light multi-tenancy in v1, and one machine for the test environments

- **Status:** Accepted
- **Date:** 2026-11-08
- **Deciders:** OSS Dev1 (sponsor)
- **Amends:** [ADR-0006](0006-environment-is-deployment-tenant-is-namespace.md), its tenant half. Its environment half stands.

## Context

ADR-0006 made a tenant a namespace inside an environment, enforced by Postgres row-level security, with per-tenant connections, secrets, budgets, quotas, roles and optional worker pools. Discovery put that in v1 (FR-I2 to FR-I4, M3). M1 built the foundation: `tenant_id` on every table, in every primary key and unique index; a store bound to one tenant that filters every query by it; `/ingress/{tenant}/{source}`; spool files under the tenant. Only the `default` tenant is accepted.

Two questions came up on 8 Nov 2026: is full multi-tenancy needed for v1, and should the test environments (dev, qa, uat) share one deployment? Salesforce was the reference: each sandbox is a separate org, isolated at the data level, and sandboxes are cheap because they are tenants on shared non-production infrastructure, never on production's.

## Decision

**1. Light multi-tenancy in v1.**
- v1 keeps the M1 foundation and the single `default` tenant.
- Full multi-tenancy moves **after v1**, to milestone M5 (set on 9 Nov 2026): choosing the tenant per request and per task, row-level security, per-tenant config, secrets, budgets and roles, quotas, worker pools, a tenant switcher and cross-tenant tests (FR-I2, FR-I3, FR-I4).
- Serving several customers in v1 means a deployment per customer per environment.

**2. Rules that keep full multi-tenancy cheap to add.** Every builder follows them from now on, and reviews check them:
1. Every new table has `tenant_id` in its primary key and its unique indexes.
2. All data access goes through the tenant-bound store. No SQL reads across tenants, except an explicit, audited admin path.
3. Names are unique per tenant, not globally: packages, datasets, routes, readiness sets.
4. Run, task, firing and event payloads carry the tenant explicitly.
5. Roles are stored per (tenant, user), from M1.1's `admin` and `operator` on.
6. Tests use a second tenant now and then, to prove nothing leaks.
7. Anything process-wide (caches, OAuth tokens, engine queues) is keyed by tenant, or says why it can't be.
8. Vendor budgets get a scope that can be wider than one tenant, because two tenants may share one vendor account.

**3. Production stays on its own; the test environments share one machine (layout B).**
- Production is always its own deployment, on its own machines, as ADR-0006 says.
- dev, qa and uat may run on one non-production machine: one Postgres server with a database each (`feather_dev`, `feather_qa`, `feather_uat`), and a small server and worker per environment. Each environment keeps its own database, processes and secrets, so none can reach another's data.
- Environments as namespaces inside one deployment, Salesforce's model, waits for full multi-tenancy.
- Test environments use the vendors' own test systems (a SAP test client, a Zakya test organisation), never production accounts. That protects production's API allowances and stops test postings reaching real systems.

**4. Sandbox refresh goes on the backlog.** Like Salesforce's sandbox types: dev runs simulation connectors (like Developer, no data); `feather env refresh qa --from prod --sample <template>` copies a masked sample of recent production versions (like Partial Copy); a full masked refresh serves uat and performance tests (like Full). Not scheduled; it comes after v1 unless the sponsor brings it forward.

## Consequences

**Benefits:**
- v1 is smaller. M3 loses its largest new piece, and the security review has no cross-tenant risk to cover.
- The data model doesn't change when full multi-tenancy comes. What's left is mostly additive, with worker pools and quotas the largest part.
- Test environments cost close to one deployment, with no code change.

**Costs:**
- Several customers in v1 means several deployments to run.
- The rules in decision 2 must be kept by every change, or adding tenancy later gets dearer.
- Discovery's v1 scope changes: FR-I2 to FR-I4 move after v1.

## Alternatives considered

- **Full multi-tenancy in v1 (ADR-0006 as written).** One deployment for many customers, but it is the largest part of M3, and the reference organisation needs one tenant.
- **No tenant column at all.** Simpler, but a later retrofit would touch every table, query and index.
- **Separate machines for dev, qa and uat.** The most isolated, but it costs three times as much for little gain over separate databases.
- **Environments as namespaces in one deployment now.** The cheapest, but without full multi-tenancy one test environment could exhaust another's workers or vendor budget.

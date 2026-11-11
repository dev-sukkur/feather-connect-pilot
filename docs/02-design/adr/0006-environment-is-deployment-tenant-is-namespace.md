# ADR-0006: An environment is a deployment; a tenant is a namespace

- **Status:** Accepted. Its tenant half is amended by [ADR-0013](0013-light-tenancy-and-non-prod-layout.md): v1 keeps one tenant, and full multi-tenancy comes after v1
- **Date:** 2026-10-30
- **Deciders:** OSS Dev1 (sponsor)

## Context

dev, qa, uat and prod are required. Multi-tenancy is wanted if it is cheap to add. A single control plane that manages every environment would let dev and prod share fate.

## Decision

**Environments:**
- Each environment is a separate deployment from the same images, with its own Postgres database, secrets, workers and engine.
- Promotion is by git tag and CI.
- The dashboard's fleet view reads summaries from peer environments. Actions only ever happen in the environment's own dashboard.

**Tenants:**
- A tenant is a namespace inside an environment: `tenant_id` on every row, enforced by Postgres row-level security and by the API.
- Each tenant has its own connections, secrets, budgets, quotas, RBAC, and optionally its own worker pools.
- Single-tenant installs use an implicit `default` tenant.

## Consequences

**Benefits:**
- Strong isolation between environments: prod can't be broken from dev.
- A simple mental model.
- Tenancy costs little to add.

**Costs:**
- Cross-environment views need peer federation.
- Row-level security needs care in every query and migration, which cross-tenant tests check.

## Alternatives considered

- **One control plane managing all environments.** Convenient promote buttons, but shared fate.
- **No tenancy in v1.** Simpler at first, but more expensive to retrofit later.

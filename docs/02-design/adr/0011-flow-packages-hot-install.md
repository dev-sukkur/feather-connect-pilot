# ADR-0011: Flows ship as packages, installed and activated in a running environment

- **Status:** Accepted on 2026-11-08. The sponsor answered all five questions on 2026-11-08: bring it forward, use the built-in registry, let runs finish, use the package prefix, and deploy with packages only
- **Date:** 2026-11-08
- **Deciders:** OSS Dev1 (sponsor)

## Context

In M1 an environment runs one project, which the server and every worker load when they start. Adding, changing or removing a flow means restarting all of them, all flows ship together, and a flow can be paused but not taken out of service. Customers want to release flows from their own repositories, separately from Feather Connect, into a running environment, and switch each flow on and off.

## Decision

- **A flow package** is the unit of release: a Python wheel holding the M1 project layout, a manifest and locked dependencies, built by `feather package build` in the customer's own repository.
- **Packages come from a registry built into `feather-server`**, a standard Python package index stored in Postgres and on by default. Customers' CI publishes to it with a publish-only token. A company's own Python package index can be added beside it.
- **Install** stores the package in Postgres after checks (integrity, compatibility, and a load check in a throwaway process). Installed flows start inactive. Every flow, dataset and route name carries the package prefix (`acme-invoices/invoices`).
- **Each flow is activated and deactivated per environment**, from the dashboard, CLI or API, with an audit row. Activating another version switches it, and the old version drains. Deactivating lets running runs finish.
- **Workers become supervisors.** They run one child process, a package host, per active package version, in that package's own virtual environment. Hosts start and stop on activation changes, so no container restarts.
- **The server works from flow specs** in the catalogue and stops importing flow code.
- Shared connections and budgets live at environment level.
- **Packages are the only way to deploy.** The M1 project mode is retired. Upgrading turns a running project into its first package, with history and watermarks kept. `feather dev` still runs a project folder for local work.

Details: [the flow packages design](../2026-11-08-flow-packages.md).

## Consequences

**Benefits:**

- New and changed flows go live with no restart.
- Each package is versioned, released, rolled back and owned on its own.
- Dependency conflicts and crashes stay inside one package.
- Promotion from qa to prod uses the same tested wheel, matched by sha256.

**Costs:**

- About 60–90 MB of memory per active package version on each worker replica.
- More moving parts: the supervisor, environment builds, the built-in registry, and six new tables.
- Feather now hosts customers' release files, so its database grows with every published version (about 50 KB to a few MB each), and a yank and retention policy is needed.
- Every M1 deployment must upgrade: names gain the package prefix, and the `/project` mount goes away.
- Package code runs unsandboxed in the worker container, so installing needs the admin role. Two roles, admin and operator, are pulled forward from M2's RBAC.
- A package can't bring its own webhook source connector at first.

## Alternatives considered

- **Restart on change (M1).** Simple, but every change restarts everything, and flows can't be released or switched off on their own.
- **Keep the project mode beside packages.** Lighter for small teams, but it means two code paths in the server and workers, two ways to release, and a choice for every environment.
- **Reload modules inside one worker process.** Python can't reliably unload code, and two packages can need different versions of one library, so changes would leak and conflict.
- **Pip-install flow packages into the worker image as entry points.** This keeps packages separate, but still needs a restart and puts every package in one dependency set.
- **A container per package, through the Docker or Kubernetes API.** This gives strong isolation, but needs orchestrator access and much more setup. Package hosts can move to containers later without changing the package format.
- **A custom archive format of our own.** It would need its own upload and storage, and couldn't use the registries and the dependency resolution that Python already has.

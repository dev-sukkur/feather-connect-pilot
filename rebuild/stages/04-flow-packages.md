=== proposal.md ===
# Proposal

## Why

Flows must be released one at a time into a running environment, and switched on and off, with no restart. The
pilot added this after everything else and had to rework its server, workers, names and configuration. Here it is
the only way flows have ever been deployed.

## What Changes

- Flow packages: build, the built-in registry, install checks, promotion by checksum, signing, uninstall.
- Workers as supervisors with one host process per active package version; prepare, activate, switch, deactivate.
- The environment's own connections, budgets and settings in the database; encrypted, write-only secrets.

Out of scope: any way to run a flow on a server other than from an installed package. There is no project mode and
no upgrade path from one.

## Capabilities

### New Capabilities
- `flow-packages`: build, registry, install, promotion, signing and uninstall
- `package-hosts`: supervisors, hosts, activation and isolation
- `environment-config`: layered configuration, connections and encrypted secrets

### Modified Capabilities

None.

## Impact

The server stops knowing any flow except through the catalogue. Workers gain a supervisor. `uv` becomes a runtime
dependency of workers.

**Exit criteria**

In a running Compose environment, with no container restarted:

- a package built in another repository is installed, one of its flows is activated, and it runs;
- it is upgraded to a new version, rolled back, and deactivated;
- another package keeps running throughout, with no run lost or doubled;
- a host killed with kill -9 during a switch loses and duplicates nothing.

=== design.md ===
# Design

## Context

The pilot's flow packages design (ADR-0011, and its design document with the "as built" notes) is the fullest and
most recent design in the pilot. Follow it. What it could not do, because of what already existed, is listed below.

## Goals / Non-Goals

**Goals:** packages as the only deployment path; the server never imports package code; a host gets only its own
package's secrets.

**Non-Goals:** a sandbox inside the worker; limiting a host's network (left to the deployment); several Python
versions.

## Decisions

- **No project mode, ever.** That removes the pilot's upgrade command, its rename table and its redirects.
- **Packaged datasets are fully operable.** Partition labels, verification, backfill and write cost are asked of
  the package's host, which has the code. The pilot shows keys only and offers none of these for packaged flows.
- **Hosts run with their own database role from the start**, not as an option with a warning.
- **`feather dev` uses the same path** as production: it installs the project as a development version. There is
  one way code reaches a worker.
- **Port first:** `feather-connect/tests/packages/`, `test_supervisor.py`, `test_host.py`, `test_environment.py`,
  `test_project.py`, `feather-server/tests/test_packages.py`, `test_registry.py`, `test_environment.py`,
  `feather-cli/tests/test_package.py`, `tests/e2e/test_package_hosts.py`, `tests/chaos/test_package_switch.py`.

## Risks / Trade-offs

- Installing a package runs its code on workers. Only admins install, only from trusted registries, with the
  checksum recorded; packages from parties that are not trusted belong in another environment.
- The pilot's chaos tests for hosts were among those not green at its end. Treat `package-hosts` scenarios about
  kills as unproved until they pass here.
- Each active package version costs about 60–90 MB per worker replica.

=== tasks.md ===
# Tasks

## 1. Packages

- [ ] 1.1 Port the build, check and index tests
- [ ] 1.2 The manifest schema, reproducible locked builds, `feather package build`
- [ ] 1.3 The built-in registry, publish tokens, yanking; other registries
- [ ] 1.4 The install checks, storage of wheels, the package commands
- [ ] 1.5 Promotion by checksum; signing; uninstall

## 2. Configuration and secrets

- [ ] 2.1 Port the project and environment tests
- [ ] 2.2 Project files, overlays, simulation swaps
- [ ] 2.3 Environment-level entries with audit
- [ ] 2.4 The vault, write-only secrets, key rotation

## 3. Hosts

- [ ] 3.1 Port the supervisor, host and e2e host tests
- [ ] 3.2 Dependency environments by lock hash; the load check in a child process
- [ ] 3.3 The supervisor: watch, prepare, start, drain, restart, crash loops
- [ ] 3.4 Activate, switch, roll back, deactivate; tasks follow their run's version
- [ ] 3.5 Host isolation: environment, credentials over a pipe, the database role
- [ ] 3.6 Labels, verify, backfill and cost for packaged datasets, through the host

## 4. Exit

- [ ] 4.1 The no-restart journey passes in Compose
- [ ] 4.2 The host chaos tests pass
- [ ] 4.3 `openspec validate 04-flow-packages --strict` passes; archive the change

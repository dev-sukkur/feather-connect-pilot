# ADR-0010: Docker Compose and `feather dev` as the v1 deployment targets

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor)

## Context

The system must be easy to run and maintain. Kubernetes isn't required by the sponsor or by the reference organisation.

## Decision

- **`feather dev`** runs a full local stack with one command.
- **Docker Compose** runs a full environment: Postgres, server and workers.
- The same images run on any VM or container platform.
- The on-prem agent installs with pip or as a container.
- A Helm chart follows after v1.0.
- **The CLI is named `feather`.** The shorter `fc` is a bash/zsh builtin and would be shadowed.

## Consequences

**Benefits:** simple to run, document and support.

**Costs:** Kubernetes users assemble their own manifests until the Helm chart exists.

## Alternatives considered

- **A Helm chart from day one.** More scope in v1.
- **One managed platform only.** Less portable.

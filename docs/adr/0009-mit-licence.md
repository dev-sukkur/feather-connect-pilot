# ADR-0009: MIT licence

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor)

## Context

Feather Connect is open source and aims for broad adoption by companies and contributors.

## Decision

All Feather Connect packages are MIT-licensed. The dependency policy:
- Distributed packages must not depend on copyleft libraries (GPL, AGPL, SSPL).
- Proprietary drivers, such as SAP's HANA client and NetWeaver RFC SDK, are only optional extras that the user installs.
- Separate services reached over the network, such as an S3-compatible server or the engine container, are outside this policy.

## Consequences

**Benefits:** maximum adoption, and a simple licence.

**Costs:**
- No explicit patent grant (Apache-2.0 has one).
- Others may build closed forks or hosted offerings.

## Alternatives considered

- **Apache-2.0.** Includes a patent grant.
- **AGPL core.** Protects against hosted forks, but deters enterprises.

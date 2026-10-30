# ADR-0007: Full-code flows; no visual designer in v1

- **Status:** Accepted
- **Date:** 2026-10-30
- **Deciders:** Abdul Sukkur (sponsor)

## Context

The sponsor chose a full-code approach so that AI and developers can both generate, run and monitor flows. The reference evaluation defined what "juniors can edit" means: turning knobs, not changing logic.

## Decision

- Flows are Python modules.
- Structural changes (datasets, keys, routes, transforms) go through pull requests.
- Operational knobs (schedules, pauses, budgets, concurrency, windows) live in layered YAML config. They can also be changed at runtime through audited overrides with a reason and an optional expiry.
- Templates provide starting points.
- There is no visual designer and no in-browser code editing in v1.

## Consequences

**Benefits:** flows are reviewable, testable and diffable, and AI can generate them.

**Costs:** non-developers can't build new flows alone. Templates and AI assistance mitigate this.

## Alternatives considered

- **A visual designer** (iPaaS or n8n style).
- **In-browser code editing** (Windmill style).

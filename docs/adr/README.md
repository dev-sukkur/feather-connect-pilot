# Architecture decision records

Each ADR records one decision with lasting consequences: its context, the decision itself, its consequences, and the alternatives considered.

**Status flow:** Proposed → Accepted → (Superseded by ADR-NNNN). Accepted ADRs are never edited in substance. A changed decision gets a new ADR that supersedes the old one.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-python-throughout-html-dashboard.md) | Python throughout; server-rendered HTML dashboard | Accepted |
| [0002](0002-own-control-plane-engine-behind-port.md) | Our own control plane and dashboard; engine behind a port | Accepted |
| [0003](0003-durable-engine-selection-by-spike.md) | Durable engine: DBOS Transact, chosen by a conformance spike | Accepted |
| [0004](0004-unified-data-model.md) | One data model for ETL and events | Proposed |
| [0005](0005-postgres-only-required-database.md) | Postgres as the only required database; object storage optional; no Kafka in v1 | Accepted |
| [0006](0006-environment-is-deployment-tenant-is-namespace.md) | An environment is a deployment; a tenant is a namespace | Accepted |
| [0007](0007-full-code-flows-no-visual-designer.md) | Full-code flows; no visual designer in v1 | Accepted |
| [0008](0008-ai-operability-via-mcp.md) | AI operability through MCP, with human-equivalent permissions | Accepted |
| [0009](0009-mit-licence.md) | MIT licence | Accepted |
| [0010](0010-compose-and-feather-dev-deployment.md) | Docker Compose and `feather dev` as the v1 deployment targets | Accepted |
| [0011](0011-flow-packages-hot-install.md) | Flows ship as packages, installed and activated in a running environment | Accepted |

## Template

```markdown
# ADR-NNNN: <decision in one line>

- **Status:** Proposed | Accepted | Superseded by ADR-NNNN
- **Date:** YYYY-MM-DD
- **Deciders:** <names>

## Context
## Decision
## Consequences
## Alternatives considered
```

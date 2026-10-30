# Feather Connect — project documentation

The project follows an SDLC flow. Each phase ends at a review gate, and the next phase starts only after the gate is passed.

| Phase | Document | Status | Gate |
|---|---|---|---|
| 1. Discovery | [Discovery document](01-discovery/2026-10-30-feather-connect-discovery.md) | Draft v0.2, awaiting review | Sponsor approves goals, scope, requirements and decisions |
| 2. Design | [Design specification](02-design/2026-10-30-feather-connect-design.md) | Draft v0.1, awaiting review | Sponsor approves the architecture |
| — | [Architecture decision records](adr/README.md) | 8 accepted, 2 proposed | Each proposed ADR is accepted or replaced |
| 3. Spikes | `03-spikes/` (to be written) | Not started | Engine chosen (ADR-0003); connector findings reviewed |
| 4. Plan | `04-plan/` (to be written) | Not started | Implementation plan approved |
| 5. Build | Code: M1, M2, M3 | Not started | Milestone exit criteria (design §19) |
| 6. Test and release | `06-release/` (to be written) | Not started | Release checklist; go/no-go |
| 7. Operate | `07-operate/` runbooks (to be written) | Not started | — |

## How to review

1. Read discovery §1–§3 (summary, problem, goals) and §11 (decisions). Then check the scope in §5 and the requirements in §7.
2. Read design §3 (architecture), §4 (data model) and §5 (what a flow looks like). Then check whatever matters most to you: dashboard §10, environments §11, tenancy §12, security §13, AI §14, delivery plan §19.
3. Answer what you can of the open questions: discovery §15 and design §20.
4. Mark comments inline, or list the changes you want. Both documents are revised before the gate.

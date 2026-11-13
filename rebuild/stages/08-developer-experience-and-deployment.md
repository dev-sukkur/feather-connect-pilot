=== proposal.md ===
# Proposal

## Why

The product is only useful if it starts with one command on a laptop and runs from two images in production. This
stage closes the rebuild: it brings the parts together and proves everything the pilot proved at its two exits.

## What Changes

- `feather dev`, `new`, `validate`, `run --local`, `test`, `describe`.
- The server and worker images, the Compose file, settings, migrations, upgrades.
- The quality gates in CI.

Out of scope: Helm, Kubernetes, a docs site (backlog).

## Capabilities

### New Capabilities
- `local-dev`: the developer's loop on one machine
- `deployment`: images, Compose, settings, migrations and quality gates

### Modified Capabilities

None.

## Impact

Adds `deploy/`, the demo project, and the nightly CI jobs.

**Exit criteria**

The pilot's M1 and M1.1 exit criteria, all green in CI on the same commit:

- the demo fans out to Postgres, DuckDB and HTTP;
- webhook to HTTP, 95th percentile at most 5 s;
- re-run and replay work from the dashboard;
- kill -9 mid-apply loses and duplicates nothing;
- `feather dev` shows the demo within 5 minutes;
- hard cases H1–H4, H6, H7, H8 and H14 pass;
- a package is installed, activated, switched, rolled back and deactivated with no restart;
- Compose starts healthy with at most four long-running containers, and no image holds a secret.

=== design.md ===
# Design

## Context

The pilot met every M1 criterion with measured numbers, then merged its last M1.1 pull request before CI ended and
left 14 failures and 33 errors in its last full run. This stage's gate is the rule that would have prevented that.

## Goals / Non-Goals

**Goals:** one commit on which every suite is green; a first-run experience under 5 minutes.

**Non-Goals:** macOS in CI is desirable and not required for the gate.

## Decisions

- **No merge while a required suite is red or running.** Enforced by branch protection, not by habit.
- **`feather dev` is production's path in miniature:** Postgres in Docker, the server, a supervisor, the project
  installed as a development version. No second way for code to load.
- **The server binds to loopback by default; images carry no secret or demo token.** Both were open findings in
  the pilot.
- **Helpers die with `feather dev` on every operating system**, macOS included, where the pilot leaked them.
- **Port first:** `feather-cli/tests/`, `tests/dev/`, `tests/deploy/`, `tests/demo/test_quick_start.py`.

## Risks / Trade-offs

- The pilot's deploy test was not run after its Compose file changed. Expect to repair it while porting.
- Docker-based suites are slow (the pilot's full run took about 34 minutes). Keep them out of the inner loop and in
  the gate.

=== tasks.md ===
# Tasks

## 1. Developer commands

- [ ] 1.1 Port the CLI tests
- [ ] 1.2 `new project`, `new flow`, `new connector`, with `AGENTS.md`
- [ ] 1.3 `validate`, `describe`, `test`
- [ ] 1.4 `run --local --sim`, signed webhook sources included

## 2. `feather dev`

- [ ] 2.1 Port the dev tests
- [ ] 2.2 Start-up, clear failures, the development user
- [ ] 2.3 Reload on save through development versions
- [ ] 2.4 Helper processes that die with their parent on Linux, Windows and macOS

## 3. Deployment

- [ ] 3.1 Port and repair the deploy tests
- [ ] 3.2 Images for amd64 and arm64, with no secret inside
- [ ] 3.3 Compose; settings checked at start-up; loopback by default
- [ ] 3.4 A rolling-upgrade test

## 4. Gate

- [ ] 4.1 CI jobs: check, postgres, dashboard, engine conformance, deploy; nightly chaos, hard cases, performance
- [ ] 4.2 Branch protection: required suites must be green
- [ ] 4.3 Every exit criterion green on one commit, with measured numbers written into an exit report
- [ ] 4.4 `openspec validate 08-developer-experience-and-deployment --strict` passes; archive the change

=== proposal.md ===
# Proposal

## Why

Operators recover failures from a dashboard, not a terminal. The pilot built its pages, then redesigned all of them
in a new layout a milestone later. Here the dashboard is built once, in that final layout, over a service layer
that is already complete.

## What Changes

- The app shell: sidebar, command palette, themes, status bar.
- One list pattern for every list page.
- Every page: overview, flows, partition grid, runs, deliveries and failures, versions, quarantine, trace,
  connections and secrets, packages and registries, users, status.
- Every operator action, with preview, reason and confirm.

Out of scope: pages for features in the backlog (holds, budgets, alerts, audit, fleet).

## Capabilities

### New Capabilities
- `dashboard`: the shell, the list pattern, the pages, safe actions and accessibility

### Modified Capabilities

None.

## Impact

Adds templates, one stylesheet and plain JavaScript to the server. No new dependency, and no build step.

**Exit criteria**

- Automated accessibility checks report no violation on any page, light and dark.
- Re-run, replay and retry journeys pass in a real browser, by mouse and by keyboard alone.
- Read-only pages work with JavaScript off; pages fit 360 px.
- No console error and no content-security-policy violation on any page.
- A recorded walk from the overview to a confirmed retry takes at most 5 clicks.

=== design.md ===
# Design

## Context

Sponsor decisions carried from the pilot: HTML only, with no framework, TypeScript or Node (ADR-0001); the layout
of a container list, with no other product's branding; plain HTML and JavaScript, with a richer framework only if
needed later.

## Goals / Non-Goals

**Goals:** a junior operator finds, understands and recovers a failure without help.

**Non-Goals:** editing flows in the browser; showing transforms as code.

## Decisions

- **Pages are adapters.** A page calls the service layer and renders; it holds no rule of its own. Anything a page
  can do, the API can do.
- **Links never carry an escaped slash.** Packaged names are two path segments (plan §5.2 R1).
- **Partition labels everywhere**, packaged datasets included, through the host (stage 4).
- **Measure the five clicks.** The pilot tested the journeys and never counted.
- **Port first:** `feather-server/tests/test_web_*.py`, `tests/dashboard/` (journeys, shell, accessibility), and
  the pilot's templates and stylesheet as the visual reference.

## Risks / Trade-offs

- A hand-written front end can drift into a framework by accretion. The pilot's whole script is under 600 lines;
  treat growth past that as a signal to stop and reconsider.
- Browser tests are slow. Run them on every pull request anyway: this is the surface operators use.

=== tasks.md ===
# Tasks

## 1. Shell and pattern

- [ ] 1.1 Port the shell tests and the accessibility test
- [ ] 1.2 The shell: sidebar, palette, themes, status bar
- [ ] 1.3 The list pattern: tiles, toolbar, table, row and bulk actions, footer

## 2. Pages

- [ ] 2.1 Port the page tests
- [ ] 2.2 Overview, flows, flow detail with its graph
- [ ] 2.3 Partition grid, runs, run detail with tasks, logs and records
- [ ] 2.4 Deliveries, failures, delivery and version detail, quarantine, trace
- [ ] 2.5 Connections, secrets, packages, registries, users, status

## 3. Actions

- [ ] 3.1 Port the operator journeys
- [ ] 3.2 Preview, reason, confirm for every mass action
- [ ] 3.3 Live refresh of the parts that change

## 4. Exit

- [ ] 4.1 Accessibility: no violation, light and dark
- [ ] 4.2 Journeys by mouse and by keyboard; no JavaScript; 360 px
- [ ] 4.3 The five-click walk, recorded
- [ ] 4.4 `openspec validate 06-dashboard --strict` passes; archive the change

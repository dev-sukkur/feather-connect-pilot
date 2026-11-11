# Is the bare-repository approach good for the rebuild?

**Date:** 11 Nov 2026. **Status:** assessment, awaiting review. **Follows:**
[the spec extraction and rebuild plan](2026-11-11-spec-extraction-and-rebuild-plan.md).

**Answer: yes, with five rulings settled first and one trial change before committing to it.** The approach fixes
the pilot's two real weaknesses. Used exactly as the template is written, it would also make the rebuild far slower
than it needs to be, and it does not fit the staged seed in `rebuild/`.

## 1. What "the bare-repository approach" is

The new repository (`feather-connect`, public) holds a process and nothing else: no code, no specs, no changes. It
was generated from the template `intent-driven-with-human-control` (v1.1.0, Python acceptance stack). Building the
product there means:

| Rule | What it means in practice |
|---|---|
| Every piece of work starts from a GitHub issue | One issue, one worktree, one branch, one PR, from proposal to archive |
| Schema `intent-driven` | Proposal, specs, design, ADR, tasks, in that order, each reviewed and committed on its own |
| Specs are the source | Scenarios are fenced Gherkin; the acceptance runner (behave) extracts and runs them against the running app |
| Code only for an active change | No code without a scenario that asks for it; specs and code are never committed together |
| Three modes | Autopilot (subagents build), build (default: the human predicts each test), learning (every command explained) |
| Reconcile at the end of each change | 100% line coverage, no unused code, types, mutation testing |
| Specs written for a product person | No code, table, endpoint or file names; no status or ID labels; one or two sentences per requirement |

What was checked: every file on `main`, the unmerged setup branch, and the six process skills that set these rules.
What was not: nothing was run there. No acceptance project exists yet to run.

## 2. What it gets right

| The pilot's weakness | How this approach answers it |
|---|---|
| Specs, docs and tests were three separate things. This extraction could only call a requirement "verified" by matching test names | The scenario is the test. A spec that is not true fails the suite |
| The last M1.1 PR merged before CI ended, leaving 14 failures and 33 errors | A green effective suite is the gate for archive, and archive is the last commit before merge |
| Decisions were settled while building (twelve rulings in Stage 4) and written down afterwards | Design decisions and ADRs are approved before tasks exist. An accepted ADR is never edited |
| History carries discovery notes that were not written for publication | A new history, under an explicit "open-source content only" rule |

The last row alone justifies a new repository over publishing the pilot.

## 3. Where it does not fit as written

### 3.1 The seed in `rebuild/` cannot be loaded

| The seed | The new repository |
|---|---|
| Schema `spec-driven` | Schema `intent-driven`, with an ADR artifact |
| Scenarios as WHEN/THEN bullets | Fenced Gherkin, executed |
| 240 technical requirements | Product-level promises only |
| A `*Pilot: status*` line per requirement | No status or legacy labels |
| Eight large changes | One small change per issue |
| Writes a new `openspec/` | `openspec/` already exists; `seed.py` refuses |

Loading all 22 capabilities as open changes would also break the runner: it executes every active change's
scenarios, so the suite would be red for months. The seed stays useful as the reference to write from. It should
not be copied in.

### 3.2 The build order is cut the wrong way

The eight stages are layers, bottom up: core model, durable runtime, control plane, and so on. Outside-in work
needs each change to end in something a person can do against a running app. Stage 1 (the core model) has no such
thing, so it has no scenario and, by the rules, no code.

The order has to be re-cut as vertical slices. The first is a walking skeleton: one flow, from a simulated source
to a Postgres table, started from the command line and visible on the dashboard. It touches every layer thinly.
Later slices deepen one promise each (changed records only, a second sink, retry and replay, a webhook source,
packages, tenancy).

The capabilities and requirements still apply. Only their grouping into changes changes.

### 3.3 Speed

The pilot has about 1,800 tests. In build mode the human predicts the result of each test, and every red run is a
commit. A rebuild at that pace means stops in the thousands. Build mode is designed for learning how code comes to
be, which is not the goal of rebuilding a product whose behaviour is already known.

Autopilot removes the stops and keeps the test-first order. It should be the default here, with build mode kept for
the few changes where a wrong step is costly (exactly-once delivery, the engine port, access control).

### 3.4 "Written for a product person" on a developer platform

Most of the pilot's requirements are contracts: the store, the engine port, checkpoints, the connector SDK. The
spec rules ban the words those contracts are made of. Two things follow:

- **"The user" needs a definition.** Here it is three people: the flow author (whose interface is Python and the
  command line), the operator (the dashboard) and the administrator. A command name or an SDK call is that user's
  surface, not an implementation detail, and the rule should say so.
- **Contracts move, they do not vanish.** The promise goes in the spec ("a worker killed mid-delivery loses nothing
  and duplicates nothing"). The contract behind it goes in `design.md`, an ADR and the inner tests.

### 3.5 Checks the process has no place for

| Pilot check | In the template |
|---|---|
| Chaos tests (kill -9 mid-apply) | Expressible as a scenario, but slow; no nightly tier |
| Performance (95th percentile at most 5 s) | No place |
| Engine conformance kit, connector conformance kit | Inner tests; no named place |
| 100% line coverage and mutation testing | Required on every change; costly on Docker-backed tests |

Mutation testing over tests that need Postgres in a container will be slow. Whether it runs on every change, or
only on the pure-Python core, needs deciding before the first reconcile.

### 3.6 Reusing the pilot

The plan said "port the tests first". The template says no code without a failing scenario, and the minimal code to
pass it. Read strictly, that forbids bringing over a pilot module or its test file whole.

A workable reading: pilot code and tests are a source to copy from, one piece at a time, when a failing test asks
for that piece. This keeps the rule and still saves most of the writing.

### 3.7 Tooling

- The template is written for JavaScript and OpenCode. The Python acceptance pack (behave) is included. The Python
  equivalents for coverage, unused code, types and mutation exist only on the unmerged setup branch.
- On Windows the skill links check out as text files, so the process skills do not load in the main checkout. The
  fix is also on the unmerged branch.
- That branch should merge before anything else.

### 3.8 The process against the sponsor's working preferences

| Preference in the pilot | The template |
|---|---|
| Work in the main session, no subagents | Autopilot builds with subagents; the UI verifier and the spec reviewer are subagents |
| Fewer PRs, stages grouped | One PR per change |
| Complete documents in one pass, reviewed at the end | One decision per question on every artifact |

The new repository belongs to another owner and the template is theirs. Its rules should win there, but the
sponsor should agree to the change in working style knowingly.

## 4. Options

| | Option | For | Against |
|---|---|---|---|
| A | Bare repository, template exactly as written, build mode | Full traceability of every step | Thousands of stops; no place for chaos, performance or conformance checks |
| **B** | **Bare repository, template with the five rulings in §5, autopilot by default, vertical slices** | Keeps executable specs and the gate; realistic pace; pilot work reused | Needs the rulings agreed with the template's owner |
| C | Load the staged seed (`spec-driven`) into a new repository | Ready today | Ignores the process already chosen there; specs stay unexecuted, the pilot's weakness |
| D | Publish the pilot and keep going | No rebuild | History not written for publication; keeps the scaffolding the plan wanted gone |

**Recommended: B.**

## 5. Rulings needed before the first change

| # | Question | Recommendation |
|---|---|---|
| 1 | Who is "the user" in a spec? | Flow author, operator, administrator. Command names and SDK calls are allowed as that user's surface; tables, endpoints and files stay banned |
| 2 | Which mode is the default? | Autopilot. Build mode for changes rated red or touching delivery, the engine port, secrets or access |
| 3 | May pilot code and tests be copied in? | Yes, the smallest piece a failing test asks for, never a whole package ahead of need |
| 4 | Where do chaos, performance and conformance checks live? | A nightly suite outside the per-change gate; any failure files a red issue. Mutation testing per change on pure-Python code only |
| 5 | How do the pilot's 13 ADRs arrive? | One foundation change imports them into `adr/` as accepted, after the owner reads each; R1–R7 from the plan are decided in the same change |

## 6. Trial before committing

One change, built end to end under option B, then a decision.

**The change:** the walking skeleton in §3.2.

**What to record:**

| Measure | Why |
|---|---|
| Hours from issue to merge | The pace the other slices can expect |
| Human stops, by kind | Whether autopilot leaves the right decisions with the human |
| Whether each promise could be written under the spec rules | Tests ruling 1 |
| Acceptance suite time, with Postgres in a container | Whether the suite can stay in the inner loop |
| Reconcile time (coverage, unused code, types, mutation) | Tests ruling 4 |
| Lines copied from the pilot against lines written new | Tests ruling 3 |

**Then:** if the trial holds, cut the remaining capabilities into slices as issues, blocked-by links in build
order. If the spec rules or the reconcile cost fail the trial, change the rulings before going further, not after
ten changes.

On this machine the trial's Postgres uses port 25432; 5432 and 55432 are taken.

## 7. What happens to the work already done

| Artifact | Use from here |
|---|---|
| `openspec/specs/` (22 capabilities) | The reference each slice's Gherkin is written from. Stays in this repository |
| `openspec/traceability.md` | Points each promise to the pilot code and tests to copy from. Stays here; it names pilot files |
| `rebuild/stages/` | Design notes and exit criteria, re-cut into slices. The layer order no longer applies |
| `rebuild/seed.py` | Not used under option B. Keep it until the trial decides |
| The plan's decisions R1–R7 and the backlog | Go into the foundation change and into green issues |

## 8. Decisions for the sponsor

1. Option B, or another.
2. The five rulings in §5, with the template's owner.
3. Go ahead with the trial change in §6.

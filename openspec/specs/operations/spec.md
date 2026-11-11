# operations Specification

## Purpose

The actions an operator takes on running flows, and the rules every surface shares when taking them: one service
layer behind the dashboard, the JSON API and the command line, previews before mass actions, reasons, and safe
retries of requests.

## Requirements

### Requirement: One service layer, several adapters
Every operation SHALL live in one service layer. The HTML pages, the JSON API and the remote commands of the CLI
SHALL be thin adapters over it, so that validation, role checks, previews and audit are the same for people and
scripts.

*Pilot: verified*

#### Scenario: The same action, three ways
- **WHEN** a partition is re-run from the dashboard, from the API and from the CLI
- **THEN** each writes the same kind of run and the same kind of audit record

### Requirement: Run, re-run, backfill and cancel
An operator SHALL be able to start a run of a flow now, re-run chosen partitions of a dataset, backfill a flow over
a range of dates, and cancel a run. A backfill SHALL plan the partitions the range covers in the flow's time zone.

*Pilot: verified*

#### Scenario: Re-run a business slice
- **WHEN** an operator re-runs "store 1509, 18 October" of a dataset
- **THEN** a run reads that partition alone and its changes are delivered to every route

#### Scenario: A backfill of a week
- **WHEN** an operator backfills a daily-partitioned flow from 1 to 7 October
- **THEN** the preview names seven partitions, and confirming it starts a run for them

### Requirement: Mass actions are previewed, then confirmed
Re-run, backfill, and retry, skip and replay of deliveries SHALL be two steps. A preview SHALL return the impact
(partitions and deliveries touched, estimated budget units when known, and samples) and a confirmation token, and
the action SHALL run only with that token. A token SHALL be signed, SHALL expire after 10 minutes, SHALL work once,
and SHALL be refused as stale if what the action would touch has changed since the preview.

*Pilot: verified*

#### Scenario: A preview goes stale
- **WHEN** an operator previews a retry of 12 failed deliveries, and one of them is retried by someone else before the confirm
- **THEN** the confirm is refused as stale and asks for a new preview

#### Scenario: A token used twice
- **WHEN** two requests present the same confirmation token at the same moment
- **THEN** one acts and the other is refused

#### Scenario: A selection that is too wide
- **WHEN** a delivery selection matches more deliveries than the limit for one action
- **THEN** the preview is refused and asks for a narrower selection

### Requirement: Selecting deliveries
A delivery action SHALL select by any of route, dataset, partition, error class, error message and version ids,
each narrowing the selection. A partial outcome SHALL be reported as such: deliveries the runtime refuses to move
are left as they are and named in the audit record, and the others go ahead.

*Pilot: verified*

#### Scenario: Retry one failure group
- **WHEN** an operator retries every delivery of one route that failed with the same error
- **THEN** exactly those deliveries return to pending

### Requirement: Reasons
Every state-changing action SHALL accept a reason, which is written to the audit record. In an environment marked
protected, skipping deliveries SHALL require one.

*Pilot: partial*

#### Scenario: A skip with no reason in production
- **WHEN** an operator confirms a skip with an empty reason in a protected environment
- **THEN** the action is refused and says a reason is required

### Requirement: Verify a partition
An operator SHALL be able to verify a partition: the system reads it as a run would, compares it with the last
sealed version, reports how many upserts and deletes differ, and stores nothing.

*Pilot: partial*

#### Scenario: Nothing differs
- **WHEN** a partition is verified and the source matches what was last sealed
- **THEN** the result is 0 upserts and 0 deletes, and no version, quarantine entry or state is written

### Requirement: Test a connection
An operator SHALL be able to test a connection, which calls the connector's check and reports whether it works and,
if not, why, within a time limit and with secrets redacted.

*Pilot: verified*

#### Scenario: A check that hangs
- **WHEN** a connector's check never returns
- **THEN** the action answers after its time limit that the check timed out

### Requirement: JSON API conventions
The JSON API SHALL be served under `/api/v1` and described by an OpenAPI document. Errors SHALL be RFC 9457
`application/problem+json`: 403 for want of a role, 409 for a conflict with the current state, 422 for a request
that can't be understood. Lists SHALL page by an opaque cursor with a limit from 1 to 500. Actions SHALL be `POST`
to a resource with a verb after a colon, such as `/flows/{flow}:run`, with `:preview` for each mass action.

*Pilot: verified*

#### Scenario: An error
- **WHEN** a call names a flow that doesn't exist
- **THEN** the answer is 404 with a `problem+json` body holding a title, a status and a detail

#### Scenario: Paging
- **WHEN** a list has more rows than the limit
- **THEN** the answer carries a cursor, and calling again with it returns the next rows with none repeated

### Requirement: API resources
The API SHALL offer reads of flows, datasets, routes, runs with their tasks, logs and records, partitions, versions,
deliveries, record traces, budgets, connections, configuration, secrets' names, users, packages, registries and a
summary. It SHALL offer the actions run, backfill, pause, resume, activate and deactivate on a flow; pause and
resume on a route; cancel on a run; re-run on partitions; retry, skip and replay on deliveries; and check on a
connection.

*Pilot: verified*

#### Scenario: Reading a run
- **WHEN** `GET /api/v1/runs/{id}` is called
- **THEN** the answer gives the run's flow, trigger, actor, status, reason and times

### Requirement: Requests can be retried safely
A state-changing call SHALL accept an `Idempotency-Key` header of bounded length. A repeat of a key by the same
actor for the same request SHALL return the first answer and SHALL NOT act again, even when both requests arrive at
the same instant. A key reused for a different request SHALL be answered 422. Requests that carry a secret's value
SHALL NOT be kept.

*Pilot: corrected*

#### Scenario: A client retries after a timeout
- **WHEN** a client sends `POST /flows/orders:run` twice with one key
- **THEN** one run is started, and both answers name it

#### Scenario: Two requests at once
- **WHEN** two identical requests with one key arrive at the same moment
- **THEN** one run is started

### Requirement: Remote commands
The CLI SHALL offer `status`, `runs`, `logs`, `run`, `backfill` and `replay` against a server, taking its address
and a token from options, the environment or the project's environment file. Commands that change many things SHALL
show the preview and ask before acting, SHALL act without asking with `--yes`, and SHALL only preview with
`--dry-run`. A token from an environment file SHALL be sent only to that file's own server.

*Pilot: verified*

#### Scenario: A dry run
- **WHEN** `feather backfill orders --from 2026-10-01 --to 2026-10-07 --dry-run --json` runs
- **THEN** it prints the preview as JSON and starts nothing

#### Scenario: Another server
- **WHEN** a command is given `--server` for a host that is not the environment file's
- **THEN** the environment file's token is not sent to it

### Requirement: Held events
An operator SHALL be able to reprocess a held push event, which gives it another try, or release it, which lets the
events behind it go on without it.

*Pilot: verified*

#### Scenario: A fixed parser
- **WHEN** an event was held because its parser failed, the package is fixed, and an operator reprocesses it
- **THEN** the event is sealed and delivered

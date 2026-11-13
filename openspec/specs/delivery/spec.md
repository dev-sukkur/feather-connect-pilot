# delivery Specification

## Purpose

How versions reach destinations: routes that progress independently, the ledger that is each route's cursor and its
dead-letter state, ordering, quarantine of single records, and replay of history.

## Requirements

### Requirement: Routes progress independently
One dataset SHALL be deliverable to any number of routes. Each route SHALL have its own ledger entries, and SHALL
progress, fail and recover without affecting the others.

*Pilot: verified*

#### Scenario: One destination is down
- **WHEN** a flow delivers to three routes and the destination of one is unreachable
- **THEN** the other two routes apply every version
- **AND** the third route's deliveries wait, and apply once the destination is back

### Requirement: Extraction never waits for delivery
Sealing a version SHALL write the version and one pending delivery per route in one transaction, and SHALL NOT call
any destination. A destination outage SHALL only grow that route's backlog.

*Pilot: verified*

#### Scenario: The warehouse is down during the read window
- **WHEN** a source's read window opens while a destination is down
- **THEN** the extract completes and its versions are sealed
- **AND** the deliveries catch up after the destination recovers, with nothing read again

### Requirement: Versions apply in seal order per partition
A route SHALL apply the versions of one (dataset, partition) one at a time, oldest unapplied first. Version n+1
SHALL NOT be applied until version n is applied, skipped or compensated. Within a version, records SHALL keep the
order they were received in.

*Pilot: verified*

#### Scenario: A failed version blocks its stream
- **WHEN** version 5 of a partition fails on a route and version 6 is sealed
- **THEN** version 6 stays pending on that route until version 5 is retried or skipped
- **AND** other partitions of the same route carry on

### Requirement: A delivery is applied at most once
A delivery (route, version) SHALL reach `applied` at most once. Each chunk given to a sink SHALL carry an idempotency
key derived from the route, the version and the chunk number, and sinks SHALL be idempotent by key, so that a crash
between the sink call and the ledger write cannot duplicate effects.

*Pilot: verified*

#### Scenario: A worker is killed mid-apply
- **WHEN** a worker is killed with kill -9 after a sink call and before the ledger is updated
- **THEN** the delivery is applied again from its checkpoint
- **AND** the destination holds each record once

#### Scenario: A worker is wrongly declared dead
- **WHEN** a live worker is declared dead and its apply task is started on another worker
- **THEN** the destination holds each record once

### Requirement: Delivery states
A delivery SHALL be in one of `pending`, `running`, `applied`, `failed`, `held`, `skipped` or `compensated`, and
SHALL move only along these transitions: pending → running; running → applied, pending, failed or held; held →
pending or skipped; failed → pending or skipped; applied → pending or compensated. `skipped` and `compensated` SHALL
be final. Any other move SHALL be refused.

*Pilot: verified*

#### Scenario: An illegal move
- **WHEN** something tries to move a skipped delivery to pending
- **THEN** the ledger refuses it and the delivery is unchanged

### Requirement: Every transition is recorded
Every transition of a delivery SHALL write an attempt record with the statuses before and after, the actor, the
reason, the error, the rows applied and rejected, the code version and the time.

*Pilot: verified*

#### Scenario: An operator skips a failed delivery
- **WHEN** an operator skips a failed delivery with a reason
- **THEN** the delivery's history shows failed → skipped, the operator's name, the reason and the time

### Requirement: Route modes
A route SHALL apply in a mode its sink declares. `upsert` (the default) inserts or updates by key, and a delete
removes the row. `append` adds every record with its key, operation, version, run, extraction time and partition, so
that history is kept. `replace_partition` atomically replaces the destination's slice, and SHALL be allowed only for
complete versions; a `snapshot_diff` dataset on such a route SHALL also store the whole slice.

*Pilot: verified*

#### Scenario: An append route keeps history
- **WHEN** a record is changed twice and delivered by an `append` route
- **THEN** the destination holds three rows for its key, each with its version and operation

#### Scenario: A partition is replaced
- **WHEN** a complete version of a partition is applied by a `replace_partition` route
- **THEN** the destination's slice holds exactly the version's records, and readers never see it half replaced

### Requirement: Sinks mark what they wrote
A sink that writes to a table SHALL add the columns `_fc_key`, `_fc_partition`, `_fc_version_id`, `_fc_op` and
`_fc_applied_at`, and SHALL keep a `_feather_applied` table in the destination that records the chunks applied.
Preparing these in a new schema SHALL be safe when two first deliveries arrive at once.

*Pilot: verified*

#### Scenario: Two first deliveries race
- **WHEN** two routes make their first delivery to a new schema at the same moment
- **THEN** both succeed

### Requirement: Errors are classified
A sink SHALL signal a failure by its type. A `TransientError` SHALL be retried with backoff, honouring a
`retry_after` it carries. A `PermanentError` SHALL fail the delivery without a retry. A `RecordRejected`, or a
rejection returned in the sink's result, SHALL quarantine that one record. Error text SHALL be redacted before it
is stored or shown.

*Pilot: verified*

#### Scenario: A rate limit with a wait
- **WHEN** a sink raises `TransientError` with `retry_after=20`
- **THEN** the delivery returns to pending and is tried again no sooner than 20 seconds later

#### Scenario: An authentication failure
- **WHEN** a sink raises `PermanentError`
- **THEN** the delivery is failed at once, and its error is shown without any secret value

### Requirement: A rejected record does not stop its version
A record that a sink rejects SHALL be quarantined with its key, error, payload and route, and the rest of the
version SHALL proceed. A quarantine entry SHALL be `open`, `resolved`, `superseded` or `dismissed`, and an operator
SHALL be able to dismiss entries.

*Pilot: verified*

#### Scenario: One bad record in a thousand
- **WHEN** a sink rejects 1 record of a 1,000-record version
- **THEN** the delivery is applied with 999 rows applied and 1 rejected
- **AND** the quarantine holds that record with the sink's reason

### Requirement: A newer version resolves an older rejection
For a dataset that keeps record state, when a later version of the same key applies on a route, an open quarantine
entry for that key on that route SHALL be marked `superseded`.

*Pilot: built*

#### Scenario: A fixed order arrives
- **WHEN** an order is rejected, and a corrected version of the same order is later applied
- **THEN** the quarantine entry for the first is superseded

### Requirement: Retry and skip
An operator SHALL be able to retry a failed or held delivery, which returns it to pending, and to skip one, which
closes it and lets the next version of its partition proceed. Both SHALL record the actor and the reason.

*Pilot: verified*

#### Scenario: A retry after a fix
- **WHEN** a delivery failed for a missing table, the table is created and an operator retries
- **THEN** the delivery applies, and the versions waiting behind it follow in order

### Requirement: Replay
Any retained version SHALL be replayable to a route. A replay with no version named SHALL send the latest version of
each stream as it is when the replay runs, not as it was at the preview. Replaying a version that is not the newest
of its partition SHALL be refused unless the newer ones were skipped, because it would put old data in the
destination. Replaying a version whose chunks are gone SHALL be refused with the reason, never reported as done.

*Pilot: verified*

#### Scenario: Replay from the partition grid
- **WHEN** an operator replays a partition with ten versions to a route
- **THEN** only the newest version is sent

#### Scenario: An old version
- **WHEN** an operator asks to replay version 3 while version 5 is applied
- **THEN** the replay is refused, with the reason

#### Scenario: Chunks past retention
- **WHEN** an operator asks to replay a version whose chunks retention has removed
- **THEN** the replay is refused, with the reason, and nothing is marked applied

### Requirement: Routes can be paused
An operator SHALL be able to pause and resume a single route. A paused route SHALL apply nothing and SHALL keep its
deliveries pending; extraction and the other routes SHALL carry on.

*Pilot: verified*

#### Scenario: A route is paused and resumed
- **WHEN** a route is paused, three versions are sealed, and the route is resumed
- **THEN** the three versions apply in order after the resume

### Requirement: Destination answers are kept
A sink SHALL be able to report, per record, the status the destination answered with and the fields a response map
took from the answer, and the body of a rejected record. The system SHALL keep these in a table with the delivery,
including those of a chunk that failed part-way. A sink SHALL never store a raw response body or headers.

*Pilot: verified*

#### Scenario: An API returns the id it created
- **WHEN** an HTTP sink with a response map delivers an order and the API answers with `{"id": 77}`
- **THEN** the delivery's answers hold status 201 and `id = 77` for that order's key

### Requirement: Pending deliveries are never orphaned
A pending delivery with no active task SHALL be submitted again by the sweeper. A delivery whose task has ended
SHALL NOT stay `running`.

*Pilot: verified*

#### Scenario: A worker dies between sealing and submitting
- **WHEN** a worker seals a version and dies before it submits the apply tasks
- **THEN** the sweeper submits them, and the version is applied

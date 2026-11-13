# flow-authoring Specification

## Purpose

The SDK a developer or an AI assistant writes flows with: datasets, flows, routes, triggers and transforms as plain
Python, with checks that find mistakes before anything runs and a harness that tests a flow with no infrastructure.

## Requirements

### Requirement: Flows are plain Python
A flow SHALL be an ordinary Python module that imports the SDK as `import feather_connect as fc`. There SHALL be no
proprietary DSL, XML or visual format. The public names SHALL be `Dataset`, `Flow`, `to`, `strategy`, `partition`,
the triggers `cron`, `every`, `manual` and `on_event`, `batch_map`, `Record`, `Context`, the connector types and the
errors `TransientError`, `PermanentError` and `RecordRejected`. The SDK SHALL be fully typed.

*Pilot: verified*

#### Scenario: A minimal flow
- **WHEN** a module defines `fc.Flow("orders", reads=fc.Dataset(...), deliver=[fc.to(sink)], triggers=[fc.manual()])`
- **THEN** the flow loads, validates and runs with no other file but its project configuration

### Requirement: A dataset declares its source, key, partitions and strategy
A `Dataset` SHALL be declared by a name, a source, a key of one or several fields, and a change strategy. It SHALL
accept a partition scheme, `keep_state`, a priority, a schema as a Pydantic model or an Arrow schema,
`on_record_error` (`quarantine` by default, or `fail`), `allow_empty`, and a planner that names the partitions to
read. Behaviour beyond these SHALL be added through Python hooks, not configuration flags.

*Pilot: verified*

#### Scenario: A scheme that lists its own partitions needs no planner
- **WHEN** a dataset is partitioned `by_day` and has no planner
- **THEN** a scheduled run reads today and yesterday in the scheme's time zone

#### Scenario: A scheme that can't list its partitions
- **WHEN** a pull dataset is partitioned by a plain field and has no planner
- **THEN** validation reports the error `planner_required`, with a hint showing a planner

### Requirement: A flow declares what it reads, where it delivers and what starts it
A `Flow` SHALL have a name, one or more datasets, one or more routes, and triggers. It SHALL accept an owner, tags,
an overlap policy (`queue` by default, `skip`, `cancel_previous`), a catch-up policy (`latest` by default, `each`)
and `enabled`. A flow SHALL be able to describe itself as JSON that names its datasets, routes, connections,
strategies, modes and triggers and holds no code and no secret.

*Pilot: verified*

#### Scenario: A flow's description
- **WHEN** a flow with one dataset, two routes and a cron trigger is asked for its description
- **THEN** the JSON names the dataset's connection, connector, key and strategy, each route's connection, connector and mode, and the trigger's expression and time zone
- **AND** it holds no function and no secret value

### Requirement: Routes are declared with `to`
`fc.to(sink, map=None, mode="upsert", priority="normal", name=None)` SHALL declare a route to a sink. A route SHALL
be named after the sink's connection unless a name is given.

*Pilot: verified*

#### Scenario: Two routes to one connection
- **WHEN** a flow delivers twice to the connection `warehouse` and names neither route
- **THEN** validation reports the error `route_name_duplicate`

### Requirement: Triggers
The SDK SHALL provide `cron(expr, tz=...)` with a required IANA time zone, `every(interval)` with intervals written
as whole numbers and units (`30s`, `15m`, `2h`, `7d`, `1h30m`), `manual()` and `on_event(dataset)`.

*Pilot: verified*

#### Scenario: A cron with an unknown time zone
- **WHEN** a flow declares `fc.cron("0 2 * * *", tz="Asia/Nowhere")`
- **THEN** the module fails to load with an error naming the time zone, before any tick

#### Scenario: An interval that isn't one
- **WHEN** a flow declares `fc.every("soon")`
- **THEN** loading fails with a message that shows the accepted form

#### Scenario: An event trigger on a pull dataset
- **WHEN** a flow declares `fc.on_event(d)` and `d` does not use the `push` strategy
- **THEN** validation reports the error `trigger_needs_push`

### Requirement: Record maps
A route's map SHALL be a function of a record and a context. Returning a dict SHALL deliver one payload, returning a
list of dicts SHALL split the record into several, and returning `None` SHALL filter it out. A route with no map
SHALL pass the payload on as it is. Any other return value SHALL fail the delivery as a permanent error that names
the route.

*Pilot: verified*

#### Scenario: A map filters a record
- **WHEN** a route's map returns `None` for a record
- **THEN** the sink does not receive that record and the delivery still counts as applied

#### Scenario: A map splits a record
- **WHEN** a route's map returns a list of three dicts
- **THEN** the sink receives three payloads for that record

### Requirement: Batch maps
A function marked with `@fc.batch_map` SHALL receive a whole batch as an Arrow table with the context and SHALL
return an Arrow table.

*Pilot: verified*

#### Scenario: A vectorised transform
- **WHEN** a route's map is a batch map that adds a column
- **THEN** every record the sink receives has that column

### Requirement: Transforms are pure
Transforms SHALL be deterministic functions of the record, the configuration and the context's time, which is fixed
for the whole run. Side effects belong in sinks. Each delivery attempt SHALL record the code version that ran it.

*Pilot: built*

#### Scenario: The time inside a run
- **WHEN** a map reads `ctx.now` for the first and the last record of a run
- **THEN** both calls return the same time

### Requirement: Static validation
`Flow.validate()` SHALL return every mistake that shows without running the flow, each as a problem with a severity
(`error` or `warning`), a code, a message, a path in terms of the flow's arguments and a hint that says what to do.
It SHALL open no connection.

*Pilot: verified*

#### Scenario: A sink that can't replace partitions
- **WHEN** a route uses `mode="replace_partition"` and its sink declares only `upsert`
- **THEN** validation reports `mode_unsupported` at `deliver[0].mode` with the modes the sink offers

#### Scenario: A sound flow
- **WHEN** a flow has no mistake
- **THEN** validation returns no problem

### Requirement: Validation codes
Validation SHALL report at least these codes: `strategy_unsupported`, `mode_unsupported`, `mode_needs_complete`
(`replace_partition` on incomplete versions), `planner_required`, `route_name_duplicate`, `trigger_needs_push`,
`push_cannot_fail` (a push dataset with `on_record_error="fail"`), `poller_needs_one_slice` (a poller under five
minutes that reads several slices in full), and the warning `moves_need_state`.

*Pilot: verified*

#### Scenario: A push dataset asked to fail on a bad record
- **WHEN** a dataset with the `push` strategy sets `on_record_error="fail"`
- **THEN** validation reports the error `push_cannot_fail`, because there is no run to fail

### Requirement: Route and dataset names are unique across a project
Validating a project SHALL report two datasets, or two routes, of the same name in different flows of that project.

*Pilot: corrected*

#### Scenario: Two flows name a route alike
- **WHEN** two flows of one project each have a route named `warehouse`
- **THEN** `feather validate` reports an error that names both flows

### Requirement: A test harness that needs no infrastructure
`feather_connect.testing.FlowHarness` SHALL run a flow in process, with every connection swapped for an in-memory
connector and no database. A test SHALL be able to load source records, run the flow, and assert each sink's rows
and deleted keys and the ledger's state.

*Pilot: verified*

#### Scenario: Fan-out and a propagated delete
- **WHEN** a test loads two records, runs the flow, loads one of them changed, and runs again on a `snapshot_diff` dataset
- **THEN** every sink holds the changed record, every sink reports the other key deleted, and the ledger says all deliveries applied

### Requirement: Projects carry conventions for AI assistants
A generated project SHALL include an `AGENTS.md` stating the rules an assistant must follow: transforms are pure,
budgets attach to connections, task arguments never carry data, and every flow has a test.

*Pilot: verified*

#### Scenario: A new project
- **WHEN** `feather new project` creates a project
- **THEN** the project holds an `AGENTS.md` with those rules

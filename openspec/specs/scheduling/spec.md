# scheduling Specification

## Purpose

What starts a run and what a run is: cron and interval triggers evaluated by one scheduler, a record for every tick
whether it ran or not, high-frequency pollers kept readable, and pausing a flow.

## Requirements

### Requirement: Every trigger writes a run
Every trigger SHALL create a run record with its flow, trigger, actor, parameters, status and reason. A tick that
does not go ahead SHALL be recorded as a skipped run with one of the reasons `paused`, `disabled`, `overlap` or
`catch_up_collapsed`. Nothing SHALL be skipped silently. A run SHALL be `queued`, `running`, `succeeded`, `failed`,
`cancelled` or `skipped`.

*Pilot: verified*

#### Scenario: A tick of a paused flow
- **WHEN** a cron tick is due for a paused flow
- **THEN** a run is written with status `skipped` and reason `paused`

#### Scenario: A run that can't start
- **WHEN** a due tick's run can't be started
- **THEN** a run is written with status `failed` and the redacted reason, and the next tick goes ahead

### Requirement: Cron and interval triggers
The scheduler SHALL fire cron triggers at the times their expression gives in their own time zone, and interval
triggers each interval. A trigger seen for the first time SHALL only note the time: nothing before it is made up.
A flow that can't be ticked SHALL be logged and left, and the others SHALL carry on.

*Pilot: verified*

#### Scenario: A daily cron across daylight-saving changes
- **WHEN** a flow runs at 02:30 each day in a zone that changes its clocks
- **THEN** it fires once on each day, the two change days included

#### Scenario: A new flow
- **WHEN** a flow with an hourly trigger is first seen at 10:20
- **THEN** its first run is at 11:00, and none is created for earlier hours

### Requirement: A tick fires exactly once
A due tick SHALL fire once, however many scheduler processes run and whenever one restarts. A crash between noting
a tick and writing its run SHALL NOT lose the tick: the tick and its run record SHALL be written together.

*Pilot: corrected*

#### Scenario: Two schedulers race
- **WHEN** two scheduler processes both see a tick as due
- **THEN** one run is written for it

#### Scenario: A crash at the worst moment
- **WHEN** the scheduler dies just after deciding a tick is due
- **THEN** after restart, exactly one run exists for that tick

### Requirement: Overlap policy
When a tick is due while an earlier run of the flow is still queued or running, the flow's overlap policy SHALL
decide: `queue` (the default) starts the run anyway, `skip` records a skipped run with reason `overlap`, and
`cancel_previous` cancels the active runs and starts the new one.

*Pilot: verified*

#### Scenario: Skip on overlap
- **WHEN** a flow with `overlap="skip"` is still running at its next tick
- **THEN** the tick is recorded as skipped with reason `overlap`

### Requirement: Catch-up after downtime
After downtime, a flow with `catch_up="latest"` (the default) SHALL run once for the gap and record the missed ticks
as one skipped run with reason `catch_up_collapsed`, naming how many and from when to when. A flow with
`catch_up="each"` SHALL run every missed tick, up to the newest 1,000, and SHALL record the rest as collapsed.

*Pilot: verified*

#### Scenario: The server was down for six hours
- **WHEN** an hourly flow with `catch_up="latest"` missed six ticks
- **THEN** one run starts, and one skipped run records five collapsed ticks

### Requirement: High-frequency pollers share a daily run
A trigger with an interval under five minutes SHALL be a poller. Each of its ticks SHALL be a task of one stream run
per flow per day, a skipped tick included, so that the runs list stays readable. A poller's overlap SHALL always be
`skip`. A tick that finds nothing new SHALL seal no version. Missed ticks of a poller SHALL collapse into the
newest one.

*Pilot: verified*

#### Scenario: A 30-second poller
- **WHEN** a flow polls every 30 seconds for a day
- **THEN** the runs list shows one run for that day, with one task per tick

#### Scenario: A tick while a manual run reads
- **WHEN** a poller's tick is due while a manual run of the same flow is reading
- **THEN** the tick is recorded as a skipped task with reason `overlap`

### Requirement: Stream runs are closed and cleaned up
An earlier day's stream run SHALL be closed once its ticks have ended, and what it left behind SHALL be removed.

*Pilot: verified*

#### Scenario: Yesterday's stream run
- **WHEN** a new day's stream run is opened
- **THEN** yesterday's ends `succeeded` once its last tick has finished

### Requirement: Manual runs
An operator SHALL be able to start a run of a flow at once, optionally for named partitions. A manual run of a
paused flow SHALL be refused with the reason.

*Pilot: verified*

#### Scenario: Run now
- **WHEN** an operator starts a run of a flow
- **THEN** a run is written with the trigger `manual` and the operator as actor, and it proceeds

### Requirement: Pause and resume a flow
An operator SHALL be able to pause a flow, with a reason and an optional expiry, and to resume it. A paused flow
SHALL stay in service and record each due tick as skipped. A pause with an expiry SHALL lift by itself. A pause
SHALL be visible to anyone who lists overrides. Pausing something that doesn't exist SHALL be refused.

*Pilot: verified*

#### Scenario: A pause that expires
- **WHEN** a flow is paused until 18:00
- **THEN** ticks before 18:00 are recorded as skipped, and the first tick after 18:00 runs

### Requirement: Planning
A run SHALL plan the partitions it reads: a scheduled or manual run plans the dataset's recent partitions, or those
its planner names; a backfill plans an explicit range; and a `by_stamp` dataset reads once per run across all
partitions.

*Pilot: verified*

#### Scenario: A daily-partitioned snapshot dataset
- **WHEN** a scheduled run plans a `snapshot_diff` dataset partitioned by day
- **THEN** it reads today's and yesterday's partitions in the scheme's time zone

### Requirement: Cancelling a run
An operator SHALL be able to cancel a queued or running run. A cancelled run SHALL seal nothing further.

*Pilot: verified*

#### Scenario: Cancel mid-read
- **WHEN** a multi-slice run is cancelled after two of five slices are read
- **THEN** the run ends `cancelled`, and none of its versions is sealed

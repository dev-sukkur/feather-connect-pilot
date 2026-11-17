# durable-execution Specification

## Purpose

How work survives crashes: a small engine port with a replaceable durable engine behind it, the tasks that move
data, retries, worker liveness, and the sweeper that picks up whatever a dead process left.

## Requirements

### Requirement: The engine sits behind a port
The runtime SHALL reach the durable engine only through a port. Its control side SHALL submit, cancel and signal
tasks, re-queue a dead worker's tasks, say whether a task has ended, and report health; its worker side SHALL run
registered tasks until stopped. A task call SHALL carry a name, arguments, an idempotency key, an optional
concurrency key, labels, a priority and a retry policy. Scheduling, budgets, the ledger and the catalogue SHALL stay
outside the port.

*Pilot: verified*

#### Scenario: Nothing outside the adapter names the engine
- **WHEN** the source is searched for the engine product's name
- **THEN** it appears only in the engine adapter package and its tests

### Requirement: Task arguments carry ids only
Engine task arguments SHALL carry identifiers, never records, and SHALL be at most 64 KB. Data SHALL live in the
spool.

*Pilot: verified*

#### Scenario: An apply task's arguments
- **WHEN** an apply task is submitted for a version of 2 million rows
- **THEN** its arguments hold the route and the version id and nothing of the rows

### Requirement: What a task body may rely on
A task body SHALL get a context with `step(name, fn)`, `sleep(duration)` and `wait_signal(key, timeout)`. Once a
step has succeeded, a retry or a replay of the task SHALL return the result it saved and SHALL NOT run it again.
A sleep SHALL survive a restart of the worker. The port SHALL have no wall-clock task timeout, because a long wait
is a durable sleep and a task's age says nothing about whether it is stuck.

*Pilot: verified*

#### Scenario: A retry after two good steps
- **WHEN** a task's third step fails and the task is retried
- **THEN** the first two steps are not run again

#### Scenario: A sleep across a restart
- **WHEN** a task sleeps for an hour and its worker restarts after ten minutes
- **THEN** the task wakes about fifty minutes later, not an hour later

### Requirement: Idempotent submission
Submitting a task whose idempotency key was already submitted SHALL run the task once and SHALL return the existing
task.

*Pilot: verified*

#### Scenario: The sweeper submits what a worker already submitted
- **WHEN** the same apply task is submitted twice
- **THEN** it executes once

### Requirement: One task at a time per concurrency key
Tasks that share a concurrency key SHALL run one at a time, in priority order and then in submission order. The
runtime SHALL key planning by flow, extraction by dataset and partition, run sealing by dataset, and apply by route
and partition, so that there is one writer per (route, partition).

*Pilot: verified*

#### Scenario: Two versions of one partition
- **WHEN** apply tasks for versions 5 and 6 of one (route, partition) are queued
- **THEN** they never run at the same time

### Requirement: Retries with backoff and jitter
A failed task SHALL be retried by its policy: by default 6 attempts in all, with a wait that starts at 10 seconds,
doubles with each failure up to 10 minutes, and is cut by equal jitter to between half and all of that wait. A
permanent error SHALL end the task with no retry. A wait for budget SHALL NOT use up an attempt.

*Pilot: verified*

#### Scenario: The waits of the default policy
- **WHEN** a task fails five times with transient errors
- **THEN** its waits fall within 5–10 s, 10–20 s, 20–40 s, 40–80 s and 80–160 s

### Requirement: Killing any process loses no acknowledged work
Killing any worker, server or database process with kill -9, or restarting Postgres, SHALL lose no acknowledged
work and SHALL duplicate no delivery.

*Pilot: verified*

#### Scenario: kill -9 during extraction
- **WHEN** a worker is killed while it writes a version's chunks
- **THEN** exactly one clean version is sealed after recovery

#### Scenario: Postgres restarts during apply
- **WHEN** Postgres is restarted while deliveries are being applied
- **THEN** every delivery ends applied once, and none is lost

### Requirement: Worker liveness
Each worker SHALL record a heartbeat every 5 seconds, with its id, labels, code version and slots. A worker with no
heartbeat for 30 seconds SHALL be declared dead by the sweeper, which SHALL then re-queue its tasks through the
port. A draining worker that stops beating SHALL be recovered too. A worker that starts again under an id marked
dead or draining SHALL be marked alive. If recovery fails, the worker SHALL be left for the next sweep, not marked
recovered.

*Pilot: verified*

#### Scenario: A worker dies and never returns
- **WHEN** a worker is killed and not restarted
- **THEN** within about 30 seconds its tasks are re-queued and another worker finishes them

#### Scenario: A keyed task waits its turn
- **WHEN** a dead worker's keyed task is re-queued while another task waits on the same key
- **THEN** the re-queued task runs first

### Requirement: Graceful drain
On a stop signal a worker SHALL stop taking new tasks, give its running tasks up to 30 seconds to finish, and leave
the rest to be re-queued.

*Pilot: verified*

#### Scenario: A deploy mid-drain
- **WHEN** a worker is stopped while tasks are queued and running, and a new worker starts
- **THEN** every task finishes once

### Requirement: The sweeper
A leader-elected sweeper SHALL, on each pass: submit again a queued run that lost its submit; submit again a pending
delivery that has no active task; submit again the sealing of a run whose slices are all read; fail a delivery
whose task has ended and left it open; and discard the staged versions of failed and cancelled runs. It SHALL leave failed and held deliveries alone, and SHALL do no work for
deliveries waiting behind a failure.

*Pilot: verified*

#### Scenario: A submit is lost
- **WHEN** a run is written as queued and the process dies before submitting its plan
- **THEN** the sweeper submits it, and the run proceeds

#### Scenario: A backlog behind a failure
- **WHEN** 10,000 deliveries wait behind one failed delivery
- **THEN** a sweep's cost does not grow with the 10,000

### Requirement: One leader for singleton loops
The scheduler, the sweeper and retention SHALL each run on one process at a time, chosen through Postgres, and
another process SHALL take over when the leader stops.

*Pilot: verified*

#### Scenario: Two servers
- **WHEN** two server replicas run
- **THEN** each scheduled tick fires once

### Requirement: Two engines, one contract
The system SHALL ship an in-process engine that is deterministic and not durable, for tests and local runs, and a
durable engine for production. Both SHALL pass the same engine conformance suite where it applies.

*Pilot: verified*

#### Scenario: A flow test
- **WHEN** a flow test runs on the in-process engine
- **THEN** it needs no database and gives the same result on every run

### Requirement: Engine conformance suite
A suite SHALL run against the durable engine on every adapter change and engine upgrade. It SHALL check priority and
one-at-a-time per key, kill -9 mid-step, a deploy mid-drain, a rollback that resumes dequeuing, deduplication, a
durable sleep across a restart, retry timing, a succeeded step not run again, schema isolation, two workers sharing
work, a shared budget, dispatch latency, and recovery of a dead worker in key order.

*Pilot: verified*

#### Scenario: An engine upgrade
- **WHEN** the pinned engine version is raised
- **THEN** the conformance suite runs, and the upgrade merges only if it passes

### Requirement: Engine operating rules stay inside the adapter
The adapter SHALL keep the engine's application version fixed across ordinary deploys, so that in-flight tasks are
recovered by new workers, and SHALL keep each replica's executor identity stable across restarts. Flow code SHALL
run inside steps, so that changing a flow never changes a task's step sequence. Queue polling SHALL default to
0.5 seconds and be configurable.

*Pilot: verified*

#### Scenario: A deploy with tasks in flight
- **WHEN** new worker code is deployed while 50 tasks are in flight
- **THEN** all 50 finish, and none is stranded

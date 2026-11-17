# observability Specification

## Purpose

How people and tools see what the system did without a separate monitoring stack: structured logs, task logs kept
for the dashboard, a trace of any record by its business key, and health endpoints.

## Requirements

### Requirement: Structured logs with correlation ids
Every process SHALL write JSON logs to standard output. A log line written while work is in hand SHALL carry the
ids of that work: tenant, flow, run, task, dataset, partition, version and route, as far as each applies.

*Pilot: verified*

#### Scenario: A line from an apply task
- **WHEN** a sink logs a warning during an apply
- **THEN** the line holds the run, the task, the route, the version and the partition

### Requirement: Logs never hold secrets
Known secret values and common token patterns SHALL be masked in log lines, in stored errors and in anything shown
on a page or returned by the API. Messages that may quote a request body, such as a webhook parser's error, SHALL
be logged by their class only.

*Pilot: verified*

#### Scenario: A connector echoes a credential
- **WHEN** a database error message quotes the connection string with its password
- **THEN** the stored error and the log line show the password masked

### Requirement: Task logs are kept for the dashboard
A capped excerpt of each task's log SHALL be stored with the task, so that the dashboard, the API and the CLI show
a run's logs with no log stack.

*Pilot: verified*

#### Scenario: Reading a run's logs
- **WHEN** `feather logs <run>` runs
- **THEN** it prints the stored lines of each of the run's tasks, in order

### Requirement: The task journal
Every engine task SHALL have a journal row with its run, kind, dataset, partition, route, worker, code version,
status, attempt, last error and times. A task that was retried SHALL show the attempt it is on and its last failure.

*Pilot: verified*

#### Scenario: A retried task
- **WHEN** a task succeeds on its third attempt
- **THEN** its row shows attempt 3 and the error of the second

### Requirement: A task's records
For each task the system SHALL be able to show which records it read or applied, by key, with each record's
outcome, without reading the whole version.

*Pilot: verified*

#### Scenario: The records of an apply task
- **WHEN** an operator opens the records of an apply task
- **THEN** they see each key it applied, and for a rejected key the reason

### Requirement: Record trace by business key
Given a dataset and a business key, the system SHALL show every sealed version that held the record, in order, with
whether it was inserted, updated or deleted there, and for each route what became of that version: applied, failed,
skipped or rejected, with the destination's answer where one was kept.

*Pilot: verified*

#### Scenario: Where is invoice INV-1001?
- **WHEN** an operator traces `INV-1001` in the invoices dataset
- **THEN** they see the three versions that held it, and that the warehouse route applied all three while the ERP route rejected the second with a reason

### Requirement: Health and readiness
The server SHALL answer `/healthz` when its process is up and `/readyz` only when it can reach the database. Neither
SHALL need credentials.

*Pilot: verified*

#### Scenario: The database is down
- **WHEN** Postgres is unreachable
- **THEN** `/healthz` answers 200 and `/readyz` does not

### Requirement: A summary of the environment
`GET /api/v1/summary` SHALL return the environment's name, its failing flows, its stale datasets, budget use and
worker status, as the overview page shows them. A dataset SHALL count as stale when its newest version is older
than two intervals of its flow.

*Pilot: verified*

#### Scenario: A flow that stopped
- **WHEN** an hourly flow has sealed nothing for three hours
- **THEN** the summary lists its dataset as stale, with the time of its last version

### Requirement: Worker code versions are visible
Each worker SHALL register the code version it runs, and every task and delivery attempt SHALL record the code
version that ran it.

*Pilot: verified*

#### Scenario: Which code applied this delivery?
- **WHEN** an operator opens a delivery's history
- **THEN** each attempt shows the code version that made it

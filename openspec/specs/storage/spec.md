# storage Specification

## Purpose

Where everything lives: Postgres as the only required database, the store contract that both the in-memory and the
Postgres store meet, the spool that holds version data before delivery, and the retention that keeps it from growing
without bound.

## Requirements

### Requirement: Postgres is the only required database
The system SHALL need no stateful dependency other than Postgres 15 or later, with no extensions, so that any
managed Postgres works. The catalogue, runs, spool, ledger, record state, audit and the engine's own state SHALL all
live in that one database, so that one point-in-time backup covers the whole system. The system's tables SHALL live
in their own schema, apart from the engine's.

*Pilot: verified*

#### Scenario: A managed Postgres with no extensions
- **WHEN** the migrations run against a Postgres 15 database on which no extension can be created
- **THEN** they succeed

### Requirement: One store contract, two implementations
The runtime SHALL reach storage only through a store contract. An in-memory store and a Postgres store SHALL both
implement it, and one suite of contract tests SHALL run against both. The contract SHALL be split by aggregate:
the run journal, versions, the ledger, record state, quarantine, destination answers, the inbox, budgets, the
catalogue, workers, overrides, schedules, users and tokens, the registry, packages, audit, environment
configuration, secrets and webhook events.

*Pilot: verified*

#### Scenario: The same tests, both stores
- **WHEN** the contract tests run
- **THEN** every test passes against the in-memory store and against Postgres

### Requirement: Sealing is one transaction
Sealing a version SHALL, in one transaction, mark the version sealed, give it the next seal sequence number of its
dataset, write its key index, and insert one pending delivery per route. A version claimed by a newer writer SHALL
refuse writes and seals from the writer it overtook.

*Pilot: verified*

#### Scenario: A task runs twice at once
- **WHEN** two executions of one extract task both write the same version
- **THEN** the later claim wins, the earlier one's writes are refused, and one version is sealed

### Requirement: Spool chunks
Version data SHALL be stored as compressed Parquet chunks of at most 50,000 rows. Every data chunk SHALL hold the
payload columns, nested types included, plus `_fc_key`, `_fc_op` and `_fc_row_hash`. Column names starting with
`_fc_` SHALL be reserved, and a source column that uses the prefix SHALL be refused. A chunk SHALL read back as the
same Python values that were written. Chunk kinds SHALL include `data`, `snapshot` and `full`.

*Pilot: verified*

#### Scenario: A round trip of awkward values
- **WHEN** a record holding a decimal, a date, an aware datetime, bytes and a nested list is written and read back
- **THEN** each value comes back equal and of the same type

#### Scenario: A reserved column name
- **WHEN** a source yields a column named `_fc_object_mtime`
- **THEN** the extract fails with a message naming the reserved prefix

### Requirement: Spool throughput
One worker SHALL write at least 20,000 rows per second from a source batch to the spool, source limits apart.

*Pilot: verified*

#### Scenario: The throughput test
- **WHEN** the spool throughput test writes a million rows with one writer
- **THEN** the rate is at least 20,000 rows per second

### Requirement: The key index
Sealing SHALL record, for each key in the version, the version, partition, operation, row hash and chunk number, so
that a record can be found by its business key without reading chunks.

*Pilot: verified*

#### Scenario: Finding a key
- **WHEN** the history of key `INV-1001` is asked for
- **THEN** every sealed version that held it is returned in seal order, with the chunk that holds it

### Requirement: Retention
A retention job SHALL remove, by default: spool chunks 14 days after every route has applied their version; task
logs after 30 days; runs, tasks and deliveries after 180 days; version manifests after 400 days; inbox events 7 days
after sealing; and webhook event records after 30 days. Snapshots and record state SHALL be kept while the dataset
exists. A day of spool that still holds an open delivery SHALL be kept, and the job SHALL report why.

*Pilot: verified*

#### Scenario: An open delivery keeps its data
- **WHEN** a version is 20 days old and one route has not applied it
- **THEN** its chunks are kept, and the retention report names the route

#### Scenario: Old logs
- **WHEN** the retention job runs
- **THEN** task logs older than 30 days are gone

### Requirement: Growth is managed by partitions
Tables that grow with traffic SHALL be partitioned by time with Postgres native partitioning: by day for spool
chunks and the inbox, and by month for delivery attempts, task logs and audit. The system SHALL create future
partitions ahead of time and SHALL remove expired data by dropping whole partitions. Dropping an inbox day SHALL NOT
deadlock with events arriving. Indexes over work in progress SHALL be partial.

*Pilot: verified*

#### Scenario: Tomorrow's partition exists
- **WHEN** the clock passes midnight
- **THEN** writes to the spool and the inbox succeed without waiting for a partition to be created

### Requirement: Database connections are bounded
Every connection pool SHALL have a connect timeout, keepalives and a statement timeout. Reloading in development
SHALL NOT leak connections.

*Pilot: verified*

#### Scenario: The database stops answering
- **WHEN** Postgres accepts a connection and never answers a statement
- **THEN** the statement fails after its timeout, and the caller gets an error

### Requirement: Forward-only migrations with one baseline
The schema SHALL be created and changed by forward-only migrations that run with `feather server migrate`. A new
repository SHALL start from a single baseline migration.

*Pilot: partial*

#### Scenario: An empty database
- **WHEN** `feather server migrate` runs against an empty database
- **THEN** the full schema exists, and running it again changes nothing

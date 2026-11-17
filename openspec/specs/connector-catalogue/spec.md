# connector-catalogue Specification

## Purpose

The connectors that ship with the first release, so that real integrations can be built without writing a
connector: simulation, Postgres, object storage, HTTP in four forms, DuckDB and MotherDuck, and Zoho Zakya.

## Requirements

### Requirement: JSON simulation connector
A simulation source SHALL serve records from JSON or JSONL fixtures, or from generator specs with a fixed seed so
that runs are repeatable, for the `replace`, `by_stamp` and `push` strategies. It SHALL be able to inject faults:
duplicates, late and out-of-order rows, hard deletes, key rewrites, schema changes, and 429s, 5xxs and timeouts,
each with a configurable probability. A simulation sink SHALL capture what it is given, to memory or to JSON files,
for assertions.

*Pilot: verified*

#### Scenario: A repeatable run
- **WHEN** a generated source is read twice with the same seed
- **THEN** both reads give the same records

#### Scenario: Injected rate limits
- **WHEN** a simulation source is set to answer 429 one time in ten
- **THEN** a flow reading it retries and still delivers every record once

### Requirement: Postgres connector
The Postgres connector SHALL read a table or a query with the `by_stamp`, `snapshot_diff` and `replace` strategies,
and SHALL write to a table in the `upsert`, `append` and `replace_partition` modes. An append route SHALL add the
history columns it needs to the destination table. The connector SHALL pass the conformance kit.

*Pilot: verified*

#### Scenario: Replicate a table incrementally
- **WHEN** a table with an `updated_at` column is read with `by_stamp` every ten minutes into a warehouse table
- **THEN** each run delivers only the rows changed since the last

### Requirement: Object storage connector
The object storage connector SHALL read CSV, JSON, JSONL and Parquet files from S3-compatible storage and from
local folders, with the `replace` strategy and with `by_stamp` on the object's modification time, exposed as the
column `_object_mtime`. It SHALL write files per partition. A row it can't parse SHALL be quarantined with its raw
text, and the rest of the file SHALL be read.

*Pilot: verified*

#### Scenario: A file is dropped
- **WHEN** a new CSV file appears in a bucket
- **THEN** the next run reads that file and none of the older ones

### Requirement: REST source
The REST source SHALL read paginated APIs by offset, page number, cursor and link header, with the `by_stamp`,
`replace` and `snapshot_diff` strategies, and SHALL be able to resume from the page it had reached. It SHALL stop
with an error after a bounded number of pages.

*Pilot: verified*

#### Scenario: A cursor-paged API
- **WHEN** an API returns 25 pages linked by a cursor
- **THEN** the read yields every record of the 25 pages, each call metered against the connection's budget

### Requirement: GraphQL source and sink
The GraphQL connector SHALL read cursor connections and SHALL write through mutations, with the same strategies and
authentication as REST.

*Pilot: verified*

#### Scenario: A connection with pages
- **WHEN** a query's connection has `hasNextPage` true
- **THEN** the source follows `endCursor` until the last page

### Requirement: HTTP call sink
The HTTP call sink SHALL send each record as a request built from a template, SHALL map fields of the answer into
the delivery's kept answers, and SHALL carry the delivery's idempotency key and version in headers. By default it
SHALL treat 401, 403, 404, 405 and 407 as failures of the whole delivery, a 409 as already applied, and other 4xx
answers as the rejection of one record. It SHALL reject delete records unless told how to send them.

*Pilot: verified*

#### Scenario: One record is refused by the API
- **WHEN** an API answers 422 for one order of a hundred
- **THEN** ninety-nine are applied, and the one is quarantined with the status and the body that was sent

#### Scenario: A repeat after a crash
- **WHEN** a chunk is sent again and the API answers 409 for records it already has
- **THEN** those records count as applied

### Requirement: Webhook source
The webhook source SHALL be a push source configured with `signature_secret`, `unsigned` and `event_id_header`, and
SHALL verify HMAC-SHA256 signatures with a timestamp tolerance.

*Pilot: verified*

#### Scenario: Webhook to API
- **WHEN** a signed webhook arrives for a flow that delivers to an HTTP call sink
- **THEN** the destination API receives the mapped record within the push latency budget

### Requirement: DuckDB and MotherDuck connector
The DuckDB connector SHALL read by SQL with every strategy, and SHALL write Arrow batches to a local database file
or to MotherDuck in the `append`, `upsert` and `replace_partition` modes. There SHALL be one writer per (table,
partition), and long writes SHALL be split into chunks.

*Pilot: verified*

#### Scenario: A bronze table
- **WHEN** invoices are delivered to a DuckDB table in `append` mode
- **THEN** the table holds every version of every invoice, with its version and operation

### Requirement: Zoho Zakya connector
The Zakya connector SHALL authenticate with Zoho OAuth2 against the region's host, and SHALL meter every call against a budget with per-minute and per-day windows, shared through the vendor
account `zakya:<organisation id>`. It SHALL read invoices, credit notes, payments, items and price lists, by
`last_modified_time` or in full, and SHALL write items, prices and stock idempotently. It SHALL pass the conformance
kit against a simulator.

*Pilot: verified*

#### Scenario: A changed invoice
- **WHEN** an invoice that was delivered is changed in Zakya
- **THEN** the next run delivers it once more, and unchanged invoices not at all

#### Scenario: A budget for another organisation
- **WHEN** a connection for organisation 60001 names a budget whose account is `zakya:60002`
- **THEN** `feather validate` reports the mismatch

### Requirement: Connectors are documented and tested
Every connector SHALL have reference documentation of its settings, strategies and modes, and a runnable example
that is tested in CI.

*Pilot: built*

#### Scenario: A connector's example
- **WHEN** CI runs
- **THEN** each connector's example flow validates and runs on simulated data

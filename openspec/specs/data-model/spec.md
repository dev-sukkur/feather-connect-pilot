# data-model Specification

## Purpose

The concepts every flow is built from, for batch extracts and application events alike: records, keys, partitions,
versions and deliveries, and the invariants that hold for all of them.

## Requirements

### Requirement: Records are keyed by business identity
A record SHALL be one keyed document with a key, an operation (`upsert` or `delete`), a payload and a row hash. Its
key SHALL be derived from the payload fields the dataset names: the value as text for one key field, and a canonical
JSON list of the values for several. A record with no value for a key field SHALL be rejected, never stored under a
made-up key.

*Pilot: verified*

#### Scenario: A single key field
- **WHEN** a dataset with `key="invoice_id"` reads a payload whose `invoice_id` is `1001`
- **THEN** the record's key is the text `1001`

#### Scenario: Several key fields
- **WHEN** a dataset with `key=("material", "price_list")` reads a payload with `material="M1"` and `price_list="RET"`
- **THEN** the record's key is the canonical JSON list `["M1","RET"]`

#### Scenario: A missing key field
- **WHEN** a payload has no value, or a null value, for a key field
- **THEN** the record is quarantined with the reason
- **AND** the rest of the read carries on

### Requirement: Row hashes are stable
The row hash of a payload SHALL be the 8-byte BLAKE2b digest of its canonical JSON, read as a big-endian unsigned
integer. Canonical JSON SHALL sort keys, use no spaces, and encode decimals as text, dates and datetimes as ISO-8601
with aware datetimes converted to UTC, and bytes as base64. The hash of the same data MUST never change between
releases, because it is stored and compared with later reads.

*Pilot: verified*

#### Scenario: Equal instants give equal hashes
- **WHEN** two payloads differ only in the UTC offset used to write the same instant
- **THEN** their row hashes are equal

#### Scenario: Key order does not matter
- **WHEN** two payloads hold the same fields in a different order
- **THEN** their row hashes are equal

### Requirement: Partitions name business slices
A scheme SHALL derive each record's partition key, such as `store_id=1509/date=2026-10-18`, from its
fields. The system SHALL provide the schemes `none` (the default),
`by_day(field, tz)`, `by_month(field, tz)`, `by_bucket(field, n)` and `by_fields(...)`, which composes the others.
Date schemes SHALL require an explicit IANA time zone. A partition SHALL be the unit of planning, re-runs and
backfills, and SHALL NOT be the unit of storage or identity.

*Pilot: verified*

#### Scenario: A day partition in a time zone
- **WHEN** a dataset partitioned `by_day("date", tz="Asia/Kolkata")` reads a record stamped 2026-10-18T20:00:00Z
- **THEN** the record's partition is `date=2026-10-19`

#### Scenario: A composed scheme
- **WHEN** a dataset is partitioned `by_fields("store_id", by_day("date", tz))`
- **THEN** a record of store 1509 dated 18 October 2026 has the partition `store_id=1509/date=2026-10-18`

#### Scenario: A bucket keeps a key in one stream
- **WHEN** a dataset is partitioned `by_bucket("increment_id", 8)`
- **THEN** every record with the same `increment_id` falls in the same one of 8 partitions

#### Scenario: A record with no partition
- **WHEN** a record's partition field is missing or can't be read as the scheme needs
- **THEN** the record is quarantined with the reason and the rest of the read carries on

### Requirement: Partitions have business labels
Each scheme SHALL give a partition key a label in business words, such as "Store 1509 · 18 Oct 2026", and the
bounds of each of its fields. Every surface that shows a partition to a person SHALL show the label.

*Pilot: partial*

#### Scenario: A label for a composed key
- **WHEN** the partition `store_id=1509/date=2026-10-18` is shown on a page
- **THEN** it reads "Store 1509 · 18 Oct 2026"

### Requirement: Versions are immutable batches
Each extraction of one dataset partition SHALL produce a version: an immutable batch of upserts and deletes with a
manifest holding its counts, content hash, schema hash, watermark, run and code version. A version SHALL be either
complete, describing the whole slice, or incomplete, carrying only the changes it read. A version's id SHALL be
derived from its run, dataset and partition, so that a restarted extraction writes the same version.

*Pilot: verified*

#### Scenario: A sealed version can't be written
- **WHEN** a task tries to write a chunk to a version that is sealed
- **THEN** the write is refused
- **AND** the sealed chunks are unchanged

#### Scenario: A restarted extraction
- **WHEN** an extract task is killed after writing some chunks and runs again
- **THEN** it writes the same version id, replaces the chunks it had written and removes any left over
- **AND** exactly one version is sealed

### Requirement: Only complete versions imply deletes
A complete version SHALL imply the deletion of every key that the slice held before and no longer holds. An
incomplete version SHALL delete only through explicit delete records.

*Pilot: verified*

#### Scenario: A key missing from a complete read
- **WHEN** a complete version of a partition lacks a key that the previous snapshot held
- **THEN** the version carries a delete for that key

#### Scenario: A key missing from an incomplete read
- **WHEN** an incomplete version lacks a key that an earlier version held
- **THEN** no delete is produced for that key

### Requirement: An empty complete read is refused
A complete read that returns no rows SHALL fail unless the dataset sets `allow_empty=True`, so that a transient empty
answer from a source cannot delete a whole slice at its destinations.

*Pilot: verified*

#### Scenario: A source answers with nothing
- **WHEN** a `snapshot_diff` read of a partition that held 500 records returns no rows, and `allow_empty` is not set
- **THEN** the extract fails, no version is sealed and no destination is changed

### Requirement: A moved record is an update
A record whose partition changes SHALL be an update of the same record, not a new one. With `keep_state=True`, the
upsert in the new partition SHALL carry `moved_from` naming the old partition, so that a route which replaces
partitions removes the record from the old slice.

*Pilot: verified*

#### Scenario: An invoice's date is edited
- **WHEN** an invoice stored under 18 October is re-read with its date changed to 19 October
- **THEN** the 19 October partition gets an upsert with `moved_from` naming the 18 October partition
- **AND** a destination that replaces partitions holds the invoice once, under 19 October

### Requirement: Record state is kept on request
With `keep_state=True` the system SHALL keep, per key, the latest payload, row hash, partition and last version.
Record state SHALL be off by default.

*Pilot: verified*

#### Scenario: State after a seal
- **WHEN** a version of a dataset with `keep_state=True` is sealed
- **THEN** the state of each of its upserted keys holds that version's payload, hash and partition
- **AND** the state of each deleted key is removed

#### Scenario: State after a crash
- **WHEN** a process dies after sealing a version and before writing its record state
- **THEN** the next seal of the dataset writes the missing state first

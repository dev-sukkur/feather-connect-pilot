# change-detection Specification

## Purpose

How a read of a source becomes the upserts and deletes of a version: the change strategies, the rules that keep
unchanged rows from churning destinations, and how deletes and moves are decided across the slices of one run.

## Requirements

### Requirement: Incremental reads by change stamp
The `by_stamp(field, lookback, deleted_field)` strategy SHALL read the records whose change stamp is at or after the
dataset's last watermark less the lookback, and seal incomplete versions. It SHALL read once per run and seal one
version per partition it touched. Stamps SHALL be dates, datetimes, numbers or ISO-8601 text, of one kind for the
life of the dataset; a record with a stamp of another kind SHALL be quarantined.

*Pilot: verified*

#### Scenario: A second run reads only what changed
- **WHEN** a run has sealed records stamped up to 10:00 and a later run reads with `lookback="2h"`
- **THEN** the source is asked for records stamped 08:00 or later

#### Scenario: A soft delete
- **WHEN** a record arrives with the dataset's `deleted_field` set
- **THEN** the version carries a delete for its key

#### Scenario: A stamp of the wrong kind
- **WHEN** a dataset whose stamps are datetimes reads a record whose stamp is the text `n/a`
- **THEN** the record is quarantined and the watermark does not move to it

### Requirement: A longer lookback reaches back
When a dataset's lookback is made longer, the next read SHALL start at the last watermark less the new lookback, and
the no-op check SHALL compare against every version whose watermark falls in that window.

*Pilot: corrected*

#### Scenario: The lookback grows from 2 hours to 2 days
- **WHEN** a dataset that read with `lookback="2h"` is redeployed with `lookback="2d"` and runs
- **THEN** records stamped within the last two days are read
- **AND** those unchanged since an earlier version are dropped as no-ops

### Requirement: Whole-slice diff for stampless sources
The `snapshot_diff()` strategy SHALL read the whole partition, compare each record's row hash with the previous
snapshot of that partition, and seal a complete version holding the added and changed records and a delete for each
key that vanished. It SHALL keep one snapshot per partition, of each record's key and row hash, for as long as the
dataset exists. A read whose rows are all unchanged SHALL seal no version.

*Pilot: verified*

#### Scenario: Inserts, updates and hard deletes
- **WHEN** a table with no change stamp gains a row, changes a row and loses a row between two reads
- **THEN** the second version holds one upsert for the new row, one for the changed row and one delete

#### Scenario: Nothing changed
- **WHEN** a partition is read twice with the same content
- **THEN** the second read seals no version and no delivery is created

### Requirement: Full reload
The `replace()` strategy SHALL read the whole partition and seal a complete version holding every record read, with
a delete for each key that vanished since the previous read.

*Pilot: verified*

#### Scenario: A small master is reloaded
- **WHEN** a `replace` dataset of 40 records is read and 2 were removed at the source
- **THEN** the version holds 38 upserts and 2 deletes

### Requirement: Custom strategies
A dataset SHALL accept any object that implements the strategy contract: a name, whether its versions are complete,
and a `diff(new, old)` that returns the keys to upsert and the keys to delete.

*Pilot: verified*

#### Scenario: A custom strategy is used
- **WHEN** a dataset is given a strategy object of the author's own
- **THEN** its `diff` decides the version's upserts and deletes
- **AND** its `complete` flag decides whether routes may replace partitions

### Requirement: Unchanged records are dropped
A record whose row hash equals the last known state of its key SHALL be left out of the version. The last known
state SHALL come from record state when the dataset keeps it, and otherwise from the versions whose watermark falls
within the lookback window.

*Pilot: verified*

#### Scenario: A lookback re-reads rows
- **WHEN** a `by_stamp` run re-reads 1,000 rows inside its lookback and 3 have changed
- **THEN** the version holds 3 upserts
- **AND** a row that became visible late is captured exactly once

### Requirement: A rewritten key keeps one current row
When a source rewrites its own row identifier in place, a dataset keyed by business identity SHALL keep exactly one
current record for that business key.

*Pilot: verified*

#### Scenario: The source's internal id changes
- **WHEN** a price for (material M1, price list RET) is re-read with a new internal condition id
- **THEN** destinations hold one row for (M1, RET), with the new values

### Requirement: Deletes across slices are decided for the whole run
A run that reads several slices in full SHALL stage each slice's version with its candidate deletes, and SHALL decide
the deletes only after every slice of the run has been read. A candidate found in another slice of the same run
SHALL be treated as a move: it is dropped as a delete, and the upsert records `moved_from`. All of the run's
versions SHALL then be sealed in one transaction.

*Pilot: verified*

#### Scenario: A record moves between two slices of one run
- **WHEN** a run reads slices A and B in full, and a key that was in A is now in B
- **THEN** no delete is sealed for the key in A
- **AND** the upsert in B carries `moved_from` naming A

#### Scenario: One slice fails
- **WHEN** one slice of a multi-slice run fails to read
- **THEN** none of the run's versions is sealed until the slice is retried or the run is abandoned

### Requirement: Staged versions of failed runs are cleaned up
The system SHALL discard the staged versions and working chunks of a run that failed or was abandoned.

*Pilot: verified*

#### Scenario: A run is abandoned
- **WHEN** a multi-slice run is cancelled after staging two versions
- **THEN** the two staged versions and their chunks are removed

### Requirement: Unreadable rows are quarantined at the source
A source SHALL be able to yield a marker for a row it could not parse, with the raw text and the reason. The system
SHALL quarantine it and carry on with the rest of the read. Records that fail the dataset's schema SHALL be
quarantined the same way, or SHALL fail the whole version when the dataset sets `on_record_error="fail"`.

*Pilot: verified*

#### Scenario: A CSV row with too few fields
- **WHEN** a file source yields an unreadable row among 100 good ones
- **THEN** the version holds 100 records and the quarantine holds the raw row with its reason

#### Scenario: The dataset asks to fail
- **WHEN** a dataset with `on_record_error="fail"` reads a record that fails its schema
- **THEN** the extract fails and no version is sealed

### Requirement: Reads can resume
A source that declares itself resumable SHALL be able to save a checkpoint during a read. A read that is interrupted
SHALL be started again from the latest checkpoint.

*Pilot: verified*

#### Scenario: A worker dies mid-read
- **WHEN** a resumable source has saved a checkpoint after page 40 and its worker is killed
- **THEN** the retried read asks the source to resume from that checkpoint

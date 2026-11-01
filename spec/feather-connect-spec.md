# Feather Connect Open Specification

**Version 0.1.0-draft.** Licence: MIT.

This specification defines a language-neutral model for moving keyed data from sources to many destinations with one set of concepts for batch and events. It defines the concepts, the byte-exact encodings, the rules a runtime must keep, and the contracts for connectors and durable engines. It does not define a programming-language API, a database layout, a user interface or a choice of engine.

The Python package `feather-connect` is the reference implementation. If this document and the conformance vectors disagree, that is a defect in the specification and should be reported.

## 1. Conventions and conformance

**MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT** and **MAY** are used as described in BCP 14 (RFC 2119, RFC 8174) when they appear in capitals.

Sections and features marked **[Reserved]** have their names and meaning set aside, but they are not part of conformance yet. An implementation that does not support one MUST refuse it rather than ignore it.

| Level | What an implementation claims | How it is checked |
|---|---|---|
| **Encoding** | It produces the same bytes and values for keys, hashes, ids, partitions, durations and limits | §4 and the [vectors](conformance/vectors.json) |
| **Model** | It also follows the delivery state machine, change strategies and budgets | §6, §8, §10 and the vectors |
| **Runtime** | It also keeps every invariant in §5 through crashes and retries | §5, §9, §11 and the scenarios in §14 |

Claim a level as, for example, "Feather Connect Spec 0.1 Runtime". Interchange documents are defined by the [JSON Schemas](schemas/) and are optional at every level, but an implementation that exchanges these documents MUST use those schemas.

## 2. Concepts

| Concept | Definition |
|---|---|
| **Connector** | Code that reads from or writes to a kind of system. It declares its abilities in a connector spec (§11.1). |
| **Connection** | A configured connector instance. Secrets are held by reference, never by value. |
| **Dataset** | A named stream of keyed records from one source entity, with a key, a partition scheme and a change strategy. |
| **Record** | One keyed document: the unit of identity. It has a `key`, an `op` (`upsert` or `delete`), a `payload` and a `row_hash`. |
| **Partition** | A named business slice derived from record fields. It is used to plan, re-run and verify, not to store. |
| **Version** | An immutable batch of upserts and deletes for one (dataset, partition) from one run. A **complete** version describes the whole slice. An **incomplete** version describes only changes. |
| **Run** | One execution of a flow, and the record that a trigger fired. |
| **Route** | A path from a flow's datasets to one sink: an optional transform, a mode and a priority. |
| **Delivery** | The application of one version to one route. The set of deliveries is the **ledger**, which is both each route's cursor and its dead-letter state. |
| **Flow** | The deployable unit: the datasets it reads, the routes it delivers to and its triggers. |
| **Budget** | A named, shared rate limit with one or more windows and an optional priority reserve. |
| **Quarantine** | Records turned down, with the reason, while the rest of the version proceeds. |
| **Spool** | Where version data lives between extraction and delivery. |

## 3. Architecture requirements

A Runtime implementation separates three things:

1. **Extraction** reads a source and writes versions to the spool.
2. **The ledger** records each route's progress through sealed versions.
3. **The durable engine** runs tasks and survives crashes. It carries identifiers, never data (I8).

Extraction never waits for a route. A destination outage only grows that route's backlog (I5).

## 4. Canonical encodings

Everything in this section is stored and compared across time. For the same input these values MUST NOT change, in any version of an implementation.

All hashes are **BLAKE2b** (RFC 7693), unkeyed, with the digest size stated.

### 4.1 Canonical JSON

Canonical JSON is the UTF-8 encoding of a JSON text with these rules:

- **Objects.** Members are sorted by key, comparing Unicode code points. Keys MUST be strings. There is no whitespace: `,` and `:` are the only separators.
- **Strings.** Printable ASCII (U+0020 to U+007E) is written as itself, except `"` as `\"` and `\` as `\\`. U+0008, U+0009, U+000A, U+000C and U+000D are written `\b`, `\t`, `\n`, `\f` and `\r`. Everything else, including U+007F, is written `\uXXXX` with lowercase hex, using a UTF-16 surrogate pair above U+FFFF.
- **Integers.** Plain decimal, of any size, with no exponent.
- **Floats.** The shortest decimal text that reads back as the same IEEE 754 double:
  - A float always shows it is one, so `1.0`, never `1`.
  - It uses positional form when the decimal exponent is from -4 to 15, and otherwise exponent form with a sign and at least two exponent digits: `1e+16`, `1.5e-05`.
  - Non-finite values are written `NaN`, `Infinity` and `-Infinity`. Those tokens are not valid JSON, so they SHOULD NOT appear in payloads, but they hash as written.
- **Literals.** `true`, `false`, `null`.

Types beyond JSON are written as strings:

| Type | Text |
|---|---|
| Date | `YYYY-MM-DD` |
| Datetime with an offset | Converted to UTC, then `YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00` |
| Datetime without an offset | `YYYY-MM-DDTHH:MM:SS[.ffffff]`, as it stands |
| Fractional seconds | Exactly six digits, and left out when zero |
| Exact decimal | Its text with scale and exponent kept: `1.50` and `1.5` differ |
| Bytes | Base64 (RFC 4648 §4), with padding |

A value of any other type is an error, and the record that holds it is turned down.

### 4.2 Row hash

`row_hash(payload)` is the 8-byte BLAKE2b digest of the canonical JSON of the payload, read as a big-endian unsigned integer.

The value `0` is reserved in snapshots to mean "this key has never reached a destination" (§6.4).

### 4.3 Record key

- **One key field:** the key is the value as text.
- **Several key fields:** the key is the canonical JSON of the list of values, in declared order, such as `["M1","RET"]`.
- A missing or null key field is an error, and the record is quarantined under the name `<missing>` or fails per `on_record_error`.

Portable key fields SHOULD be strings or integers. The text of other types in a single-field key is implementation-defined, so two implementations may disagree on it.

### 4.4 Stable id

`stable_id(part1, ..., partN)`:

1. For each part in order, feed the hash the part's UTF-8 byte length as 8 bytes big-endian, then its bytes.
2. Take the 64-byte BLAKE2b digest.
3. Encode it as Base32 (RFC 4648 §6, uppercase) and keep the first 26 characters.

A **version id** is `stable_id(run_id, dataset_name, partition_key)`, so a restarted extraction reuses its id and replaces what it left behind.

Other ids, such as run ids, SHOULD be ULIDs. They are compared only for equality, except that a journal MAY order by them.

### 4.5 Content hash

`content_hash` of a version:

1. Build a list with one `[key, op, row_hash]` for each record, and one `[key, "delete", previous_row_hash]` for each implied delete.
2. Sort it: by key in code points, then op, then hash.
3. Take the 16-byte BLAKE2b digest of the canonical JSON of the list, as lowercase hex.

`schema_hash` is opaque. An implementation MUST give equal schemas equal hashes, but it is not comparable between implementations.

### 4.6 Partition keys

The key of a partition is text, with parts joined by `/`.

| Scheme | Key | Default planning window at time T |
|---|---|---|
| `none` | `_` | `_` |
| `by_day(field, tz)` | `field=YYYY-MM-DD` | Yesterday and today in `tz` |
| `by_month(field, tz)` | `field=YYYY-MM` | Last month and this month in `tz` |
| `by_bucket(field, n)` | `bucket=I`, where I is the 8-byte BLAKE2b digest of the canonical JSON of the value, as a big-endian integer, modulo `n` | Every bucket |
| Field, as a text part of `by_fields` | `field=VALUE`, with `%` written `%25` and then `/` written `%2F` | None: the scheme can't list its partitions, so a planner is required |
| `by_fields(p1, ..., pN)` | Each part's key joined by `/`; a `by_fields` inside a `by_fields` is flattened | The cross product of the parts' windows, in order, or none if any part has none |

- `tz` is an IANA time zone name, and it is required: a day means nothing without one.
- A date field holds a date, a datetime or ISO-8601 text. A datetime with an offset is converted to `tz` before taking its calendar day. One without an offset is taken to be in `tz` already.
- A missing, null or unreadable partition field is an error for that record.
- `*` is not a partition. It appears only as the read request of a dataset read across partitions (§6.1).

**Labels.** Implementations SHOULD show partitions in business words: `Store 1509 · 18 Oct 2026`. Labels are for people and are not compared, though the vectors record the reference labels.

### 4.7 Durations and limits

- **Duration:** one or more runs of digits and a unit from `s`, `m`, `h`, `d`, matching `([0-9]+[smhd])+`. `1h30m` is 5400 seconds.
- **Limit:** `UNITS/PERIOD`, with UNITS an integer of at least 1 and PERIOD one of `second`, `minute`, `hour`, `day`.

### 4.8 Reserved names

Payload columns whose names begin with `_fc_` are reserved. A record that uses one is turned down.

| Column | Meaning |
|---|---|
| `_fc_key`, `_fc_op`, `_fc_row_hash` | Record identity, in spool chunks |
| `_fc_moved_from` | The earlier partition of a moved record, or null |
| `_fc_version`, `_fc_run`, `_fc_extracted_at`, `_fc_partition` | Added with `_fc_key` and `_fc_op` to rows on `append` routes |

`_fc_extracted_at` is the version's seal time, so it stays the same on a replay.

## 5. Invariants

A Runtime implementation MUST keep these for every flow, including across crashes and retries.

- **I1.** A sealed version is immutable.
- **I2.** A route applies the versions of one (dataset, partition) in seal order. Version n+1 is not applied until version n is applied, skipped or compensated.
- **I3.** A delivery (route, version) reaches `applied` at most once per replay. The ledger prevents re-application, and sinks are idempotent by key, so a crash between the sink call and the ledger write cannot duplicate effects.
- **I4.** Only complete versions imply deletes. Incomplete versions delete only through explicit delete records.
- **I5.** Extraction never waits for a route.
- **I6.** Every trigger, including a skipped tick, creates a run with a reason.
- **I7.** Records are keyed by business identity, and partitions are derived from record fields. A record whose partition changes is an update, not a new record.
- **I8.** Engine task arguments carry identifiers only. Data lives in the spool. Arguments SHOULD stay within 64 KB.
- **I9.** [Reserved] Every action by a person, an agent or the system is authorised, audited and attributed. A Runtime implementation MUST already attribute each delivery transition with an actor (§8).
- **I10.** Deletes implied by complete versions are decided after every slice of the run has been read. A key absent from one slice and present in another slice of the same run is a move, not a delete.

## 6. Change strategies

A strategy says how reads become versions.

| Strategy | Reads | Complete | Upserts | Deletes |
|---|---|---|---|---|
| `by_stamp(field, lookback, deleted_field?)` | Rows with a stamp at or after the last watermark less the lookback | No | New or changed `(key, row_hash)` | Only rows whose `deleted_field` is truthy |
| `snapshot_diff` | The whole partition | Yes | New or changed `(key, row_hash)` | Keys in the old snapshot and not the new |
| `replace` | The whole partition | Yes | Every key | Keys in the old snapshot and not the new |
| `push` | Events as they arrive | No | New or changed `(key, row_hash)` | Only explicit delete events |

Implementations MAY add custom strategies with their own names. A connector spec lists the names it serves.

### 6.1 Planning a read

A run reads, in order of preference: the partitions it was explicitly given, the dataset's planner, or the scheme's default window (§4.6). A scheme with no window and no planner is an error.

A `by_stamp` dataset reads once per run with the partition request `*`, whatever else was asked, and seals one version for each partition it touched.

### 6.2 Reads

- **Watermark.** A version's watermark is the greatest stamp among its records. Reads start from the dataset's greatest sealed watermark, less the lookback, and include that stamp.
- **Stamps.** A dataset's stamps MUST all be of one kind: dates, datetimes with offsets, datetimes without offsets, or numbers. ISO-8601 text counts as the datetime it spells, and without an offset it is taken as UTC. A record whose stamp is of another kind is turned down.
- **Duplicates.** If a key appears more than once in one read, the last occurrence wins.
- **No-op suppression.** A record whose `row_hash` matches the last known state is dropped from the version, so that lookback re-reads don't churn destinations. Known state is the record state when kept, and otherwise the row hashes of versions whose watermark falls within the lookback.
- **Empty read guard.** A complete read that would delete records and accepted no rows MUST fail with nothing sealed, unless the dataset sets `allow_empty`. A slice left empty only because its records moved to other slices is not an empty read.
- **Validation.** A record that fails the dataset's schema is quarantined and the version proceeds, unless `on_record_error` is `fail`, which fails the read.

### 6.3 Sealing

Sealing assigns the next `seal_seq`, which is unique and increasing, and creates one `pending` delivery per route, in one atomic step. Sealing a sealed version again changes nothing.

A delivery is created for each route of the flow when its version is sealed. If a worker dies before submitting the apply task, a sweeper MUST find the pending delivery and submit it (the outbox pattern).

### 6.4 Multi-slice reads (I10)

A complete dataset read in more than one slice in one run:

1. Each slice **stages** its version and its delete candidates: keys in that partition's previous snapshot and absent from the new one.
2. After the last slice, one task compares the candidates with every new snapshot of the run. A candidate found in another slice is a move: it is dropped as a delete, and the upsert in its new partition records `moved_from`.
3. All of the run's versions are sealed in one atomic step.

A multi-slice run therefore delivers nothing until every slice has been read. A failed slice keeps the run open until it is retried or abandoned.

A snapshot holds `(key, row_hash)` for one partition. A key that was read but turned down, and has no known previous hash, is held with hash `0`, so that a later good row upserts and a later absence deletes it without first having been delivered.

A complete dataset partitioned by anything other than `none` SHOULD keep record state, because a move cannot otherwise be recognised when the old partition is read alone.

### 6.5 Record state

A dataset with `keep_state` keeps the latest partition, row hash, payload and last version per key. It enables no-op suppression, move detection and supersede (§8.3).

## 7. Routes

| Mode | Semantics | Requirement |
|---|---|---|
| `upsert` | Insert or update by key; deletes delete | Keyed destination |
| `append` | Every record, deletes included, becomes a row with the columns of §4.8 | History tables |
| `replace_partition` | Atomically replace the destination's slice with the whole slice | Every dataset of the flow must be complete |
| `side_effect` | [Reserved] Business operations, idempotent by a key, with `compensate` to reverse | Sink implements both |

- **Delivered content.** A `replace_partition` route gets the whole slice, so an implementation MUST retain it when such a route exists. Every other mode gets the version's changes.
- **Replacing with gaps.** If a key that reached the destination was turned down at extraction, a replace would silently drop it, so the delivery MUST fail instead.
- **Emptying a slice.** A complete version with no records, on a `replace_partition` route, MUST still send one empty batch with `is_first` and `is_last` set, so the sink empties the slice.
- **Transforms.** A record transform takes a record and returns one payload, several or none (a filter). A batch transform takes and returns a table, and MUST keep the key column. Transforms MUST be deterministic functions of the record, the configuration and versioned reference data. Side effects belong in sinks. Deletes skip the transform.
- **Transform failures.** A failing record transform quarantines that record, except on `replace_partition`, where a transform failure MUST fail the delivery before the first batch is sent. A failing batch transform fails the delivery, since nothing ties the failure to one record.
- **Priority.** `low`, `normal` or `high`, for shared budgets (§10).

## 8. Delivery state machine

The ledger keeps one row per (route, version). Each transition MUST be recorded with its actor, reason, error, row counts and code version.

| From | Allowed to |
|---|---|
| `pending` | `running` |
| `running` | `applied`, `pending` (retryable error), `failed`, `held` |
| `held` | `pending` (approved), `skipped` (rejected) |
| `failed` | `pending` (retry), `skipped` |
| `applied` | `pending` (replay), `compensated` |
| `skipped` | None: final |
| `compensated` | None: final |

Any other transition MUST be refused.

**Closed** means `applied`, `skipped` or `compensated`. A closed delivery no longer holds up the next version of its stream. `failed` and `held` do, which is deliberate: applying later versions on top of a failed one would be unsafe.

### 8.1 Failure handling

| Failure | Handling |
|---|---|
| Transient (timeout, rate limit, 5xx) | The delivery returns to `pending` and is retried with backoff, honouring any delay the destination asked for |
| Tries used up | The error becomes permanent |
| Permanent, version-level (bad credentials, missing table) | `failed`, which blocks that partition's stream until an operator retries or skips it |
| Record-level (one rejected record) | Quarantined; the version continues; the stream isn't blocked |
| Budget exhausted | A wait, never a failure (§10) |

If `on_record_error` is `fail`, a rejected record makes the delivery `failed`. On `replace_partition`, a sink's rejection fails the delivery because the destination may hold a partial slice.

### 8.2 Operator actions

- **Retry** moves `failed` to `pending`. It restarts the delivery's count of tries.
- **Skip** moves `failed` or `held` to `skipped`, with a reason.
- **Replay** moves `applied` to `pending`. It MUST be refused while a newer delivery of the same route and partition is open, and it gives the delivery a new sink-level idempotency key, so it reaches the sink again.

### 8.3 Supersede

For a dataset with `keep_state`, when a delivery applies, open quarantine entries on that route from older versions of the same dataset, for keys that this version delivered, MUST close as `superseded`. A key rejected again by this version stays open.

## 9. Tasks and the engine contract

### 9.1 Tasks

Work runs as durable tasks. These are the names and keys for interchange. `{tenant}` is `default` for a single-tenant system.

| Task | Idempotency key | Concurrency key |
|---|---|---|
| `feather.plan` | `plan:{run_id}` | `plan:{tenant}:{flow}` |
| `feather.extract` | `extract:{run_id}:{dataset}:{partition}` | `extract:{tenant}:{dataset}:{partition}` |
| `feather.seal_run` | `seal:{run_id}:{dataset}` | `seal:{tenant}:{dataset}` |
| `feather.apply` | `apply:{route}:{version_id}:{replays}:{retries}` | `apply:{tenant}:{route}:{partition}` |

`replays` and `retries` count the replays and operator retries of the delivery. Tasks with the same idempotency key run once. Tasks with the same concurrency key run one at a time.

A batch handed to a sink carries the idempotency key `{route}:{version_id}:{chunk_no}:{replays}`.

### 9.2 Engine

An engine MUST provide:

- **Submit** a call with a task name, arguments, an idempotency key, an optional concurrency key, labels, a priority and a retry policy; it returns a reference.
- **Cancel** by reference, **signal** by reference and key, **recover** a dead worker's tasks, and report **health**.
- For a task body: a **step** that saves its result, so that a retry or a replay returns the saved result without running again; a durable **sleep**; and a durable **wait for a signal** with a timeout.

A task's age says nothing about whether it is stuck, because long waits are durable sleeps, so the contract has no wall-clock task timeout. Connector calls have their own timeouts.

### 9.3 Retry policy

Defaults: `max_attempts` 6, `first_delay` 10 s, `max_delay` 10 min.

After attempt N fails (the first attempt is 1), the wait W is `min(first_delay × 2^(N−1), max_delay)`. It is then cut by equal jitter to a value from W/2 to W.

### 9.4 Runs

A run's status is one of `queued`, `running`, `succeeded`, `failed`, `cancelled` or `skipped`. A run ends when none of its tasks is running. It fails with the first failure among them.

Triggers are `cron(expr, tz)`, `every(duration)`, `manual` and `on_event(dataset)`, which requires a `push` dataset. A cron trigger MUST name a time zone. Flows declare `overlap` as `queue` (the default), `skip` or `cancel_previous`, and `catch_up` as `latest` (the default; one run covers the gap) or `each`.

## 10. Rate budgets

A budget is one or more windows and an optional `reserve`, such as `{"high": 0.6}`.

- **Refill.** Each window is a token bucket that starts full and refills continuously at `units / seconds`, up to `units`.
- **Acquire is all or nothing.** A request takes its units from every window, or from none.
- **Priorities.** `low` requests may not take a window below `units × reserve.high`. `normal` and `high` requests may use the whole window.
- **Refusal.** A refused request is told the wait until the same request would be granted: the largest, across windows, of `(units_requested + held_back − tokens) × seconds / window_units`, rounded up to a whole microsecond.
- **Impossible requests.** A request for more than a window can ever give that priority MUST be an error, not a wait.
- **Exhaustion is a wait.** A wait under 60 seconds MAY sleep in-process. A longer one MUST be a durable sleep, and the work shows as deferred, not failed.

A budget is attached to a connection, so every flow that uses that connection shares it. Connectors declare a cost per call type.

## 11. Connector contract

### 11.1 Connector spec

A connector declares a spec ([schema](schemas/connector-spec.schema.json)): its `name`, the `strategies` it can serve, the route `modes` it can apply, how it reports `deletes`, a `config` schema and the budget `cost` of each kind of call. Tools check flows against specs without running anything (§12).

### 11.2 Sources

A pull source provides:

- `check` and `discover`;
- `read(request)`, which yields batches of records for a partition, or for `*`.

The request carries the partition, the scheme, a `since` and `stamp_field` for stamped reads, and a `resume` checkpoint. A source SHOULD use the scheme's bounds to narrow its read, but bounds may not cover every field. It MUST check each record against the partition and MUST NOT return records outside it, unless the request is `*`, because the runtime files what a partition read returns under that partition.

A read MAY save a checkpoint so that a restart resumes. Without one, a restart rewrites the version from the start under the same id.

A push source provides `verify(request)`, which raises on a bad signature, and `receive(request)`, which returns records and explicit deletes. The runtime derives keys and hashes.

### 11.3 Sinks

A sink provides:

- `check`;
- `apply(batch)`, which MUST be idempotent per key: applying the same batch twice changes nothing further;
- `compensate(batch)` [Reserved];
- `confirm(token)` [Reserved], for destinations that accept a batch and report the outcome later. A sink that returns `pending_confirmation` from an implementation without this support MUST cause a failed delivery, never an applied one.

A batch has the route, version id, chunk number, partition, mode, records, idempotency key, and `is_first` and `is_last` flags. A delete record has no payload. A multi-field key reaches a sink as its list of parts.

`apply` returns a count of applied records and any per-record rejections with an error class and a message.

### 11.4 Errors

Connectors classify failures as **transient**, with an optional retry delay, or **permanent**. An unclassified error is treated as transient until tries run out. A connector may also reject one record, which quarantines it.

## 12. Flow validation

An implementation that offers validation SHOULD report problems without opening any connection, in the form of [problem.schema.json](schemas/problem.schema.json), with these codes where they apply:

| Code | Severity | Meaning |
|---|---|---|
| `strategy_unsupported` | error | The source does not declare the dataset's strategy |
| `planner_required` | error | The scheme can't list its partitions and no planner is given |
| `mode_unsupported` | error | The sink does not declare the route's mode |
| `mode_needs_complete` | error | A `replace_partition` route reads an incomplete dataset |
| `route_name_duplicate` | error | Two routes of a flow share a name; a route takes its connection's name by default |
| `trigger_needs_push` | error | `on_event` names a dataset that isn't `push` |
| `moves_need_state` | warning | A partitioned, complete dataset doesn't keep record state |

## 13. Interchange documents and spool format

| Document | Schema |
|---|---|
| Flow description, without code | [flow.schema.json](schemas/flow.schema.json) |
| Connector spec | [connector-spec.schema.json](schemas/connector-spec.schema.json) |
| Version manifest | [version.schema.json](schemas/version.schema.json) |
| Delivery (ledger row) | [delivery.schema.json](schemas/delivery.schema.json) |
| Delivery attempt (one transition) | [attempt.schema.json](schemas/attempt.schema.json) |
| Validation problem | [problem.schema.json](schemas/problem.schema.json) |

`row_hash` is an unsigned 64-bit value, and in JSON documents it is an integer that some parsers can't hold exactly. Implementations MUST read such values without loss of precision.

**Spool chunks.** A spool chunk is a table holding the payload columns plus `_fc_key` (text), `_fc_op` (text), `_fc_row_hash` (uint64) and `_fc_moved_from` (nullable text). A snapshot chunk holds `_fc_key` and `_fc_row_hash`. When chunks are exchanged between implementations they SHOULD be Parquet files. A column whose values have no single type is held as JSON text and named in the schema metadata key `fc.json_columns`, as a JSON list.

## 14. Acceptance scenarios

A Runtime implementation MUST pass these. Each is stated for any implementation, and a reference test suite covers those marked ✓.

| ID | Scenario | Expected | Ref |
|---|---|---|---|
| S1 | A record moves partitions | No stale copy remains in partition-replace destinations | ✓ |
| S2 | `snapshot_diff` on a table with no change stamp | Inserts, updates and hard deletes are all detected | ✓ |
| S3 | The source rewrites a key in place | The business key keeps exactly one current row | ✓ |
| S4 | Late-visible rows inside the lookback | Captured once, without churn | ✓ |
| S5 | The destination is down during the read window | The extract completes, and deliveries catch up afterwards | ✓ |
| S6 | Two flows of different priority share a budget | The cap is never exceeded, and the higher priority keeps its reserve | ✓ |
| S7 | A long-window cap is exhausted | Work is deferred to the next window and is visible, not failed | ✓ |
| S8 | A quarantined record is overtaken by a newer version of its key | Resolved as superseded | ✓ |
| S9 | A complete source returns no rows by mistake | The read fails, and nothing is deleted | ✓ |
| S10 | A worker is killed in the middle of applying | Nothing is lost or duplicated | Planned |
| S11 | New code is deployed during a backlog | No lost or duplicated deliveries | Planned |
| S12 | A burst of events at ten times the normal rate | No loss, and latency recovers | Planned |

## 15. Reserved for later versions

These names are set aside. Their definitions are expected in later versions, and nothing in this version requires them.

| Feature | Names |
|---|---|
| Periodic reconcile of incremental datasets | `reconcile` task |
| Readiness groups and consistent cuts | `complete_through`, `on_ready` |
| Approvals and holds | The `held` status's entry conditions, approval policies |
| Schema evolution policy | `schema_change` holds |
| Side effects and reversal | `side_effect` mode, `compensate` task, `compensated` status |
| Accept-then-confirm sinks | `confirm`, `pending_confirmation` |
| Runtime overrides, tenancy, access control and audit | I9 and the `{tenant}` part of concurrency keys |
| Remote agents for private networks | The `remote` task |
| Operator JSON API and agent tools | No names reserved yet |

## 16. Versioning of this specification

This specification uses semantic versioning. While the major version is 0, any minor release MAY change the model. From 1.0 on, a change to a §4 encoding is a major change, because stored values depend on it, and new optional features are minor changes.

Every value in the conformance vectors is stable within a major version.

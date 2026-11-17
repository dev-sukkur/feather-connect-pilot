# connector-sdk Specification

## Purpose

The contracts a connector implements to read from or write to a system, what it declares about itself so flows can
be checked before they run, the services the runtime gives it, and the conformance kit that decides its maturity.

## Requirements

### Requirement: Source contract
A pull source SHALL name its connection and implement `check(ctx)`, which says whether the connection works,
`discover(ctx)`, which lists the streams it offers with their fields, and `read(ctx, request)`, which yields the
requested records in batches. A batch SHALL be an Arrow record batch or a list of dicts. The request SHALL carry the
partition (or `*` for all), the partition scheme, the stamp to read from and its field, and a checkpoint to resume
from.

*Pilot: verified*

#### Scenario: A read of one partition
- **WHEN** the runtime asks a source for the partition `date=2026-10-18`
- **THEN** only records of that partition are taken into the version

### Requirement: Sink contract
A sink SHALL implement `check(ctx)` and `apply(ctx, batch)`. A batch SHALL carry the route, version, chunk number,
partition, mode, records (key, operation, payload, and the partition a moved record came from), the chunk's
idempotency key, and whether the chunk is the first or last of its version. `apply` SHALL be idempotent per key. Its
result SHALL give the count applied and the records rejected, each with an error class and a message.

*Pilot: verified*

#### Scenario: The same batch twice
- **WHEN** a sink is given the same batch a second time
- **THEN** the destination is unchanged by the second call

#### Scenario: A composite key
- **WHEN** a sink receives a record of a dataset keyed by two fields
- **THEN** the record's key is a tuple of the two values

### Requirement: Push source contract
A push source SHALL implement `verify(ctx, request)`, which raises unless the request is genuine, and `receive(ctx,
request)`, which returns the records in the request, with explicit deletes marked. The request SHALL give the
method, path, lower-case headers, raw body and arrival time, and `ctx.now` SHALL be that arrival time. A push source
SHALL either name the secret it verifies against or state that it is unsigned; loading SHALL refuse one that does
neither.

*Pilot: verified*

#### Scenario: A push source with no stated check
- **WHEN** a project is loaded whose push source names no signature secret and does not say it is unsigned
- **THEN** loading fails and names the source

### Requirement: Connectors declare a spec
Every connector SHALL declare a spec with its name, the Pydantic model of its connection settings, the strategies a
source can serve, the modes a sink can apply, how it reports deletes, the budget units each kind of call costs, its
maturity, whether its reads are resumable, and which settings are secret. Settings typed as secret SHALL be secret
by default. The spec SHALL drive static validation, connection forms and the connector's description.

*Pilot: verified*

#### Scenario: Describing a connector
- **WHEN** `feather describe connector postgres.table --json` runs
- **THEN** the output gives its strategies or modes, how it reports deletes, its call costs, its maturity and each setting with its type and whether it is secret

### Requirement: Third-party connectors need no fork
Connectors SHALL be separate distributions that register through a Python entry point. A connector from another
publisher SHALL work without changing or forking the core, and type checkers SHALL be able to follow imports of
connector modules.

*Pilot: partial*

#### Scenario: Installing a connector
- **WHEN** a connector distribution is installed beside the core
- **THEN** `feather describe connectors` lists it and flows can use it

#### Scenario: Type checking
- **WHEN** a strict type check runs over a flow that imports a connector from another distribution
- **THEN** the import resolves to real types with no ignore comment

### Requirement: The context
Each call to a connector SHALL receive a context with the connection's settings, a logger, the run's time (the same
for every call of the run), the flow's time zone, the tenant, and, when the connection has them, a budget handle
and a metered HTTP client. The context SHALL offer `secret(name)`, `checkpoint(value)` for resumable reads, and
`answered(response)` to note a destination's answer as it arrives.

*Pilot: verified*

#### Scenario: A missing secret
- **WHEN** a connector asks the context for a secret that is not configured
- **THEN** it gets a permanent error that names the secret and not any value

### Requirement: The shared HTTP client
The runtime SHALL give HTTP connectors one client per connector instance that retries transient failures with
backoff, honours `Retry-After`, meters every call against the connection's budget at the cost the spec declares, and
redacts secrets and common token patterns from logs and errors. A `Retry-After` longer than 60 seconds SHALL raise a
transient error at once, so that the engine waits and not a worker thread.

*Pilot: verified*

#### Scenario: A long Retry-After
- **WHEN** an API answers 429 with `Retry-After: 300`
- **THEN** the call raises a transient error carrying 300 seconds, and no worker thread sleeps for it

#### Scenario: A token in an error message
- **WHEN** an API's error body quotes the bearer token that was sent
- **THEN** the stored and shown error has the token masked

### Requirement: HTTP authentication
The shared client SHALL support API-key, basic, bearer and OAuth2 (client credentials and refresh token)
authentication, with the scheme's settings at the connection's top level and only secrets under `auth`.

*Pilot: verified*

#### Scenario: A bearer connection
- **WHEN** a connection sets `auth_scheme: bearer` and gives its token under `auth`
- **THEN** every call through it carries the token in the `Authorization` header

### Requirement: OAuth2 tokens are shared across processes
An OAuth2 access token SHALL be fetched once per grant and shared by every process of the tenant that uses the
connection, so that vendors that limit token requests are not asked once per worker.

*Pilot: corrected*

#### Scenario: Two workers use one connection
- **WHEN** two worker processes make calls through the same OAuth2 connection within a token's lifetime
- **THEN** the vendor's token endpoint is called once

### Requirement: The conformance kit
The SDK SHALL ship a conformance kit that a connector's tests subclass. It SHALL check the spec's validity, `check`
and `discover`, pagination and resuming from a checkpoint, idempotent apply, delete handling, budget metering,
error classification as retryable or permanent, and bounded memory on large batches.

*Pilot: verified*

#### Scenario: A sink that is not idempotent
- **WHEN** the kit applies a batch twice to a sink that inserts blindly
- **THEN** the kit fails and says the second apply changed the destination

### Requirement: Maturity levels
A connector SHALL be `experimental` when it builds with basic tests, `beta` once its conformance tests pass, and
`stable` when it is beta, has run in production for at least 30 days, has complete documentation and has a named
maintainer.

*Pilot: verified*

#### Scenario: A connector's level is shown
- **WHEN** the connectors are listed
- **THEN** each shows its maturity

### Requirement: In-memory connectors
The core SHALL include an in-memory source and sink that support every strategy and mode, for tests and local runs.

*Pilot: verified*

#### Scenario: A test without infrastructure
- **WHEN** a flow test runs on the in-memory connectors
- **THEN** it needs no network, database or file

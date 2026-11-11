# deployment Specification

## Purpose

How an environment is run in production: two images, Docker Compose with Postgres as the only stateful service,
settings from the environment, forward-only migrations, and upgrades with no downtime.

## Requirements

### Requirement: An environment is a deployment
dev, qa, uat and prod SHALL each be a separate deployment of the same images, with its own database, secrets and
workers. Behaviour SHALL differ between them only through configuration.

*Pilot: verified*

#### Scenario: The same images everywhere
- **WHEN** qa and prod are deployed
- **THEN** both run the same image digests, and differ only in their settings

### Requirement: Compose topology
The system SHALL ship a Docker Compose file with Postgres, a one-shot migration job, the server and the worker. It
SHALL have at most four long-running containers, and Postgres SHALL be the only one with stored state. Compose
SHALL refuse to start when the database password or the session signing key is not set. The number of workers
SHALL be a setting, and workers SHALL share work without doing any of it twice.

*Pilot: built*

#### Scenario: A missing signing key
- **WHEN** Compose is started with no session signing key
- **THEN** it refuses to start, and says which setting is missing

#### Scenario: A restart
- **WHEN** the whole stack is stopped and started
- **THEN** every run, version and delivery is still there

### Requirement: Images
The system SHALL publish a server image and a worker image for linux/amd64 and linux/arm64. Neither image SHALL
contain a secret, a credential or a demo's token file. Every package SHALL run on the worker image's Python.

*Pilot: corrected*

#### Scenario: Scanning the images
- **WHEN** the built images are searched for secret files
- **THEN** none is found

### Requirement: Settings come from the environment
A process SHALL take its settings from environment variables: the environment's name, the database address, the
engine and its polling interval, the session signing key, the server's public address, the master key or the file
that holds it, the host database role's address, the log level, and the ingress limits. A setting that can't be
read SHALL stop the process at start-up with a message that names it.

*Pilot: verified*

#### Scenario: A bad number
- **WHEN** an ingress limit is set to `fast`
- **THEN** the server does not start, and says which setting is not a number

### Requirement: The server binds safely by default
`feather server` SHALL listen on the loopback address unless told otherwise. Listening on every address SHALL be an
explicit choice.

*Pilot: corrected*

#### Scenario: A plain start
- **WHEN** `feather server` starts with no host option
- **THEN** it is reachable from the same machine only

### Requirement: Migrations and upgrades
`feather server migrate` SHALL bring the schema up to date, and SHALL be safe to run more than once. An upgrade
SHALL be: run the migrations, restart the servers one at a time, then drain and restart the workers. Migrations
SHALL be written so that the old and the new code can both run against the schema during the upgrade.

*Pilot: built*

#### Scenario: A rolling upgrade
- **WHEN** a stack with flows in flight is upgraded to the next release
- **THEN** no run is lost and no delivery is applied twice

### Requirement: Administration from the server's command line
On the server's host, `feather server create-user`, `set-role` and `create-token` SHALL manage the first users and
tokens with no running server. A password SHALL be read from standard input, never from an argument.

*Pilot: verified*

#### Scenario: The first admin
- **WHEN** an installer runs `feather server create-user admin --role admin` and types a password
- **THEN** that user can sign in to the dashboard as an admin

### Requirement: Recovery
A restart of the server or of Postgres SHALL cost a running task at most one retry. With point-in-time recovery of
Postgres, one backup SHALL cover the catalogue, the ledger, the spool, installed packages and the engine's state.

*Pilot: partial*

#### Scenario: Postgres restarts
- **WHEN** Postgres is restarted while flows run
- **THEN** every flow resumes by itself, and nothing is lost or doubled

### Requirement: Quality gates
CI SHALL run, on every pull request: lint and format checks, a strict type check of the SDK, unit tests on Linux
and Windows, the tests that need Postgres, the dashboard tests in a real browser, engine conformance, and the image
and Compose tests. Chaos, hard cases and performance SHALL run nightly and on product changes. Test coverage SHALL
be at least 85% for the SDK core and 70% overall. No change SHALL merge while a required suite is failing or still
running.

*Pilot: partial*

#### Scenario: A red suite
- **WHEN** a pull request's chaos suite is failing
- **THEN** the pull request can't be merged

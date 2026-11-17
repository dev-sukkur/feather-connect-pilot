# local-dev Specification

## Purpose

The developer's loop on one machine: scaffold a project, check it, run it in process on simulated data, test it,
and bring up a whole environment with one command and watch it on the dashboard.

## Requirements

### Requirement: One command to a working environment
`feather dev` SHALL start Postgres in Docker, or use the database a setting names, run the migrations, start the
server and a worker, install the project as a package and activate its flows, print the dashboard's address and
the development user's password once, and open the dashboard. From a clean install, the demo flow SHALL be visible
on the dashboard within 5 minutes. Stopping it SHALL leave no process behind, and starting it again SHALL keep the
data.

*Pilot: verified*

#### Scenario: The first run
- **WHEN** `feather dev` runs on a machine with Docker and no earlier state
- **THEN** within 5 minutes the dashboard shows the demo flow running

#### Scenario: Stop and start
- **WHEN** `feather dev` is stopped with Ctrl+C and started again
- **THEN** no process of the first run is left, and the runs of the first run are still listed

### Requirement: Reload on save
`feather dev` SHALL install the project folder as the next development version on each save of a flow or
environment file, activate it, and keep the last three development versions. A save that does not load SHALL leave
the running version alone and SHALL print why. A reload SHALL leave no database connection behind.

*Pilot: verified*

#### Scenario: A syntax error is saved
- **WHEN** a flow file is saved with a syntax error
- **THEN** the running version keeps running, and the terminal shows the error

### Requirement: Clear failures at start-up
When `feather dev` can't start it SHALL say what to do, with no traceback: Docker not installed or not answering,
the dashboard's port taken (naming the port and the option that changes it), a database address that can't be
reached, or a project that does not load, which SHALL stop it before anything starts. With `--json`, a failure
SHALL still be one JSON document.

*Pilot: verified*

#### Scenario: Docker is not running
- **WHEN** `feather dev` runs while Docker is installed and not running
- **THEN** it says to start Docker, and exits with an error

### Requirement: Helper processes
A project SHALL be able to name helper processes for development in its configuration, each with a command and a
port. `feather dev` SHALL start them after the database is up and stop them with everything else, on every
supported operating system, even when `feather dev` itself is killed.

*Pilot: partial*

#### Scenario: `feather dev` is killed
- **WHEN** `feather dev` is killed with kill -9 on Linux, Windows or macOS
- **THEN** its helper processes exit

### Requirement: Scaffolding
`feather new project`, `feather new flow` and `feather new connector` SHALL write code, configuration and tests
that pass `feather validate` and `feather test` as written. Names SHALL be lower-case letters, digits, hyphens and
underscores.

*Pilot: verified*

#### Scenario: A new connector
- **WHEN** `feather new connector acme` runs and then `feather test`
- **THEN** the scaffolded connector's tests pass

### Requirement: Validate a project
`feather validate` SHALL check a project without running it or opening a connection: every flow's static problems,
that every connection a flow names is configured for the environment, that connection settings fit their
connector's model, that budgets can be read, and that a budget's account matches its connection. It SHALL exit
non-zero when there is an error, and with `--json` SHALL print each problem with its severity, code, message, path
and hint.

*Pilot: verified*

#### Scenario: A missing connection
- **WHEN** a flow names the connection `erp` and the `prod` environment does not configure it
- **THEN** `feather validate --env prod` reports an error that names the flow and the connection

### Requirement: Run a flow in process
`feather run <flow> --local` SHALL run a flow once in process, with no server and no database, and SHALL print what
each route applied. With `--sim` it SHALL run on simulation stand-ins of the flow's connections, with the project's
fixtures as data, for every kind of source, signed webhook sources included. `--partition` SHALL limit the run to
named partitions.

*Pilot: corrected*

#### Scenario: The quick start
- **WHEN** `feather run orders --project examples/demo --env qa --local --sim` runs
- **THEN** it prints the rows each of the three routes applied, and exits 0

#### Scenario: A flow fed by a signed webhook
- **WHEN** `feather run --local --sim` runs a flow whose dataset is fed by a signed webhook
- **THEN** the fixture events are taken as verified, and the flow runs

### Requirement: Test a project
`feather test` SHALL run the project's flow tests and the conformance tests of its own connectors.

*Pilot: verified*

#### Scenario: A project's tests
- **WHEN** `feather test` runs in a project
- **THEN** it runs the tests under `tests/` and exits non-zero if any fails

### Requirement: Describe what is installed
`feather describe connector <name>`, `connectors`, `flow <name>` and `dataset <name>` SHALL print what a connector
offers and what a flow or dataset declares, for a person and, with `--json`, for an AI assistant.

*Pilot: verified*

#### Scenario: An assistant asks about a connector
- **WHEN** `feather describe connector http.call --json` runs
- **THEN** the JSON gives the connector's settings, modes and call costs

### Requirement: Every command speaks JSON
Every command SHALL accept `--json`, and with it SHALL print exactly one JSON document on standard output, on
success and on failure alike, with everything else on standard error.

*Pilot: verified*

#### Scenario: A failure with `--json`
- **WHEN** a command fails with `--json`
- **THEN** standard output holds one JSON document with the error, and the exit code is non-zero

### Requirement: Development on three operating systems
Development SHALL work on Windows, macOS and Linux, and CI SHALL run the unit tests on Windows and Linux at least.

*Pilot: partial*

#### Scenario: CI
- **WHEN** a pull request is opened
- **THEN** lint, type checks and unit tests run on Linux and on Windows

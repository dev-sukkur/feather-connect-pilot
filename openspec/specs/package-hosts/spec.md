# package-hosts Specification

## Purpose

How installed packages run without restarting anything: workers are supervisors that start one host process per
active package version, flows are switched on, off and between versions per environment, and package code is kept
apart from the server and from the worker's secrets.

## Requirements

### Requirement: The server never loads package code
The server's scheduler, ingress, dashboard and API SHALL work only from the flow descriptions that hosts register in
the catalogue. No server process SHALL import or run a package's code.

*Pilot: verified*

#### Scenario: A flow is activated while the server runs
- **WHEN** a flow is activated at 10:00:05 with no process restarted
- **THEN** the scheduler fires its next tick on time

### Requirement: Workers are supervisors
A worker SHALL run a supervisor that imports no flow code. The supervisor SHALL watch for activation changes by
database notification, with a poll every 2 seconds as a fallback; start one host process per active package
version; stop a host whose version is no longer active; and heartbeat as a worker of its own with no slots, so that
it shows before any package is active. A host SHALL register as `<worker>/<package>@<version>`.

*Pilot: verified*

#### Scenario: Activation with no restart
- **WHEN** a flow of a ready version is activated
- **THEN** each supervisor has a host for that version running within 30 seconds, and no container restarts

### Requirement: A version is prepared before it can be activated
After an install, a supervisor SHALL prepare the version: build its dependency environment, load-check it there,
and check that it brings no webhook receiver as code. The version SHALL be `preparing` until it is `ready` or
`failed` with the error. Activating a version that is not ready SHALL be refused with 409. A failed preparation
SHALL be tried again with a growing wait.

*Pilot: verified*

#### Scenario: Activating too early
- **WHEN** a flow is activated while its version is still `preparing`
- **THEN** the call is answered 409 with the check `not_ready` and the version's state

### Requirement: One dependency environment per lock
A supervisor SHALL build each version's dependencies into a Python environment from the package's lock, from binary
wheels only, keyed by the lock's hash. Two versions with the same lock SHALL share one environment. Environments
that no installed version needs SHALL be removed, and the space they take on a worker SHALL be capped.

*Pilot: verified*

#### Scenario: Two packages, two versions of one library
- **WHEN** two active packages pin different versions of the same library
- **THEN** each runs with its own version

### Requirement: The load check runs in a throwaway process
The load check SHALL import the package in a child process with an empty environment, a temporary home directory and
a time limit, and SHALL kill the child's process tree when it ends. Unpacking a wheel SHALL accept only regular
files that land inside the target directory, at most 10,000 files and 200 MB. The check SHALL fail when the code and
the manifest disagree, or when the package imports something neither its lock nor the worker provides.

*Pilot: verified*

#### Scenario: A wheel with a path that escapes
- **WHEN** a wheel holds an entry named `../../etc/passwd`
- **THEN** unpacking is refused, and nothing is written outside the target directory

#### Scenario: Code that needs a variable at import
- **WHEN** a package reads an environment variable with no default as it is imported
- **THEN** the load check fails and shows the tail of the child's error output

### Requirement: Activate and deactivate per environment
Each flow of an installed version SHALL have a state in the environment: inactive, activating, active, deactivating
or failed. Activating SHALL make the scheduler fire the flow's triggers and ingress accept its webhooks.
Deactivating SHALL stop its triggers within 5 seconds and answer its webhooks 404, while runs in progress finish,
their deliveries and retries included, and its history stays visible. Deactivating is not pausing.

*Pilot: verified*

#### Scenario: Deactivate with a run in flight
- **WHEN** a flow is deactivated while one of its runs is delivering
- **THEN** no new run starts, and the run in flight finishes all of its deliveries

### Requirement: Switching versions loses and doubles nothing
Activating another version of an active flow SHALL start that version's hosts, wait until they say they are ready,
and only then move the flow's triggers to it in one transaction. The old version's host SHALL keep running while
runs tagged with its version are queued or running. Every task SHALL run on the code of the version its run is
tagged with. Two versions SHALL never read one partition at once. Rolling back SHALL
be activating the older version again.

*Pilot: verified*

#### Scenario: A host is killed during a switch
- **WHEN** a host is killed with kill -9 while its flow is switching from 1.4.0 to 1.5.0
- **THEN** no run is lost and no delivery is applied twice

#### Scenario: A new version fails to load
- **WHEN** version 1.5.0 is activated and its host fails to start
- **THEN** the flow stays on 1.4.0, and the activation shows the error

### Requirement: Hosts are supervised
A supervisor SHALL restart a host that crashes, with a growing wait of at most 60 seconds, and SHALL report a crash
loop, five crashes in five minutes, on the package's page with the tail of the host's log. A draining host that has
not exited after 90 seconds SHALL be killed. A host SHALL be a process tree of its own that dies with its
supervisor.

*Pilot: verified*

#### Scenario: The supervisor is killed
- **WHEN** a supervisor is killed with kill -9
- **THEN** none of its hosts is left running, and each of their tasks runs once after recovery

### Requirement: A host holds no secret of the worker's
A host's environment SHALL hold only what Python needs and the variables an operator explicitly passes through. It
SHALL never hold the master key, the session key or the worker's database address. A host SHALL ask its supervisor
for each connection's credentials over a pipe, and the supervisor SHALL give only those of connections the
package's manifest and configuration declare.

*Pilot: verified*

#### Scenario: A package asks for another's secret
- **WHEN** a host asks for the credentials of a connection its package does not declare
- **THEN** the supervisor refuses

### Requirement: A database role for hosts
Hosts SHALL connect to the database as a role of their own that can use the run, task, version and delivery tables
they need and cannot read the secrets, users or tokens tables. A supervisor with no such role configured SHALL
warn that its hosts connect as the worker.

*Pilot: verified*

#### Scenario: A host reads the tokens table
- **WHEN** package code on a host queries the tokens table
- **THEN** the database refuses it

### Requirement: Packaged datasets are fully operable
For a dataset that comes from a package, the dashboard and the API SHALL show partition labels in business words,
and SHALL offer partition verification, backfill and the write cost of a replay, by asking the package's host,
which holds the code.

*Pilot: corrected*

#### Scenario: Backfill of a packaged flow
- **WHEN** an operator backfills a packaged flow from the dashboard
- **THEN** the preview names the partitions by their labels, and confirming it starts the runs

### Requirement: Flow commands
The CLI SHALL offer `feather flow activate <package/flow> --version <v>` and `feather flow deactivate
<package/flow>`, each taking a reason. Each activation and deactivation SHALL write an audit record.

*Pilot: verified*

#### Scenario: Go live
- **WHEN** `feather flow activate acme-invoices/invoices --version 1.4.0 --env prod --reason "go live"` runs
- **THEN** the flow becomes active on 1.4.0, and the audit record holds the reason

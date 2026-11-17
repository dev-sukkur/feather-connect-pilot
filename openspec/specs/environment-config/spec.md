# environment-config Specification

## Purpose

Where configuration lives and how secrets are kept: a project's own files, the environment's shared connections,
budgets and settings in the database, and secrets that are either references or encrypted values that can be set
and never read back.

## Requirements

### Requirement: A project is configured by files
A project SHALL have a `feather.yaml` with its name, version, flow modules, settings (a time zone among them),
connections and budgets, and one overlay file per environment under `env/`. An overlay SHALL change only what it
names. The same package SHALL serve every environment, with behaviour differing only through configuration.

*Pilot: verified*

#### Scenario: An overlay changes one setting
- **WHEN** `env/dev.yaml` sets only the `base_url` of the connection `erp`
- **THEN** in dev the connection has that address, and every other setting comes from `feather.yaml`

### Requirement: An overlay can swap a connection for a simulation
An environment's overlay SHALL be able to give a connection another connector, so that dev and qa run flows on
simulation stand-ins of the real systems with no change to flow code.

*Pilot: verified*

#### Scenario: qa on stand-ins
- **WHEN** qa's overlay sets `connector: simulation.sink` for the connection `warehouse`
- **THEN** the flow's deliveries in qa go to the simulation, and the flow's code is unchanged

### Requirement: The environment has configuration of its own
Each environment SHALL keep connections, budgets and settings of its own in the database, outside any package and
shared by every package installed there. An environment-level entry SHALL override a package's entry of the same
name. Entries SHALL be editable from the dashboard, the API and the CLI, and
importable from an environment file. An entry SHALL be checked before it is kept, so that nothing
accepted can stop a server from starting.

*Pilot: verified*

#### Scenario: One connection, many packages
- **WHEN** the environment defines the connection `zakya` and two packages use a connection of that name
- **THEN** both use the environment's settings for it

#### Scenario: A bad entry
- **WHEN** an admin saves a budget whose limit can't be read
- **THEN** the save is refused with the reason, and the stored configuration is unchanged

### Requirement: Plain settings are visible and audited
A connection's settings that are not secret, such as an address, an organisation id or a client id, SHALL be stored
as plain values that anyone signed in may read. Every change SHALL be audited with the old and the new value.

*Pilot: verified*

#### Scenario: An address is changed
- **WHEN** an admin changes a connection's organisation id
- **THEN** the audit record holds the old and the new id

### Requirement: Secrets are references or encrypted values
A secret SHALL be given only as a reference: `${secret:env:NAME}`, `${secret:file:PATH}` or `${secret:db:NAME}`.
The fields a connector marks secret, and any key that looks like a credential, SHALL be accepted only as a
reference under the connection's `auth`. Secret values SHALL be resolved in the memory of the process that uses the
connection, and SHALL never be logged, shown, returned by the API or sent to a browser.

*Pilot: verified*

#### Scenario: A password written in plain text
- **WHEN** a connection's entry gives `password: hunter2` as a value
- **THEN** it is refused, and the message asks for a secret reference under `auth`

### Requirement: Encrypted secrets in the database
A `db` secret SHALL be sealed with AES-256-GCM under the environment's master key, with a fresh nonce each time and
the tenant, environment and name bound in, so that a ciphertext copied elsewhere does not open. The master key SHALL
come from outside the database and SHALL never be stored in it. A value SHALL be at most 64 KB. Without a master key
the server SHALL still start, and resolving a `db` secret SHALL fail with a message that says how to set one.

*Pilot: verified*

#### Scenario: A ciphertext is moved
- **WHEN** the stored ciphertext of one secret is copied over another's
- **THEN** opening it fails cleanly

### Requirement: Secrets are write-only
A secret SHALL be settable, replaceable, listable by name and deletable, and SHALL never be readable back. A page
or an API answer SHALL show only that it is set, when it was last changed and by whom. Changes SHALL be audited by
name only. The CLI SHALL read a secret's value from standard input or a file, never from an argument.

*Pilot: verified*

#### Scenario: Looking at a secret
- **WHEN** an admin opens a connection whose client secret is set
- **THEN** the page shows "set", with the time and the user, and no part of the value

### Requirement: A secret opens only for its own connection
A `db` secret named for a connection and field SHALL open only for that connection and field. Changing where a
connection goes, such as its base URL or host, SHALL clear its secrets and show them as needing re-entry, so that a
secret is never sent to a new place without being given again.

*Pilot: verified*

#### Scenario: A connection is pointed elsewhere
- **WHEN** an admin changes a connection's base URL
- **THEN** its stored secrets are cleared and the page asks for them again

### Requirement: Key rotation
An admin SHALL be able to rotate the master key with no downtime. A process SHALL hold a current key, which it seals
with, and previous keys, which it may still open with. Rotation SHALL seal every secret again under the new key in
one transaction. A process whose current key is not the active one SHALL open secrets and SHALL refuse to set them.
A wrong key SHALL fail cleanly, naming the key the secret was sealed with.

*Pilot: verified*

#### Scenario: Rotating
- **WHEN** every process is given the new key as a previous key, then as its current key, and `feather secret rotate-key` runs
- **THEN** every secret opens under the new key, and no connection failed during the change

### Requirement: Effective configuration shows its source
The dashboard SHALL show, for each connection, its effective settings and which layer set each one: the package's
file, the environment overlay, or the environment's own configuration.

*Pilot: built*

#### Scenario: An overridden address
- **WHEN** the environment overrides a package's `base_url`
- **THEN** the connection's page shows the environment's value and says the environment set it

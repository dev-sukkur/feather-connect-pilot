# flow-packages Specification

## Purpose

Flows ship as versioned packages, the only way to deploy them: built in the customer's own repository, published to
a registry built into the server, checked and installed into a running environment, promoted between environments
by checksum, and removed, all without a restart.

## Requirements

### Requirement: A flow package is a Python wheel
A flow package SHALL be a standard Python wheel that holds the project and a manifest. The manifest SHALL state the
package's name, version and import module, the product versions it works with, each flow with its datasets and
routes, the connections it needs, its dependencies, the hash of its lock, and the build time and commit. The wheel
SHALL register the manifest through an entry point. The name and version SHALL come from the project's packaging
metadata.

*Pilot: verified*

#### Scenario: A built wheel
- **WHEN** `feather package build` runs in a project
- **THEN** it writes one `py3-none-any` wheel to `dist/` and prints its sha256

#### Scenario: A manifest that disagrees with the code
- **WHEN** a wheel's manifest names a flow that its code does not define
- **THEN** the load check fails and names the difference

### Requirement: Builds are reproducible and locked
Building the same tree twice SHALL give byte-identical wheels. A build SHALL lock the package's dependencies with
hashes, so that an install resolves exactly what was tested, and SHALL warn when the project holds something that
looks like a secret value.

*Pilot: verified*

#### Scenario: Two builds
- **WHEN** a clean tree is built twice
- **THEN** the two wheels have the same sha256

### Requirement: A version means one set of bytes
A package version, once published or installed, SHALL never be replaced. A change SHALL need a new version number.

*Pilot: verified*

#### Scenario: Publishing a version again
- **WHEN** a version that exists in the registry is uploaded again, with the same or other bytes
- **THEN** the upload is refused

### Requirement: The built-in registry
The server SHALL serve a standard Python package index at `/registry/simple/`, with the standard upload API, so that
`uv publish`, `twine` and `uv pip install` work against it. It SHALL be on by default, and wheels SHALL be stored in
the database. Uploading SHALL need an admin's credentials or a publish-scoped token, and reading any valid token. An
admin SHALL be able to yank a version, and a yanked version SHALL NOT be installable.

*Pilot: verified*

#### Scenario: Standard tools
- **WHEN** a wheel is uploaded with `feather package publish` and fetched with a standard Python installer pointed at the index
- **THEN** the installer gets the same bytes

#### Scenario: A yanked version
- **WHEN** an admin yanks a version and someone then installs it
- **THEN** the install fails at the check `not_yanked`

### Requirement: Other registries
An admin SHALL be able to add, list, test and remove other package indexes that the environment trusts. An index's
address SHALL be HTTPS, with plain HTTP allowed only for a local host. Its credentials SHALL be given as a secret
reference, never a value.

*Pilot: verified*

#### Scenario: A plain-HTTP index
- **WHEN** an admin adds a registry at `http://pkgs.example.com/simple/`
- **THEN** it is refused, and the message asks for HTTPS

### Requirement: Install checks
Installing a package version SHALL run these named checks in order and stop at the first that fails:
`not_installed`, `fetch`, `size` (at most 50 MiB by default), `sha256`, `manifest`, `name_version`, `not_yanked`,
`signature`, `compatibility`, `lock` (every dependency pinned with a hash) and `wheels` (every dependency available
as a binary wheel). The server SHALL NOT run the package's code. A wheel SHALL also be installable from an uploaded
file, behind the same checks.

*Pilot: verified*

#### Scenario: An incompatible package
- **WHEN** a package that supports product versions `>=2` is installed on version 1
- **THEN** the install fails at `compatibility` and nothing is stored

#### Scenario: Installing the same version twice
- **WHEN** a version that is installed is installed again
- **THEN** the call is answered 409 at `not_installed`

### Requirement: An installed package is stored whole
An install SHALL store the wheel in the database and record the version with its sha256, registry, manifest, size,
lock hash, who installed it and when. Workers SHALL load this stored copy, so that a registry outage or a deleted
release never stops a running environment. Every flow of a newly installed version SHALL be inactive, and nothing
SHALL run until a flow is activated.

*Pilot: verified*

#### Scenario: The registry goes away
- **WHEN** a package is installed and its registry is then removed
- **THEN** its flows keep running and can still be activated

### Requirement: Names carry the package prefix
Every flow, dataset and route a package defines SHALL be named `<package>/<name>` in the environment, on every
surface. Inside its own code a package SHALL use short names, and the prefix SHALL be added when it is loaded.
Environment-level connections and budgets SHALL have no prefix, because they are shared on purpose.

*Pilot: verified*

#### Scenario: Two packages use one short name
- **WHEN** two packages each define a flow named `invoices`
- **THEN** they run side by side as `acme-invoices/invoices` and `partner-orders/invoices`

### Requirement: Packaged names are addressable without escaping
Every API route and page that names a packaged flow, dataset or route SHALL take the package and the short name as
two path segments, so that no link carries an escaped slash.

*Pilot: corrected*

#### Scenario: A link to a packaged flow
- **WHEN** the dashboard links to the flow `acme-invoices/invoices`
- **THEN** the link's path is `/flows/acme-invoices/invoices`

### Requirement: Promotion by checksum
`feather package promote <name> <version> --from <env> --to <env>` SHALL take the wheel out of one environment and
install it in another, refusing unless the sha256 is the same, so that what runs in the target is byte for byte
what was tested. There SHALL be no server-to-server path: the target SHALL need neither a token for the source nor
a network route to it.

*Pilot: verified*

#### Scenario: Promote qa to prod
- **WHEN** version 1.4.0 that passed in qa is promoted to prod
- **THEN** prod holds a wheel with the same sha256, installed and inactive

### Requirement: Optional signing
A wheel SHALL be signable with an ed25519 key, the signature covering the wheel's file record and kept inside the
wheel. An environment SHALL be able to list trusted public keys and to require signed packages. Where signing is
required, an unsigned package, or one signed by a key that is not trusted, SHALL fail the `signature` check.

*Pilot: verified*

#### Scenario: An unsigned package where signing is required
- **WHEN** an unsigned package is installed in an environment that requires signatures
- **THEN** the install fails at `signature`

### Requirement: Uninstall
An admin SHALL be able to uninstall a version only when none of its flows is active and no run of it is queued or
running. Uninstalling SHALL delete the stored wheel, unless another version shares it, and its dependency
environment once nothing needs it, and SHALL keep the version's row and its history. An uninstalled version SHALL
NOT be installable again.

*Pilot: verified*

#### Scenario: A version in use
- **WHEN** an admin uninstalls a version one of whose flows is active
- **THEN** the call is answered 409 with the check `in_use`

### Requirement: Package commands
The CLI SHALL offer `feather package build`, `publish`, `search`, `install`, `list`, `show`, `fetch`, `keygen`,
`promote` and `uninstall`, and `feather registry add`, `list`, `test` and `remove`. Each install, promotion,
uninstall, yank and registry change SHALL write an audit record with who, what and why.

*Pilot: verified*

#### Scenario: An install from the command line
- **WHEN** `feather package install acme-invoices==1.4.0 --env qa --json` runs
- **THEN** it prints each check with its outcome, as JSON

### Requirement: A package can't bring a webhook receiver as code
A package whose push dataset uses a source class of the package's own SHALL be refused, because the server verifies
every webhook and never loads package code. A package SHALL declare its webhooks instead.

*Pilot: verified*

#### Scenario: A home-made webhook source
- **WHEN** a package defines its own push source class and uses it for a dataset
- **THEN** preparing the version fails at `webhook_connector`, and names the dataset

# tenancy Specification

## Purpose

The light tenancy the first release ships with: every record carries a tenant from the start and one tenant,
`default`, is accepted, so that full multi-tenancy can be added later without changing the data model.

## Requirements

### Requirement: Every record carries a tenant
Every table SHALL have `tenant_id` in its primary key and in each of its unique indexes. A single-tenant install
SHALL use the implicit tenant `default`, and SHALL accept no other.

*Pilot: built*

#### Scenario: A new table
- **WHEN** a migration adds a table
- **THEN** a test fails unless `tenant_id` is in the table's primary key and unique indexes

#### Scenario: Another tenant in a URL
- **WHEN** a webhook arrives at `/ingress/acme/...` on an install whose only tenant is `default`
- **THEN** it is answered 404

### Requirement: All data access goes through a tenant-bound store
The store SHALL be bound to one tenant and SHALL filter every query by it. No query SHALL read across tenants.

*Pilot: built*

#### Scenario: A second tenant sees nothing
- **WHEN** a contract test writes runs, versions and deliveries as tenant A and reads as tenant B
- **THEN** tenant B's reads return nothing

### Requirement: Names are unique per tenant
Package, dataset, route and connection names SHALL be unique within a tenant, not globally.

*Pilot: built*

#### Scenario: Two tenants use one name
- **WHEN** two tenants each define a dataset named `invoices`
- **THEN** neither sees or affects the other's

### Requirement: Work carries its tenant
Run, task and event payloads SHALL name their tenant explicitly. Anything kept for a whole process, such as a cache,
an OAuth token or an engine queue, SHALL be keyed by tenant.

*Pilot: built*

#### Scenario: A token cache
- **WHEN** two tenants use connections with the same OAuth2 client
- **THEN** each tenant gets its own token

### Requirement: Roles are held per tenant and user
A user's role SHALL be stored per (tenant, user).

*Pilot: verified*

#### Scenario: A role is looked up
- **WHEN** a user's role is read
- **THEN** it is the role held in the store's tenant

### Requirement: A vendor account's budget may span tenants
A budget that names a vendor account SHALL share its windows with every budget, of any tenant, that names the same
account. These shared rows SHALL hold counters only, never data, and SHALL be the one sanctioned cross-tenant row.
Every budget naming an account SHALL define the same windows; a different definition SHALL be refused and SHALL
reset nothing.

*Pilot: verified*

#### Scenario: Two tenants share one Zakya organisation
- **WHEN** two tenants each define a budget for the account `zakya:60001`, with the same windows
- **THEN** a call by either takes from the same windows

#### Scenario: A mismatched definition
- **WHEN** a budget names an account whose windows another budget already defines differently
- **THEN** the definition is refused and the existing windows keep what has been used of them

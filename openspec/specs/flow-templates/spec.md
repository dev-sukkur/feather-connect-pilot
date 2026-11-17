# flow-templates Specification

## Purpose

Ready-made flows for well-known integrations, so that a new flow starts from something that already works: usable
as a library call, or copied into a project as ordinary code to edit freely.

## Requirements

### Requirement: Templates are library functions first
Each built-in flow SHALL be available as a library function that returns a flow, so that a project gets fixes by
upgrading. `feather new flow --template <name>` SHALL copy a template into the project, where it becomes ordinary
code the project owns.

*Pilot: verified*

#### Scenario: Using the library
- **WHEN** a flow module calls the template's function with its connections
- **THEN** it gets a working flow with no template code copied

#### Scenario: Taking full control
- **WHEN** `feather new flow orders --template webhook-to-api` runs
- **THEN** the project gains the flow's code, configuration, fixtures and tests as editable files

### Requirement: Generic templates
The system SHALL ship these templates: `db-to-warehouse` (a Postgres table to a DuckDB or MotherDuck bronze table,
by stamp or by snapshot diff), `api-to-warehouse` (a paginated REST API to a warehouse, within a budget),
`file-drop-to-db` (files in object storage to Postgres), `webhook-to-api` (a webhook, mapped, to an HTTP API) and
`fan-out-demo` (a JSON simulation to Postgres, DuckDB and an HTTP API).

*Pilot: verified*

#### Scenario: The fan-out demo
- **WHEN** the `fan-out-demo` template runs
- **THEN** every order reaches all three destinations

### Requirement: What every template ships with
Each template SHALL ship code, an overlay for every environment, simulation fixtures, tests and a README. It SHALL
validate for every environment, and its own tests SHALL pass.

*Pilot: verified*

#### Scenario: A template in CI
- **WHEN** CI runs
- **THEN** each template validates for dev, qa, uat and prod, and its tests pass

### Requirement: Copying a template keeps the project's files intact
Copying a template into a project SHALL add its connections and settings to the project's configuration and
environment files without losing the comments and order already there.

*Pilot: corrected*

#### Scenario: A commented configuration
- **WHEN** a template is copied into a project whose `feather.yaml` has comments
- **THEN** the comments are still there afterwards

### Requirement: A retail template for Zakya invoices
The system SHALL ship `zakya_invoices_to_erp`, which reads Zakya invoices within the organisation's shared budget
and posts each to an ERP's HTTP API once, and again only when it changes.

*Pilot: verified*

#### Scenario: The Zakya demo
- **WHEN** the demo runs twice against the Zakya simulator and one invoice changes between the runs
- **THEN** the receiver gets every invoice once, and the changed invoice a second time

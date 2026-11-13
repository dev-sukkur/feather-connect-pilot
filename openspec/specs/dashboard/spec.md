# dashboard Specification

## Purpose

The one place operators monitor and act: a server-rendered HTML dashboard with no front-end build, where a junior
operator can find a failure, understand it and recover from it without the command line or SQL.

## Requirements

### Requirement: Server-rendered HTML with no front-end stack
The dashboard SHALL be HTML rendered on the server from templates, with hand-written CSS and plain JavaScript
served by the server itself. There SHALL be no single-page-app framework, no TypeScript, no Node toolchain, no
build step and no content delivery network. Charts SHALL be rendered on the server as inline SVG and HTML.

*Pilot: verified*

#### Scenario: A page load
- **WHEN** any page is loaded
- **THEN** every script, style and image comes from the server's own address, and the console shows no error or policy violation

### Requirement: Read-only views work without JavaScript
Every page that only shows information SHALL work as a full page load with JavaScript turned off.

*Pilot: verified*

#### Scenario: No JavaScript
- **WHEN** a browser with JavaScript disabled opens the flows, runs and failures pages
- **THEN** each shows its content

### Requirement: The app shell
Every page SHALL share a shell: a left sidebar of icons with labels; a top bar with a command palette opened by
Ctrl+K, help, a theme switch and a user menu; and a bottom status bar that says how the server, the database and
the workers are, and names the environment and the version. The theme SHALL be dark or light, dark first, and a
user's choice SHALL be remembered. The layout SHALL follow a container-list style and carry no other product's
branding.

*Pilot: verified*

#### Scenario: The command palette
- **WHEN** a user presses Ctrl+K and types part of a flow's name, or a run's id
- **THEN** the palette lists the flow, or the run, and Enter opens it

#### Scenario: The status bar
- **WHEN** the database becomes unreachable
- **THEN** the status bar says so

### Requirement: One pattern for list pages
Every list page SHALL have summary tiles, with charts where they help; a toolbar with search, a column chooser and
an "only active" toggle; a dense table with select boxes, expandable rows, status dots, row actions and bulk
actions; and a footer that says how many items are shown. A user's column choices SHALL be remembered. Search SHALL
ask the server, so that it finds rows that are not on the page.

*Pilot: verified*

#### Scenario: A row is opened
- **WHEN** a user expands a flow's row
- **THEN** the row shows the flow's routes and latest runs without leaving the list

### Requirement: Pages
The dashboard SHALL have these pages: an overview; flows and a flow's detail with its graph; a partition grid; runs
and a run's detail with tasks, logs and records; deliveries, failures grouped by error per route, and a delivery's
detail; a version's detail; quarantine; record trace; connections and secrets; packages, a package's detail, a
packaged flow's page and registries; users; and a status page.

*Pilot: verified*

#### Scenario: Failures are grouped
- **WHEN** 200 deliveries of one route failed with the same error
- **THEN** the failures page shows one group of 200 with the error, and actions for the whole group

#### Scenario: The flow graph
- **WHEN** a user opens a flow that reads one dataset and delivers to three routes
- **THEN** the page draws the dataset, the three routes and their destinations, each with its status

### Requirement: Acting from the dashboard
From the pages, an operator SHALL be able to run, pause, resume and backfill a flow, re-run partitions, cancel or
re-run a run, retry, skip and replay deliveries singly or in bulk, dismiss quarantine entries, check a connection,
and activate, switch, roll back and deactivate a packaged flow. An admin SHALL also edit connections and secrets,
install and uninstall packages, and manage registries and users. A page SHALL say which role an action needs.

*Pilot: verified*

#### Scenario: Re-run from the dashboard
- **WHEN** an operator picks a partition on the grid and re-runs it
- **THEN** they see the preview, confirm it, and land on the new run

#### Scenario: Replay from the dashboard
- **WHEN** an operator replays a partition to a route
- **THEN** the destination receives the partition's latest version again

### Requirement: Safe actions
Bulk and destructive actions SHALL follow preview, then reason, then confirm. The result SHALL say what was done.
In a protected environment a skip SHALL require the reason.

*Pilot: verified*

#### Scenario: A bulk retry
- **WHEN** an operator selects a failure group and chooses Retry
- **THEN** the page shows how many deliveries and partitions it touches before anything is done

### Requirement: Recovery in five clicks
An operator SHALL be able to recover a failed delivery (find it, understand it, and retry or replay it) in at most
5 clicks from the overview, with no command line and no SQL.

*Pilot: built*

#### Scenario: From the overview to a retry
- **WHEN** an operator starts at the overview with one failing flow
- **THEN** they reach the failure, read its error and confirm a retry within 5 clicks

### Requirement: From a failure to its cause
From a failed delivery an operator SHALL be able to reach what was read: the version, the task that read it and
that task's records, and the trace of any one record by its business key.

*Pilot: verified*

#### Scenario: A rejected record
- **WHEN** an operator opens a delivery with one rejected record
- **THEN** one link opens that record's trace across versions and routes

### Requirement: Every view is a URL
Filters, searches and selections SHALL live in the query string, so that a link shared in chat opens the same view.
A link to a packaged flow, dataset or route SHALL carry no escaped slash.

*Pilot: corrected*

#### Scenario: A shared link
- **WHEN** an operator filters the runs to failed runs of one flow and copies the address
- **THEN** a colleague who opens it sees the same filtered list

### Requirement: Business words first
A partition SHALL be shown by its label, such as "Store 1509 · 18 Oct 2026", with the key available beside it.

*Pilot: partial*

#### Scenario: The partition grid
- **WHEN** an operator opens the grid of a dataset partitioned by store and day
- **THEN** rows read as stores and columns as days

### Requirement: Live pages
The overview, a running run and the lists that change SHALL refresh the parts that change without reloading the
page. A row opened after the session has ended SHALL send the user to sign in.

*Pilot: verified*

#### Scenario: Watching a run
- **WHEN** an operator has a running run open
- **THEN** its tasks change status on the page as they finish

### Requirement: Accessibility
Every page SHALL pass automated WCAG 2.1 AA checks with no violation, in the light and the dark theme. Every action
SHALL be doable with the keyboard alone, and a menu or dialog that closes SHALL give the focus back to what opened
it.

*Pilot: verified*

#### Scenario: A retry by keyboard
- **WHEN** an operator uses only the keyboard
- **THEN** they can find a failed delivery, preview a retry and confirm it

### Requirement: Narrow screens
Every page SHALL work at 360 pixels wide without scrolling sideways, with the sidebar opening on demand.

*Pilot: verified*

#### Scenario: A phone
- **WHEN** the failures page is opened on a screen 360 pixels wide
- **THEN** nothing needs sideways scrolling

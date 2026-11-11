# rate-budgets Specification

## Purpose

Shared rate limits for vendor APIs: one named budget that every flow and route using a connection draws from, with
several windows, a reserve for high-priority work, and waits in place of failures when it runs out.

## Requirements

### Requirement: Budgets have several windows
A budget SHALL be a name and a list of limits written as units per period, such as `90/minute` and `24000/day`, with
the periods second, minute, hour and day. Each window SHALL refill continuously, as a token bucket, by the store's
clock. A budget SHALL be used up when any of its windows is. A limit not of that form SHALL be refused when the
configuration is loaded.

*Pilot: verified*

#### Scenario: A badly written limit
- **WHEN** a budget lists the limit `90 per minute`
- **THEN** loading the configuration fails with a message showing the accepted form

### Requirement: Acquiring is all or nothing
A request for units SHALL take them from every window of the budget, or from none, atomically across processes.
The cap SHALL never be exceeded, however many workers draw from the budget at once.

*Pilot: verified*

#### Scenario: Many workers, one budget
- **WHEN** eight workers draw from a budget of 90 per minute as fast as they can
- **THEN** no minute sees more than 90 units granted

### Requirement: Budgets attach to connections
A connection SHALL name its budget in configuration, and every flow and route that uses the connection SHALL share
it without naming it in code. The shared HTTP client SHALL meter each call at the cost its connector declares for
that kind of call. A connector that doesn't use HTTP SHALL acquire units through the context.

*Pilot: verified*

#### Scenario: Two flows, one connection
- **WHEN** a reader flow and a writer flow both use the connection `zakya`, whose budget is `zoho-api`
- **THEN** the calls of both count against `zoho-api`

### Requirement: Priority reserve
A budget SHALL be able to reserve a share of every window for high-priority work. Low-priority requests SHALL use
only the rest; high-priority requests SHALL use all of it. A connection's work SHALL run at the priority of its
dataset or route, and a budgeted connection SHALL NOT be used at two priorities in one task.

*Pilot: verified*

#### Scenario: The reader keeps its share
- **WHEN** a budget reserves 60% for `high`, and a low-priority writer runs flat out beside a high-priority reader
- **THEN** the writer never takes more than 40% of any window
- **AND** the reader is never refused for want of the reserved share

### Requirement: Exhaustion is a wait, not a failure
A request the budget can't grant now SHALL wait. A wait under 60 seconds SHALL sleep in the process. A longer wait
SHALL become a durable sleep until the window refills, during which the task is shown as deferred with the budget's
name, and SHALL NOT use up a retry attempt. Reads competing for a budget SHALL only wait, however long the queue.

*Pilot: verified*

#### Scenario: The daily cap is reached
- **WHEN** a read needs more calls after the day's cap is used up
- **THEN** the task is deferred until the window refills, is shown as deferred, and then completes
- **AND** it is never marked failed

### Requirement: A request that can never fit fails with the reason
A request for more units than a window can ever hold, or than the share left to its priority can ever hold, SHALL
fail for good with a message that names the budget, and SHALL NOT wait for ever.

*Pilot: verified*

#### Scenario: One call costs more than the window
- **WHEN** a single acquire asks for 200 units of a budget whose largest window holds 90
- **THEN** it fails at once and names the budget

### Requirement: Usage can be read
The system SHALL report, per budget and window, how many units are in use now after refill, through the API.

*Pilot: verified*

#### Scenario: Reading usage
- **WHEN** `GET /api/v1/budgets/zoho-api/usage` is called after 30 of 90 units per minute were taken
- **THEN** the minute window reports about 30 in use, less what has refilled

### Requirement: Redefining a budget
Defining a budget again with the same windows SHALL change nothing. Defining it with other windows SHALL start it
afresh, full.

*Pilot: verified*

#### Scenario: A restart
- **WHEN** a server restarts and defines its budgets again, unchanged
- **THEN** each window keeps what had been used of it

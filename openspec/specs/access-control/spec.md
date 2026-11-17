# access-control Specification

## Purpose

Who may use the control plane and what each may do: local users and API tokens, browser sessions, the two roles the
first release ships, an audit record of every action, and the web security of the dashboard.

## Requirements

### Requirement: Local users
A person SHALL sign in with a username and password. Passwords SHALL be stored only as Argon2 hashes. A password
longer than 1,024 characters SHALL be refused before any hashing. The first user SHALL be created from the server's
own command line, with the password read from standard input.

*Pilot: verified*

#### Scenario: A good sign-in
- **WHEN** a user posts the right username and password to the sign-in page
- **THEN** they get a session and are sent to the page they asked for

#### Scenario: A bad sign-in
- **WHEN** the password is wrong
- **THEN** the answer is the same whether or not the user exists

### Requirement: Sign-in is throttled
After 5 failed sign-ins for one user from one address within 15 minutes, or 20 failed sign-ins from one address for
any users, the next attempt SHALL be refused without checking the password. The memory this takes SHALL be bounded.

*Pilot: verified*

#### Scenario: Guessing a password
- **WHEN** an address fails to sign in as one user five times in a minute
- **THEN** the sixth attempt is refused, the right password included

### Requirement: Sessions
A session SHALL be a signed cookie that is http-only, SameSite, secure when the server is reached over HTTPS, and
valid for 12 hours. The signing key SHALL come from the server's settings and SHALL be required to start.

*Pilot: verified*

#### Scenario: An old cookie
- **WHEN** a request carries a session cookie signed 13 hours ago
- **THEN** it is treated as signed out

### Requirement: API tokens
Automation SHALL authenticate with `Authorization: Bearer fc_…`. Only the SHA-256 of a token SHALL be stored, and a
token SHALL be shown once, when it is created. A token SHALL be revocable and MAY belong to a user. A request that
carries an `Authorization` header SHALL be judged by that header alone, never by a cookie it also has.

*Pilot: verified*

#### Scenario: A copy of the database
- **WHEN** someone reads the tokens table
- **THEN** nothing in it can be used to call the API

#### Scenario: A revoked token
- **WHEN** a revoked token is used
- **THEN** the call is answered 401

### Requirement: Two roles
Every user and token SHALL have the role `operator` or `admin`. An operator SHALL be able to run, re-run, backfill
and cancel, verify partitions, retry, skip and replay deliveries, dismiss quarantine entries, settle held events,
pause flows and routes, check connections, and activate and deactivate flows. An admin SHALL also manage users,
tokens, packages, registries, configuration and secrets. A token SHALL act with the lower of its own role and its
user's.

*Pilot: verified*

#### Scenario: An operator tries to install
- **WHEN** an operator asks to install a package
- **THEN** the call is answered 403 and says it needs the admin role

#### Scenario: A user is demoted
- **WHEN** an admin with an admin token is made an operator
- **THEN** the token acts as an operator from then on

### Requirement: Publish-only tokens
A token with the `publish` scope SHALL be able to upload new package versions to the built-in registry and to do
nothing else.

*Pilot: verified*

#### Scenario: A CI token tries to install
- **WHEN** a publish-scoped token calls the install endpoint
- **THEN** the call is answered 403, and the refusal is audited

### Requirement: Roles are checked in one place
Every action of the service layer SHALL name the capability it needs and SHALL check it before doing anything. No
page, API route or command SHALL check a role itself, so that the dashboard, the API and the command line are
judged alike. Every API route SHALL need an actor.

*Pilot: verified*

#### Scenario: The same refusal on every surface
- **WHEN** an operator tries an admin action from the dashboard, the API and the command line
- **THEN** each is refused in the same way

#### Scenario: No credentials
- **WHEN** any `/api/v1` route is called with no credentials
- **THEN** it is answered 401

### Requirement: Every action is audited
Every state-changing action by a person, a token or the system SHALL write an audit record with the time, the kind
of actor, the actor, the action, the target, the request, including its reason, and the result. A refused admin
action SHALL be audited too. Audit records SHALL be append-only, and secrets SHALL never be written to them.

*Pilot: verified*

#### Scenario: A skip with a reason
- **WHEN** an operator skips deliveries with the reason "duplicate of v41"
- **THEN** an audit record holds the operator, the action, the selection, the reason and how many were moved

#### Scenario: A secret is set
- **WHEN** an admin sets a secret
- **THEN** the audit record names the secret and holds nothing of its value

### Requirement: Cross-site request forgery
A state-changing request authenticated by cookie SHALL carry the session's CSRF token, as a form field or a header,
and SHALL be refused without it. A request authenticated by an `Authorization` header SHALL need none.

*Pilot: verified*

#### Scenario: A forged form
- **WHEN** another site posts a form to the dashboard using a signed-in user's cookie
- **THEN** the request is refused

### Requirement: Content security policy
Every page SHALL be served with a content security policy that allows scripts, styles and images only from the
server itself, allows no inline script or style, and forbids framing by other sites. Templates SHALL escape what
they render.

*Pilot: verified*

#### Scenario: A flow name with markup
- **WHEN** a flow's owner field holds `<script>alert(1)</script>`
- **THEN** the page shows it as text, and no script runs

### Requirement: Managing users and tokens
An admin SHALL be able to list users, create a user, change a user's role and create a token, from the dashboard,
the API and the server's command line. Creating a user that exists SHALL be refused.

*Pilot: verified*

#### Scenario: A duplicate user
- **WHEN** an admin creates a user whose name is taken
- **THEN** the call is answered 409

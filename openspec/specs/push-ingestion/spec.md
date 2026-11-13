# push-ingestion Specification

## Purpose

How events pushed by other systems become versions within seconds: a webhook endpoint that verifies and stores and
never waits on a destination, an inbox, and a sealer that turns what waits there into versions in small batches.

## Requirements

### Requirement: Ingress verifies, stores and answers
`POST /ingress/{tenant}/{package}/{webhook}` SHALL verify the request, store the raw event in the inbox and answer
with the webhook's acknowledgement status. It SHALL never call a destination or wait for one. It SHALL ask for
neither a session nor a CSRF token, because the sender's signature is what vouches for the request.

*Pilot: verified*

#### Scenario: A destination is slow
- **WHEN** a webhook's destination takes 30 seconds per call
- **THEN** the webhook is still answered within the ingress budget, and events queue as versions for that route

### Requirement: Checks go from cheap to dear
Ingress SHALL check, in order: that the webhook exists and is active (else 404); the caller's rate (429); the
declared size, then the size as read (413); the signature (401); and only then that the body is an event it can keep
(400). A failed verification SHALL answer the same whichever check failed. When the database is unavailable ingress
SHALL answer 503 with `Retry-After` within a bounded wait.

*Pilot: verified*

#### Scenario: A huge declared size
- **WHEN** a request declares a `Content-Length` of 10 GB
- **THEN** it is answered 413 before any of the body is read

#### Scenario: A database blip
- **WHEN** Postgres is down for 20 seconds while webhooks arrive
- **THEN** callers get 503 with `Retry-After`, no request hangs, and the first request after recovery is accepted

### Requirement: Unsigned callers can't use up a source's rate
Ingress SHALL keep two rate limits per webhook: one per caller address, taken before verification, and one for the
webhook as a whole, taken only by requests that verified. The memory the limits take SHALL be bounded.

*Pilot: verified*

#### Scenario: A flood of forged requests
- **WHEN** an address sends 10,000 requests a second that fail verification
- **THEN** genuine events from the real sender are still accepted

### Requirement: Built-in signature schemes
The server SHALL verify webhooks with built-in schemes only: HMAC-SHA256 and HMAC-SHA1 over the body, with a named
header, hex or base64 and an optional prefix; Shopify's; Stripe's, with its timestamp; a bearer token; and Zoho's,
marked as unverified. Any HMAC scheme SHALL be able to name a timestamp header held to a tolerance, 5 minutes by
default, against replays. Every comparison SHALL take constant time, and an empty secret SHALL never pass.

*Pilot: verified*

#### Scenario: A replayed request
- **WHEN** a correctly signed Stripe-style request is sent again ten minutes later
- **THEN** it is answered 401

#### Scenario: A signature in upper-case hex
- **WHEN** a sender signs in upper-case hex
- **THEN** the request verifies

### Requirement: Packages declare their webhooks
A package SHALL declare each webhook in its configuration: the push dataset whose code parses the events, how the
request is verified, the secret as a reference, the path in the body to the event's key, optionally the path to the
event's own time, and the status a kept event is answered with. The header that carries the signature or token
SHALL NOT be kept with the event.

*Pilot: verified*

#### Scenario: A declared webhook
- **WHEN** a package declares the webhook `partner-orders` for its dataset `orders`, and a flow of it is active
- **THEN** `POST /ingress/default/<package>/partner-orders` accepts correctly signed events
- **AND** the stored event does not hold the signature header

### Requirement: An event is kept once
Ingress SHALL keep each event once by its key: a repeat of a key already kept SHALL be acknowledged and not stored
again. A sender's own event id, when the source gives one, SHALL serve the same purpose. A record of each kept
event SHALL be retained for 30 days by default.

*Pilot: verified*

#### Scenario: A sender retries
- **WHEN** a sender delivers the same event three times
- **THEN** the destination is updated once

### Requirement: The sealer batches events into versions
A sealer SHALL turn each push dataset's inbox into versions. A batch SHALL be due when its oldest event has waited 1
second, or when it holds 500 events or 64 MB of bodies, all configurable. Each partition a batch touches SHALL get
one incomplete version, named after its first event. In one transaction the sealer SHALL mark the events sealed,
seal the versions and open their deliveries; it SHALL then submit one apply task per route and version.

*Pilot: verified*

#### Scenario: Two sealers, or a crash and a rerun
- **WHEN** a batch is sealed twice, by two processes or after a crash
- **THEN** each event ends in exactly one version

#### Scenario: A large backlog
- **WHEN** 100,000 events wait in an inbox
- **THEN** the sealer's memory stays bounded by its batch limits, and other datasets still get their turn

### Requirement: Per-key order is kept
Events of one key SHALL reach each route in the order they arrived. All the records of a batch SHALL stay in the
version in order, several of one key included: a keyed route ends with the last, and an append route gets each.
An event that would put a key into a version earlier than its previous record's SHALL start the next batch, so
that the key's last write wins.

*Pilot: verified*

#### Scenario: A key changes partition twice in one batch
- **WHEN** an order moves from bucket A to bucket B and back to A within one second
- **THEN** destinations end with the order's last state, in A

### Requirement: One bad event does not block the rest
A record that can't be stored SHALL be quarantined, and the rest of its event SHALL seal. An event that can't be
read SHALL be quarantined and held for an operator to reprocess or release, and the events after it SHALL seal. A
read that fails in a way that may pass SHALL be retried, with the events behind it waiting to keep their order.
Nesting deeper than 32 levels, and keys over 1,024 bytes, SHALL be refused before storing.

*Pilot: verified*

#### Scenario: A malformed event among good ones
- **WHEN** one event of fifty can't be parsed
- **THEN** forty-nine are delivered, and the one is held with its reason for an operator

#### Scenario: A parser that fails once
- **WHEN** parsing an event raises a transient error and then succeeds
- **THEN** the event is sealed on a later tick, in its original order

### Requirement: Push datasets are sealed where their code is
The server SHALL verify and keep an event; the host of the package's active version SHALL parse it and seal the
dataset's inbox, because only the package's code can parse its events.

*Pilot: verified*

#### Scenario: A webhook for a packaged flow
- **WHEN** an event arrives for a webhook a package declares
- **THEN** the server stores it without running package code, and the package's host seals it

### Requirement: Push latency
Under normal load, the time from a webhook's receipt to its destination being updated SHALL be at most 30 seconds
at the 95th percentile, with a target of 5 seconds, at 50 events a second on the small deployment, with no event
lost or repeated. Ingress SHALL answer within about 200 ms.

*Pilot: verified*

#### Scenario: The latency test
- **WHEN** 3,000 events are sent at 50 a second through a webhook to an HTTP destination
- **THEN** all 3,000 arrive once each, and the 95th percentile is at most 5 seconds

### Requirement: Bursts recover
A burst of ten times the normal rate SHALL lose no event, and the 95th-percentile latency SHALL return to normal
within 5 minutes of the burst ending.

*Pilot: verified*

#### Scenario: A tenfold burst
- **WHEN** events arrive at 500 a second for 30 seconds and then at 50 a second
- **THEN** every event is delivered once, and latency is back under the target within 5 minutes

### Requirement: Push work survives crashes
Killing the server in the middle of a seal, or a worker in the middle of applying a webhook's version, SHALL lose
and duplicate nothing. A crash after sealing and before record state is written SHALL be repaired at the next seal.

*Pilot: verified*

#### Scenario: The sealer dies between its steps
- **WHEN** the sealing process is killed after writing chunks and before the seal transaction
- **THEN** after restart each event is delivered once

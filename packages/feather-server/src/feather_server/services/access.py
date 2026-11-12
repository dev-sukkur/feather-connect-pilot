"""Who may do what: the roles `viewer`, `operator`, `developer` and `admin`, and the add-on grants `approver` and
`pii-reader` (design §13.2, M2 plan 2 O2).

Every action of the service layer names its capability and calls `require` before it does anything, so the dashboard,
the API, the remote CLI (through the API) and the server's own CLI are judged alike: no route checks a role itself. A
viewer sees, and does nothing. An operator runs, re-runs, retries, skips, replays and cancels. A developer also manages
the environment's connections, budgets, secrets and alert rules and deploys packages, in an environment that isn't
protected; in a protected one, those are an admin's. An admin also manages users, tokens and roles. Deciding a hold
needs the `approver` grant and unmasking PII the `pii-reader` grant, whatever the role, but an admin's, who can grant
them. Every refusal is audited.

A role is held per tenant and user (ADR-0013). A token's role is the lower of its own and its user's current one, so a
user who is demoted takes their tokens down with them (see `auth`).
"""

from collections.abc import Mapping
from typing import Literal

from feather_connect.storage.port import ROLES, AccessGrant, AuditStore, Role
from feather_server.auth import Actor

Capability = Literal[
    # operator: what an operator does in M1
    "runs.start",
    "runs.rerun",
    "runs.backfill",
    "runs.cancel",
    "partitions.verify",
    "deliveries.retry",
    "deliveries.skip",
    "deliveries.replay",
    "quarantine.dismiss",
    "events.reprocess",
    "events.release",
    "flows.pause",
    "routes.pause",
    "connections.check",
    "flows.activate",  # activating and deactivating a package's flow
    "readiness.fire",  # Fire now, for a period a flow waits for (readiness §8)
    "partitions.close",  # Close partition (readiness §4.1)
    "deliveries.compensate",  # reverse an applied delivery of a side-effect route (M2 plan 2 A2)
    "holds.decide",  # approve or reject a delivery held for approval (M2 plan 2 A1); the approver grant
    "pii.unmask",  # see a dataset's PII fields as they are (M2 plan 2 O4); the pii-reader grant
    "alerts.ack",  # acknowledge or snooze an alert (M2 plan 2 O6)
    # developer, in an environment that isn't protected
    "alerts.manage",  # alert rules and channels (M2 plan 2 O6)
    # admin
    "users.manage",
    "tokens.manage",
    "roles.manage",
    "packages.install",
    "packages.promote",
    "packages.uninstall",
    "registries.manage",
    "packages.publish",  # upload to the built-in registry: an admin, or a token with the publish scope
    "packages.yank",
    "environment.manage",  # the environment's own connections, budgets and settings (flow packages §8)
    "secrets.manage",  # the environment's encrypted secrets: set, replace, delete, rotate the key
]

NEEDS: Mapping[Capability, Role] = {
    "runs.start": "operator",
    "runs.rerun": "operator",
    "runs.backfill": "operator",
    "runs.cancel": "operator",
    "partitions.verify": "operator",
    "deliveries.retry": "operator",
    "deliveries.skip": "operator",
    "deliveries.replay": "operator",
    "quarantine.dismiss": "operator",
    "events.reprocess": "operator",
    "events.release": "operator",
    "flows.pause": "operator",
    "routes.pause": "operator",
    "connections.check": "operator",
    "flows.activate": "operator",
    "readiness.fire": "operator",
    "partitions.close": "operator",
    "holds.decide": "viewer",  # and the approver grant
    "pii.unmask": "viewer",  # and the pii-reader grant
    "deliveries.compensate": "operator",
    "alerts.ack": "operator",
    "alerts.manage": "developer",
    "users.manage": "admin",
    "tokens.manage": "admin",
    "roles.manage": "admin",
    "packages.install": "developer",
    "packages.promote": "developer",
    "packages.uninstall": "developer",
    "registries.manage": "developer",
    "packages.publish": "admin",  # a publish token's user must stay an admin (auth), so publishing stays an admin's
    "packages.yank": "developer",
    "environment.manage": "developer",
    "secrets.manage": "developer",
}
GRANT_NEEDS: Mapping[Capability, AccessGrant] = {"holds.decide": "approver", "pii.unmask": "pii-reader"}
PUBLISH_SCOPE: frozenset[Capability] = frozenset({"packages.publish"})  # all a publish-scoped token may do


class Forbidden(PermissionError):
    """The actor's role doesn't allow the action. The API answers 403; a page says what role it needs."""

    def __init__(self, capability: Capability, needed: Role, why: str | None = None) -> None:
        super().__init__(f"{capability}: {why}" if why else f"{capability} needs the {needed} role")
        self.capability, self.needed = capability, needed


def at_least(role: Role, needed: Role) -> bool:
    """Whether `role` is `needed` or higher."""
    return ROLES.index(role) <= ROLES.index(needed)


def lower(one: Role, other: Role) -> Role:
    """The lower of two roles."""
    return one if ROLES.index(one) >= ROLES.index(other) else other


def needed(actor: Actor, capability: Capability) -> Role:
    """The role the capability needs of the actor: a developer's needs an admin in a protected environment."""
    role = NEEDS[capability]
    return "admin" if role == "developer" and actor.protected else role


def can(actor: Actor, capability: Capability) -> bool:
    if actor.scope == "publish":
        return capability in PUBLISH_SCOPE
    if not at_least(actor.role, needed(actor, capability)):
        return False
    grant = GRANT_NEEDS.get(capability)
    return grant is None or actor.role == "admin" or grant in actor.grants


def require(actor: Actor, capability: Capability, audit: AuditStore, target: str | None = None) -> None:
    """Go on only if the actor may do `capability`; otherwise write an audit row of the refusal and raise
    `Forbidden`."""
    if can(actor, capability):
        return
    role = needed(actor, capability)
    grant = GRANT_NEEDS.get(capability)
    publish_only = actor.scope == "publish"
    if publish_only:
        why = "a publish token may only publish packages"
    elif not at_least(actor.role, role):
        why = f"needs the {role} role" + (" in a protected environment" if role != NEEDS[capability] else "")
    else:
        why = f"needs the {grant} grant"
    request = {"role": actor.role, **({"scope": actor.scope} if publish_only else {})}
    audit.record(actor.kind, actor.name, capability, target, request, f"refused: {why}")
    raise Forbidden(capability, role, None if why == f"needs the {role} role" else why)

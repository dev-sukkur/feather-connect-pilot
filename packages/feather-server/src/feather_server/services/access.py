"""Who may do what: the roles `admin` and `operator` (flow packages design §11), pulled forward from M2's RBAC.

Every action of the service layer names its capability and calls `require` before it does anything, so the dashboard,
the API, the remote CLI (through the API) and the server's own CLI are judged alike: no route checks a role itself. An
operator can do everything an M1 operator does; an admin can also manage users, tokens and roles, and the package and
registry actions that come with flow packages. A refusal of an admin action is audited.

A role is held per tenant and user (ADR-0013). A token's role is the lower of its own and its user's current one, so a
user who is demoted takes their tokens down with them (see `auth`).
"""

from collections.abc import Mapping
from typing import Literal

from feather_connect.storage.port import ROLES, AuditStore, Role
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
    "users.manage": "admin",
    "tokens.manage": "admin",
    "roles.manage": "admin",
    "packages.install": "admin",
    "packages.promote": "admin",
    "packages.uninstall": "admin",
    "registries.manage": "admin",
    "packages.publish": "admin",
    "packages.yank": "admin",
    "environment.manage": "admin",
    "secrets.manage": "admin",
}
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


def can(actor: Actor, capability: Capability) -> bool:
    if actor.scope == "publish":
        return capability in PUBLISH_SCOPE
    return at_least(actor.role, NEEDS[capability])


def require(actor: Actor, capability: Capability, audit: AuditStore, target: str | None = None) -> None:
    """Go on only if the actor may do `capability`; otherwise write an audit row of the refusal and raise
    `Forbidden`."""
    if can(actor, capability):
        return
    needed = NEEDS[capability]
    publish_only = actor.scope == "publish"
    why = "a publish token may only publish packages" if publish_only else f"needs the {needed} role"
    request = {"role": actor.role, **({"scope": actor.scope} if publish_only else {})}
    audit.record(actor.kind, actor.name, capability, target, request, f"refused: {why}")
    raise Forbidden(capability, needed, why if publish_only else None)

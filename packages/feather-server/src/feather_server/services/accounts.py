"""Users, their roles and API tokens: admin actions, for the dashboard, the API and the server's CLI alike.

Each call checks the actor may do it (`access.require`) and writes an audit row, whether it worked or not. A refusal for
want of the admin role is audited by `require`. The server's CLI acts as the `system` actor with the admin role, as it
works on the database directly. Everything here is of the store's tenant only (ADR-0013).
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from feather_connect.clock import Clock
from feather_connect.logs import redact
from feather_connect.storage.port import GRANTS, ROLES, AccessGrant, Role, Store, TokenScope, UserRow
from feather_server.auth import MAX_PASSWORD, Actor, hash_password, issue_token
from feather_server.services.access import Capability, at_least, require, require_reason

MAX_NAME = 200
CLI = Actor("system", "cli", "admin")  # the server's CLI, which works on the database directly


@dataclass(frozen=True)
class User:
    """A user as the dashboard and the API show one: never the password hash."""

    username: str
    role: Role
    created_at: str
    grants: tuple[str, ...] = ()

    @classmethod
    def of(cls, row: UserRow) -> "User":
        return cls(row.username, row.role, row.created_at.isoformat(), tuple(row.grants))


def _role(value: str) -> Role:
    for role in ROLES:
        if value == role:
            return role
    raise ValueError(f"{value!r} is not a role: it is {', '.join(ROLES[:-1])} or {ROLES[-1]}")


def _grant(value: str) -> AccessGrant:
    for grant in GRANTS:
        if value == grant:
            return grant
    raise ValueError(f"{value!r} is not a grant: it is {' or '.join(GRANTS)}")


def _scope(value: str) -> TokenScope:
    if value == "all":
        return "all"
    if value == "publish":
        return "publish"
    raise ValueError(f"{value!r} is not a token scope: it is all or publish")


def _name(value: str, what: str) -> str:
    if not value.strip() or len(value) > MAX_NAME:
        raise ValueError(f"a {what} has 1 to {MAX_NAME} characters")
    return value


class Accounts:
    """The users and tokens of one store's tenant."""

    def __init__(self, store: Store, clock: Clock) -> None:
        self._store = store
        self._clock = clock

    def _do[T](
        self,
        actor: Actor,
        capability: Capability,
        action: str,
        target: str,
        request: Mapping[str, Any],
        fn: Callable[[], T],
    ) -> T:
        require(actor, capability, self._store.audit, target)
        require_reason(actor, capability, request.get("reason"))
        try:
            result = fn()
        except Exception as error:
            self._store.audit.record(
                actor.kind, actor.name, action, target, request, f"failed: {redact(str(error))}"
            )
            raise
        self._store.audit.record(actor.kind, actor.name, action, target, request, "ok")
        return result

    def users(self, actor: Actor) -> list[User]:
        """Every user, by name, with their role. Needs the admin role, as it is part of managing users."""
        require(actor, "users.manage", self._store.audit, "users")
        return [User.of(row) for row in self._store.users.list_users()]

    def create_user(
        self,
        actor: Actor,
        username: str,
        password: str,
        role: str | None = None,
        *,
        reason: str | None = None,
    ) -> User:
        """Add a user. Without a `role`, the first user is an admin and any other an operator. Raises `ValueError` for
        a bad name, password or role and `DuplicateUser` for a name that is taken."""
        chosen: Role = _role(role) if role is not None else "admin" if self._first() else "operator"

        def create() -> User:
            _name(username, "username")
            if not password or len(password) > MAX_PASSWORD:
                raise ValueError(f"a password has 1 to {MAX_PASSWORD} characters")
            self._store.users.create_user(username, hash_password(password), chosen)
            found = self._store.users.get_user(username)
            assert found is not None
            return User.of(found)

        return self._do(
            actor,
            "users.manage",
            "create_user",
            f"user:{username}",
            {"role": chosen, "reason": reason},
            create,
        )

    def set_password(self, actor: Actor, username: str, password: str, *, reason: str | None = None) -> User:
        """Give the user a new password, as an admin does for someone who forgot theirs. The password is never in the
        audit record. Raises `KeyError` for an unknown user and `ValueError` for a bad password."""

        def change() -> User:
            if not password or len(password) > MAX_PASSWORD:
                raise ValueError(f"a password has 1 to {MAX_PASSWORD} characters")
            if self._store.users.get_user(username) is None:
                raise KeyError(username)
            self._store.users.set_password_hash(username, hash_password(password))
            found = self._store.users.get_user(username)
            assert found is not None
            return User.of(found)

        return self._do(actor, "users.manage", "set_password", f"user:{username}", {"reason": reason}, change)

    def _first(self) -> bool:
        return not self._store.users.list_users()

    def set_role(self, actor: Actor, username: str, role: str, *, reason: str | None = None) -> User:
        """Give the user another role; their tokens follow it at once. Raises `KeyError` for an unknown user and
        `ValueError` for a bad role, or for taking the role from the only admin, which would leave nobody to manage
        users."""
        found = self._store.users.get_user(username)
        before = None if found is None else found.role

        def change() -> User:
            wanted = _role(role)
            if found is None:
                raise KeyError(f"no user named {username!r}")
            admins = [u for u in self._store.users.list_users() if u.role == "admin"]
            if found.role == "admin" and wanted != "admin" and len(admins) == 1:
                raise ValueError(f"{username} is the only admin: make another user an admin first")
            self._store.users.set_role(username, wanted)
            return User(found.username, wanted, found.created_at.isoformat(), tuple(found.grants))

        return self._do(
            actor,
            "roles.manage",
            "set_role",
            f"user:{username}",
            {"from": before, "to": role, "reason": reason},
            change,
        )

    def set_grants(
        self, actor: Actor, username: str, grants: Sequence[str], *, reason: str | None = None
    ) -> User:
        """Give the user these add-on grants and no others: `approver` decides holds, `pii-reader` unmasks PII
        (design §13.2). Raises `KeyError` for an unknown user and `ValueError` for a grant there isn't."""
        found = self._store.users.get_user(username)
        before = None if found is None else list(found.grants)

        def change() -> User:
            wanted: list[AccessGrant] = [_grant(g) for g in grants]
            if found is None:
                raise KeyError(f"no user named {username!r}")
            self._store.users.set_grants(username, wanted)
            changed = self._store.users.get_user(username)
            assert changed is not None
            return User.of(changed)

        return self._do(
            actor,
            "roles.manage",
            "set_grants",
            f"user:{username}",
            {"from": before, "to": list(grants), "reason": reason},
            change,
        )

    def create_token(
        self,
        actor: Actor,
        name: str,
        ttl: timedelta | None,
        role: str | None = None,
        *,
        user: str | None = None,
        scope: str = "all",
        reason: str | None = None,
    ) -> str:
        """Create a token and return it, shown this once. It belongs to `user`, or else to the person acting, and acts
        with no higher a role than theirs: without a `role`, it takes theirs. A token of the CLI's with no user is for
        automation, an operator unless told otherwise. Raises `KeyError` for an unknown user and `ValueError` for a
        role higher than the user's. A token of the `publish` scope may only upload packages to the built-in registry,
        typically one per customer CI pipeline. In a protected environment a token is a `viewer` unless a role is
        asked for (least privilege, design §11)."""
        owner = user if user is not None else actor.user

        def create() -> str:
            _name(name, "token name")
            found = None if owner is None else self._store.users.get_user(owner)
            if owner is not None and found is None:
                raise KeyError(f"no user named {owner!r}")
            ceiling: Role = actor.role if found is None else found.role
            chosen = (
                _role(role)
                if role is not None
                else "viewer"
                if actor.protected
                else "operator"
                if found is None
                else found.role
            )
            if not at_least(ceiling, chosen):
                whose = f"its user {owner}" if owner else "yours"
                raise ValueError(f"a token can't have a higher role than {whose}, {ceiling}")
            return issue_token(
                self._store, name, ttl, role=chosen, created_by=owner, scope=_scope(scope), clock=self._clock
            )

        return self._do(
            actor,
            "tokens.manage",
            "create_token",
            f"token:{name}",
            {
                "role": role,
                "user": owner,
                "scope": scope,
                "ttl_s": None if ttl is None else int(ttl.total_seconds()),
                "reason": reason,
            },
            create,
        )

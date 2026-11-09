"""The environment's own configuration and secrets (flow packages §8, sponsor decisions of 2026-11-09), for the API, the
CLI and the Connections page alike.

Connections, budgets and settings are kept per environment in the database, outside any package, and shared by every
package installed there: an entry overrides a package's own of the same name. A connection's settings are plain values
(a base URL, an organisation id, a client id) that anyone may read, except the fields its connector marks secret,
which are given only as references under `auth` and shown as "set, updated … by …" for a `db` secret. Every change is
audited with the old and the new value; a value holds no secret.

Secrets are write-only (`feather_connect.vault`): set, replace, list by name and delete, never read back, audited by
name only.
"""

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import yaml

from feather_connect.clock import Clock
from feather_connect.core.connector import secret_fields
from feather_connect.core.registry import list_connectors
from feather_connect.project import ProjectError, _budgets, is_secret_ref  # pyright: ignore[reportPrivateUsage]
from feather_connect.storage.port import ENV_KINDS, EnvEntry, EnvKind, SealedSecret, Store
from feather_connect.vault import MasterKey, Vault, VaultError, check_name
from feather_server.auth import Actor
from feather_server.services.access import require

__all__ = ["ConfigProblem", "Environment", "entries_from_file", "secret_refs_of"]

_PLURAL: Mapping[str, EnvKind] = {"connections": "connection", "budgets": "budget", "settings": "setting"}
_CONNECTION_KEYS = frozenset({"connector", "site", "budget", "auth"})
_DB_REF = "${secret:db:"


class ConfigProblem(ValueError):
    """An entry that can't be kept as given. The message says which field and why, and holds no secret."""


def _secret_fields_of(connector: str) -> frozenset[str]:
    """The secret settings of a connection's connector: of every installed connector named `connector` or
    `connector.<kind>`."""
    found: set[str] = set()
    for info in list_connectors():
        if info.name == connector or info.name.startswith(f"{connector}."):
            found |= secret_fields(info.spec)
    return frozenset(found)


def secret_refs_of(value: Mapping[str, Any]) -> dict[str, str]:
    auth = value.get("auth")
    return {str(k): str(v) for k, v in cast(dict[str, Any], auth).items()} if isinstance(auth, dict) else {}


def check_entry(kind: EnvKind, name: str, value: Any) -> Any:
    """`value` checked for its kind; raises `ConfigProblem`."""
    if not name or len(name) > 200 or "/" in name:
        raise ConfigProblem(f"{name!r} is not a name: 1 to 200 characters, no '/'")
    if kind == "budget":
        try:
            _budgets({"budgets": {name: value}}, {})
        except ProjectError as error:
            raise ConfigProblem(str(error)) from None
        return value
    if kind == "setting":
        return value
    if not isinstance(value, dict):
        raise ConfigProblem(f"connection {name}: give a mapping with a connector")
    body = {str(k): v for k, v in cast(dict[str, Any], value).items()}
    connector = body.get("connector")
    if not isinstance(connector, str) or not connector:
        raise ConfigProblem(f"connection {name}: connector is required, such as zakya")
    secret = _secret_fields_of(connector)
    for key, field in body.items():
        if key in secret:
            raise ConfigProblem(
                f"connection {name}: {key} is secret: give it as a reference under auth, such as "
                f"auth.{key} = ${{secret:db:{name}-{key.replace('_', '-')}}}"
            )
        if key not in _CONNECTION_KEYS and is_secret_ref(field):
            raise ConfigProblem(f"connection {name}: {key}: a secret reference stands only under auth")
    for key, ref in secret_refs_of(body).items():
        if not is_secret_ref(ref):
            raise ConfigProblem(
                f"connection {name}: auth.{key} must be a reference such as ${{secret:db:NAME}}"
            )
    return body


def entries_from_file(path: Path) -> list[tuple[EnvKind, str, Any]]:
    """The connections, budgets and settings of an M1 environment file such as `env/prod.yaml`, to seed an
    environment's own configuration."""
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as error:
        raise ConfigProblem(f"{path.name}: can't read it ({error})") from None
    if not isinstance(loaded, dict):
        raise ConfigProblem(f"{path.name}: must be a mapping")
    found: list[tuple[EnvKind, str, Any]] = []
    for key, kind in _PLURAL.items():
        section = cast(dict[str, Any], loaded).get(key) or {}
        if not isinstance(section, dict):
            raise ConfigProblem(f"{path.name}: {key} must be a mapping")
        for n, v in cast(dict[str, Any], section).items():
            found.append((kind, str(n), v))
    return found


def _diff(old: Any, new: Any) -> dict[str, Any]:
    return {"old": old, "new": new}


class Environment:
    """The configuration and secrets of the environment `env`, for one store's tenant."""

    def __init__(self, store: Store, clock: Clock, env: str, key: MasterKey | None) -> None:
        self._store, self._clock, self._env = store, clock, env
        self.vault = Vault(store.secrets, env, key)

    @property
    def env(self) -> str:
        return self._env

    # Entries.

    def entries(self, kind: EnvKind | None = None) -> list[EnvEntry]:
        return self._store.env_config.entries(self._env, kind)

    def entry(self, kind: EnvKind, name: str) -> EnvEntry:
        found = self._store.env_config.entry(self._env, kind, name)
        if found is None:
            raise KeyError(f"environment {self._env} has no {kind} {name}")
        return found

    def put(
        self, actor: Actor, kind: EnvKind, name: str, value: Any, *, reason: str | None = None
    ) -> EnvEntry:
        """Set an entry, audited with its old and new value."""
        target = f"{kind}:{name}"
        require(actor, "environment.manage", self._store.audit, target)
        if kind not in ENV_KINDS:
            raise ConfigProblem(f"{kind!r} is not one of {', '.join(ENV_KINDS)}")
        checked = check_entry(kind, name, value)
        entry = EnvEntry(self._env, kind, name, checked, actor.name, self._clock.now())
        old = self._store.env_config.put(entry)
        request = {"env": self._env, "reason": reason, **_diff(None if old is None else old.value, checked)}
        self._store.audit.record(actor.kind, actor.name, f"set_{kind}", target, request, "ok")
        return entry

    def update(
        self, actor: Actor, name: str, changes: Mapping[str, Any], *, reason: str | None = None
    ) -> EnvEntry:
        """Change some settings of a connection: a None removes one."""
        current = self._store.env_config.entry(self._env, "connection", name)
        value: dict[str, Any] = dict(current.value) if current is not None else {}
        for key, field in changes.items():
            if key.startswith("auth."):
                auth = dict(secret_refs_of(value))
                if field is None:
                    auth.pop(key[5:], None)
                else:
                    auth[key[5:]] = field
                value["auth"] = auth
            elif field is None:
                value.pop(key, None)
            else:
                value[key] = field
        return self.put(actor, "connection", name, value, reason=reason)

    def remove(self, actor: Actor, kind: EnvKind, name: str, *, reason: str | None = None) -> None:
        target = f"{kind}:{name}"
        require(actor, "environment.manage", self._store.audit, target)
        old = self._store.env_config.remove(self._env, kind, name)
        if old is None:
            raise KeyError(f"environment {self._env} has no {kind} {name}")
        request = {"env": self._env, "reason": reason, **_diff(old.value, None)}
        self._store.audit.record(actor.kind, actor.name, f"remove_{kind}", target, request, "ok")

    def import_file(self, actor: Actor, path: Path, *, reason: str | None = None) -> list[EnvEntry]:
        """Seed the environment from an M1 file such as env/prod.yaml."""
        return self.import_entries(
            actor, entries_from_file(path), reason=reason or f"imported from {path.name}"
        )

    def import_entries(
        self, actor: Actor, found: Sequence[tuple[str, str, Any]], *, reason: str | None = None
    ) -> list[EnvEntry]:
        """Set many entries: every one checked first, so a bad one sets none."""
        require(actor, "environment.manage", self._store.audit, "environment")
        checked: list[tuple[EnvKind, str, Any]] = []
        for kind, name, value in found:
            if kind not in ENV_KINDS:
                raise ConfigProblem(f"{kind!r} is not one of {', '.join(ENV_KINDS)}")
            checked.append((cast(EnvKind, kind), name, check_entry(cast(EnvKind, kind), name, value)))
        return [
            self.put(actor, kind, name, value, reason=reason or "imported") for kind, name, value in checked
        ]

    # Views.

    def connection(self, name: str) -> dict[str, Any]:
        """A connection as the API and the page show it: its settings, and for each secret field its reference and,
        for a `db` secret, whether it is set and when and by whom."""
        entry = self.entry("connection", name)
        return self._view(entry)

    def connections(self) -> list[dict[str, Any]]:
        return [self._view(e) for e in self.entries("connection")]

    def _view(self, entry: EnvEntry) -> dict[str, Any]:
        value = cast(dict[str, Any], entry.value)
        sealed = {s.name: s for s in self.vault.names()}
        secrets: dict[str, Any] = {}
        for key, ref in secret_refs_of(value).items():
            shown: dict[str, Any] = {"ref": ref}
            if ref.startswith(_DB_REF) and ref.endswith("}"):
                found = sealed.get(ref[len(_DB_REF) : -1])
                shown |= (
                    {"set": True, "updated_at": found.updated_at.isoformat(), "updated_by": found.updated_by}
                    if found is not None
                    else {"set": False}
                )
            secrets[key] = shown
        return {
            "name": entry.name,
            "connector": value.get("connector"),
            "settings": {k: v for k, v in value.items() if k != "auth"},
            "secrets": secrets,
            "secret_fields": sorted(_secret_fields_of(str(value.get("connector") or ""))),
            "changed_by": entry.changed_by,
            "changed_at": entry.changed_at.isoformat(),
        }

    # Secrets.

    def set_secret(self, actor: Actor, name: str, value: str, *, reason: str | None = None) -> bool:
        target = f"secret:{name}"
        require(actor, "secrets.manage", self._store.audit, target)
        replaced = self.vault.set(name, value, actor.name, self._clock.now())
        request = {"env": self._env, "name": name, "replaced": replaced, "reason": reason}
        self._store.audit.record(actor.kind, actor.name, "set_secret", target, request, "ok")
        return replaced

    def secrets(self) -> list[dict[str, Any]]:
        return [_secret_row(s) for s in self.vault.names()]

    def delete_secret(self, actor: Actor, name: str, *, reason: str | None = None) -> None:
        target = f"secret:{name}"
        require(actor, "secrets.manage", self._store.audit, target)
        if not self.vault.delete(check_name(name)):
            raise KeyError(f"secret {name} is not set in environment {self._env}")
        self._store.audit.record(
            actor.kind,
            actor.name,
            "delete_secret",
            target,
            {"env": self._env, "name": name, "reason": reason},
            "ok",
        )

    def rotate_key(self, actor: Actor, new: MasterKey, *, reason: str | None = None) -> int:
        require(actor, "secrets.manage", self._store.audit, "secrets")
        try:
            count = self.vault.rotate(new, actor.name, self._clock.now())
        except VaultError as error:
            self._store.audit.record(
                actor.kind, actor.name, "rotate_master_key", "secrets", {"env": self._env}, f"failed: {error}"
            )
            raise
        request = {"env": self._env, "secrets": count, "key_id": new.key_id, "reason": reason}
        self._store.audit.record(actor.kind, actor.name, "rotate_master_key", "secrets", request, "ok")
        return count

    def open(self, name: str) -> str:
        """For `${secret:db:<name>}` when a connection is used, and nothing else."""
        return self.vault.open(name)

    def now(self) -> datetime:
        return self._clock.now()


def _secret_row(secret: SealedSecret) -> dict[str, Any]:
    return {
        "name": secret.name,
        "updated_at": secret.updated_at.isoformat(),
        "updated_by": secret.updated_by,
        "key_id": secret.key_id,
    }

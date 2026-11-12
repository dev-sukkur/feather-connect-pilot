"""The environment's own configuration and secrets (flow packages §8, sponsor decisions of 2026-11-09), for the API, the
CLI and the Connections page alike.

Connections, budgets and settings are kept per environment in the database, outside any package, and shared by every
package installed there: an entry overrides a package's own of the same name. A connection's settings are plain values
(a base URL, an organisation id, a client id) that anyone may read, except the fields its connector marks secret and
any key that looks like a credential, which are given only as `${secret:db:<connection>/<field>}` under `auth` and
shown as "set, updated … by …". Every change is audited with the old and the new value; a value holds no secret. An
entry is checked as `load_project` reads it before it is kept, so that nothing accepted can stop a server starting.

A `db` secret opens only for the connection and field it is named for, so an admin can't point a connection at
another's secret; and changing where a connection goes (its base URL or host) clears its secrets, which then show as
"needs re-entry", so that a secret is never sent to a new place without being given again.

Secrets are write-only (`feather_connect.vault`): set, replace, list by name and delete, never read back, audited by
name only.
"""

import typing
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import yaml

from feather_connect.clock import Clock
from feather_connect.core.connector import secret_fields
from feather_connect.core.registry import ConnectorInfo, list_connectors
from feather_connect.project import (
    ProjectError,
    check_environment,
    db_secret_name,
    environment_layer,
    is_secret_ref,
)
from feather_connect.storage.port import ENV_KINDS, EnvEntry, EnvKind, SealedSecret, Store
from feather_connect.vault import Vault, VaultError, check_name
from feather_server.auth import Actor
from feather_server.services.access import require, require_reason

__all__ = [
    "ENDPOINT_KEYS",
    "ConfigProblem",
    "Environment",
    "coerce_setting",
    "entries_from_file",
    "secret_refs_of",
]

_PLURAL: Mapping[str, EnvKind] = {"connections": "connection", "budgets": "budget", "settings": "setting"}
_DB_REF = "${secret:db:"
_REENTRY = "_needs_reentry"  # a connection's fields whose secret was cleared when where it goes changed
# The settings that say where a connection goes: changing one clears its secrets.
ENDPOINT_KEYS = frozenset(
    {
        "base_url",
        "url",
        "host",
        "hostname",
        "endpoint",
        "endpoint_url",
        "accounts_url",
        "accounts_domain",
        "server",
    }
)
_SECRET_WORDS = ("secret", "password", "passwd", "token", "api_key", "apikey", "private_key", "credentials")


class ConfigProblem(ValueError):
    """An entry that can't be kept as given. The message says which field and why, and holds no secret."""


def _connectors_named(connector: str) -> list[ConnectorInfo]:
    return [i for i in list_connectors() if i.name == connector or i.name.startswith(f"{connector}.")]


def _secret_fields_of(connector: str) -> frozenset[str]:
    """The secret settings of a connection's connector: of every installed connector named `connector` or
    `connector.<kind>`."""
    found: set[str] = set()
    for info in _connectors_named(connector):
        found |= secret_fields(info.spec)
    return frozenset(found)


def _looks_secret(key: str) -> bool:
    lowered = key.lower().replace("-", "_")
    return any(lowered.endswith(w) for w in _SECRET_WORDS)


def secret_refs_of(value: Mapping[str, Any]) -> dict[str, str]:
    auth = value.get("auth")
    return {str(k): str(v) for k, v in cast(dict[str, Any], auth).items()} if isinstance(auth, dict) else {}


def _walk(node: Mapping[str, Any], at: str) -> Iterator[tuple[str, str, Any]]:
    for key, value in node.items():
        path = f"{at}.{key}" if at else str(key)
        if isinstance(value, dict):
            yield from _walk(cast(dict[str, Any], value), path)
        else:
            yield path, str(key), value


def check_entry(kind: EnvKind, name: str, value: Any) -> Any:
    """`value` checked for its kind on its own; raises `ConfigProblem`. `Environment.put` then checks it together
    with the environment's other entries, as `load_project` would."""
    if not name or len(name) > 200 or "/" in name:
        raise ConfigProblem(f"{name!r} is not a name: 1 to 200 characters, no '/'")
    if kind != "connection":
        return value
    if not isinstance(value, dict):
        raise ConfigProblem(f"connection {name}: give a mapping with a connector")
    body = {str(k): v for k, v in cast(dict[str, Any], value).items()}
    connector = body.get("connector")
    if not isinstance(connector, str) or not connector:
        raise ConfigProblem(f"connection {name}: connector is required, such as zakya")
    if not _connectors_named(connector):
        known = sorted({i.name.split(".")[0] for i in list_connectors()})
        raise ConfigProblem(
            f"connection {name}: no connector {connector!r} is installed; there are {', '.join(known)}"
        )
    secret = _secret_fields_of(connector)
    for path, key, field in _walk({k: v for k, v in body.items() if k != "auth"}, ""):
        if key.startswith("_"):
            raise ConfigProblem(f"connection {name}: {path}: a setting's name doesn't start with '_'")
        if key in secret or _looks_secret(key):
            raise ConfigProblem(
                f"connection {name}: {path} is secret: give it as a reference under auth, such as "
                f"auth.{key} = ${{secret:db:{db_secret_name(name, key)}}}"
            )
        if is_secret_ref(field) or (isinstance(field, str) and "${secret:" in field):
            raise ConfigProblem(f"connection {name}: {path}: a secret reference stands only under auth")
    auth = body.get("auth")
    if auth is not None and not isinstance(auth, dict):
        raise ConfigProblem(f"connection {name}: auth must be a mapping of fields to references")
    for key, ref in secret_refs_of(body).items():
        wanted = f"{_DB_REF}{db_secret_name(name, key)}}}"
        if ref != wanted:
            raise ConfigProblem(
                f"connection {name}: auth.{key} must be {wanted}: the environment's connections keep their secrets "
                "in its own store, each named for its connection and field"
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


def _field_type(connector: str, key: str) -> Any:
    for info in _connectors_named(connector):
        model = info.spec.config
        if model is not None and key in model.model_fields:
            return model.model_fields[key].annotation
    return None


def _scalar_types(annotation: Any) -> set[type]:
    if isinstance(annotation, type):
        return {annotation}
    found: set[type] = set()
    for arg in typing.get_args(annotation):
        found |= _scalar_types(arg)
    return found


def coerce_setting(connector: str, key: str, text: str) -> Any:
    """A setting typed in a form, as the connector's config says its type is: a bool, an int, a float, or text.
    For a setting the connector doesn't type, YAML reads a number or a boolean, and anything else stays text."""
    types = _scalar_types(_field_type(connector, key)) - {type(None)}
    lowered = text.strip().lower()
    if bool in types:
        if lowered in ("true", "yes", "on", "1"):
            return True
        if lowered in ("false", "no", "off", "0"):
            return False
    if int in types:
        try:
            return int(text.strip())
        except ValueError:
            pass
    if float in types:
        try:
            return float(text.strip())
        except ValueError:
            pass
    if types:
        return text
    try:
        loaded = yaml.safe_load(text)
    except yaml.YAMLError:
        return text
    return loaded if isinstance(loaded, bool | int | float) else text


def _as_db_refs(name: str, value: Any) -> Any:
    """An M1 connection's credentials as the environment keeps them: each `auth` field a reference to the secret
    named for it in the environment's own store, which is then set with `feather secret set`."""
    if not isinstance(value, dict) or not isinstance(cast(dict[str, Any], value).get("auth"), dict):
        return value
    body = dict(cast(dict[str, Any], value))
    body["auth"] = {k: f"{_DB_REF}{db_secret_name(name, k)}}}" for k in secret_refs_of(body)}
    return body


def _diff(old: Any, new: Any) -> dict[str, Any]:
    return {"old": old, "new": new}


def _endpoint(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {k: v for k, v in cast(dict[str, Any], value).items() if k in ENDPOINT_KEYS}


class Environment:
    """The configuration and secrets of the environment `env`, for one store's tenant. `vault` is the process's
    one vault of that environment."""

    def __init__(self, store: Store, clock: Clock, env: str, vault: Vault) -> None:
        self._store, self._clock, self._env = store, clock, env
        self.vault = vault

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

    def _check_with_others(self, changes: Sequence[tuple[EnvKind, str, Any]]) -> None:
        """Check `changes` with the environment's other entries, as a server would load them."""
        changed = {(k, n) for k, n, _ in changes}
        layer = environment_layer(e for e in self.entries() if (e.kind, e.name) not in changed)
        for kind, name, value in changes:
            plural = {"connection": "connections", "budget": "budgets", "setting": "settings"}[kind]
            shown = (
                {k: v for k, v in value.items() if not k.startswith("_")}
                if isinstance(value, dict)
                else value
            )
            layer.setdefault(plural, {})[name] = shown
        try:
            check_environment(layer)
        except ProjectError as error:
            raise ConfigProblem(str(error).replace("environment: ", "")) from None

    def put(
        self, actor: Actor, kind: EnvKind, name: str, value: Any, *, reason: str | None = None
    ) -> EnvEntry:
        """Set an entry, audited with its old and new value."""
        return self._put_checked(actor, [(kind, name, value)], reason)[0]

    def _put_checked(
        self, actor: Actor, found: Sequence[tuple[str, str, Any]], reason: str | None
    ) -> list[EnvEntry]:
        checked: list[tuple[EnvKind, str, Any]] = []
        for kind, name, value in found:
            require(actor, "environment.manage", self._store.audit, f"{kind}:{name}")
            require_reason(actor, "environment.manage", reason)
            if kind not in ENV_KINDS:
                raise ConfigProblem(f"{kind!r} is not one of {', '.join(ENV_KINDS)}")
            checked.append((cast(EnvKind, kind), name, check_entry(cast(EnvKind, kind), name, value)))
        self._check_with_others(checked)
        return [self._keep(actor, kind, name, value, reason) for kind, name, value in checked]

    def _keep(self, actor: Actor, kind: EnvKind, name: str, value: Any, reason: str | None) -> EnvEntry:
        target = f"{kind}:{name}"
        current = self._store.env_config.entry(self._env, kind, name)
        if kind == "connection":
            value = dict(value)
            pending = list(cast(dict[str, Any], current.value).get(_REENTRY) or []) if current else []
            if current is not None and _endpoint(current.value) != _endpoint(value):
                pending = sorted(
                    set(pending) | self._clear_secrets(actor, name, current.value, value, reason)
                )
            if pending:
                value[_REENTRY] = [f for f in pending if f in secret_refs_of(value)]
        entry = EnvEntry(self._env, kind, name, value, actor.name, self._clock.now())
        old = self._store.env_config.put(entry)
        request = {"env": self._env, "reason": reason, **_diff(None if old is None else old.value, value)}
        self._store.audit.record(actor.kind, actor.name, f"set_{kind}", target, request, "ok")
        return entry

    def _clear_secrets(self, actor: Actor, name: str, old: Any, new: Any, reason: str | None) -> set[str]:
        """Delete a connection's secrets when where it goes changed, and return the fields that need them again."""
        fields = set(secret_refs_of(old)) | set(secret_refs_of(new))
        cleared = sorted(f for f in fields if self.vault.delete(db_secret_name(name, f)))
        self._store.audit.record(
            actor.kind,
            actor.name,
            "clear_secrets",
            f"connection:{name}",
            {
                "env": self._env,
                "secrets": [db_secret_name(name, f) for f in cleared],
                "because": _diff(_endpoint(old), _endpoint(new)),
                "reason": reason,
            },
            "ok",
        )
        return fields

    def update(
        self, actor: Actor, name: str, changes: Mapping[str, Any], *, reason: str | None = None
    ) -> EnvEntry:
        """Change some settings of a connection: a None removes one."""
        current = self._store.env_config.entry(self._env, "connection", name)
        value: dict[str, Any] = (
            {k: v for k, v in cast(dict[str, Any], current.value).items() if not k.startswith("_")}
            if current is not None
            else {}
        )
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
        require_reason(actor, "environment.manage", reason)
        if kind == "budget":
            users = sorted(
                e.name
                for e in self.entries("connection")
                if cast(dict[str, Any], e.value).get("budget") == name
            )
            if users:
                raise ConfigProblem(
                    f"budget {name} is used by the connections {', '.join(users)}: change them first"
                )
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
        """Set many entries: every one checked first, alone and together, so a bad one sets none."""
        require(actor, "environment.manage", self._store.audit, "environment")
        require_reason(actor, "environment.manage", reason)
        rows = [(k, n, _as_db_refs(n, v) if k == "connection" else v) for k, n, v in found]
        return self._put_checked(actor, rows, reason or "imported")

    # Views.

    def connection(self, name: str) -> dict[str, Any]:
        """A connection as the API and the page show it: its settings, and for each secret field its reference,
        whether it is set and when and by whom, or that it needs re-entry."""
        entry = self.entry("connection", name)
        return self._view(entry)

    def connections(self) -> list[dict[str, Any]]:
        return [self._view(e) for e in self.entries("connection")]

    def _view(self, entry: EnvEntry) -> dict[str, Any]:
        value = cast(dict[str, Any], entry.value)
        sealed = {s.name: s for s in self.vault.names()}
        pending = set(value.get(_REENTRY) or [])
        secrets: dict[str, Any] = {}
        for key, ref in secret_refs_of(value).items():
            shown: dict[str, Any] = {"ref": ref}
            if ref.startswith(_DB_REF) and ref.endswith("}"):
                found = sealed.get(ref[len(_DB_REF) : -1])
                shown |= (
                    {"set": True, "updated_at": found.updated_at.isoformat(), "updated_by": found.updated_by}
                    if found is not None
                    else {"set": False, "needs_reentry": key in pending}
                )
            secrets[key] = shown
        return {
            "name": entry.name,
            "connector": value.get("connector"),
            "settings": {k: v for k, v in value.items() if k != "auth" and not k.startswith("_")},
            "secrets": secrets,
            "secret_fields": sorted(_secret_fields_of(str(value.get("connector") or ""))),
            "changed_by": entry.changed_by,
            "changed_at": entry.changed_at.isoformat(),
        }

    # Secrets.

    def set_secret(self, actor: Actor, name: str, value: str, *, reason: str | None = None) -> bool:
        target = f"secret:{name}"
        require(actor, "secrets.manage", self._store.audit, target)
        require_reason(actor, "secrets.manage", reason)
        replaced = self.vault.set(name, value, actor.name, self._clock.now())
        request = {"env": self._env, "name": name, "replaced": replaced, "reason": reason}
        self._store.audit.record(actor.kind, actor.name, "set_secret", target, request, "ok")
        self._entered(name)
        return replaced

    def _entered(self, name: str) -> None:
        """A secret a connection needed again is set: it no longer needs re-entry."""
        connection, _, field = name.partition("/")
        entry = self._store.env_config.entry(self._env, "connection", connection)
        if entry is None or field not in (cast(dict[str, Any], entry.value).get(_REENTRY) or []):
            return
        value = dict(cast(dict[str, Any], entry.value))
        value[_REENTRY] = [f for f in value[_REENTRY] if f != field]
        if not value[_REENTRY]:
            del value[_REENTRY]
        self._store.env_config.put(
            EnvEntry(self._env, "connection", connection, value, entry.changed_by, entry.changed_at)
        )

    def secrets(self) -> list[dict[str, Any]]:
        return [_secret_row(s) for s in self.vault.names()]

    def delete_secret(self, actor: Actor, name: str, *, reason: str | None = None) -> None:
        target = f"secret:{name}"
        require(actor, "secrets.manage", self._store.audit, target)
        require_reason(actor, "secrets.manage", reason)
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

    def rotate_key(self, actor: Actor, *, reason: str | None = None) -> dict[str, Any]:
        """Seal every secret again under this process's current master key, and make it the active one."""
        require(actor, "secrets.manage", self._store.audit, "secrets")
        require_reason(actor, "secrets.manage", reason)
        ring = self.vault.keyring
        key_id = ring.current.key_id if ring is not None else None
        try:
            count = self.vault.rotate(actor.name, self._clock.now())
        except VaultError as error:
            self._store.audit.record(
                actor.kind, actor.name, "rotate_master_key", "secrets", {"env": self._env}, f"failed: {error}"
            )
            raise
        request = {"env": self._env, "secrets": count, "key_id": key_id, "reason": reason}
        self._store.audit.record(actor.kind, actor.name, "rotate_master_key", "secrets", request, "ok")
        return {"secrets": count, "key_id": key_id}

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

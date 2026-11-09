"""The environment's encrypted secrets: `${secret:db:<name>}` (flow packages §8, sponsor decision of 2026-11-09).

A secret is sealed with AES-256-GCM under the environment's master key, with a fresh 12-byte nonce each time and the
tenant, environment and name as associated data, so that a ciphertext copied to another name, environment or tenant
doesn't open. The store keeps only the ciphertext, the nonce and the key's id; the key comes from `FEATHER_MASTER_KEY`
(32 bytes, base64) or the file `FEATHER_MASTER_KEY_FILE` names, and is never in the database.

A process holds a keyring: its current key, which it seals with, and the keys it may still open with,
`FEATHER_MASTER_KEY_PREVIOUS` (or `_PREVIOUS_FILE`; several, comma-separated). The database keeps the id of each
environment's active key, so a process whose current key isn't the active one may open but not set (`KeyConflict`).
A rotation seals every secret again under the rotating process's current key, in one transaction that locks the key
row and swaps it only if it is still the key the rotation began from. To rotate: give every process the new key as a
previous one, then make it current (with the old one previous) and restart, run `feather secret rotate-key`, and drop
the old key once every process has restarted.

Secrets are write-only: they can be set, replaced, listed by name and deleted, and are opened only to resolve a
reference when a connection is used. Without a master key the server still starts; resolving a `db` secret then fails
with an error that says how to set one. A wrong key fails cleanly, naming the key the secret was sealed with.
"""

import base64
import binascii
import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from feather_connect.storage.port import KeyMismatch, SealedSecret, SecretStore

__all__ = [
    "KeyConflict",
    "Keyring",
    "MasterKey",
    "NoMasterKey",
    "Vault",
    "VaultError",
    "keyring_from_env",
    "new_master_key",
]

_NONCE = 12
_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./")
MAX_SECRET = 64 * 1024  # bytes of a secret's value


class VaultError(Exception):
    """A secret can't be set or opened. The message names the secret, never its value."""


class NoMasterKey(VaultError):
    """The environment has no master key, so its `db` secrets can't be set or opened."""


class KeyConflict(VaultError):
    """This process's current key isn't the environment's active one, or another process rotated meanwhile."""


@dataclass(frozen=True, repr=False)
class MasterKey:
    """A 32-byte key and its id: the first 12 hex digits of its sha256, which is all that is stored with a secret."""

    key: bytes

    @property
    def key_id(self) -> str:
        return hashlib.sha256(self.key).hexdigest()[:12]

    def __repr__(self) -> str:  # never the key
        return f"MasterKey({self.key_id})"

    @classmethod
    def parse(cls, text: str) -> "MasterKey":
        try:
            key = base64.b64decode(text.strip(), validate=True)
        except (binascii.Error, ValueError):
            raise VaultError("the master key is not base64: make one with `feather secret new-key`") from None
        if len(key) != 32:
            raise VaultError(
                f"the master key has {len(key)} bytes, not 32: make one with `feather secret new-key`"
            )
        return cls(key)


def new_master_key() -> str:
    """A new master key, base64, for `FEATHER_MASTER_KEY`."""
    return base64.b64encode(AESGCM.generate_key(bit_length=256)).decode()


@dataclass(frozen=True)
class Keyring:
    """The key a process seals with, and the earlier keys it may still open with."""

    current: MasterKey
    previous: tuple[MasterKey, ...] = ()

    def find(self, key_id: str) -> MasterKey | None:
        return next((k for k in (self.current, *self.previous) if k.key_id == key_id), None)

    @property
    def key_ids(self) -> list[str]:
        return [k.key_id for k in (self.current, *self.previous)]


def _read(environ: Mapping[str, str], name: str) -> str | None:
    if text := environ.get(name):
        return text
    if where := environ.get(f"{name}_FILE"):
        try:
            return Path(where).read_text(encoding="utf-8")
        except OSError as error:
            raise VaultError(f"can't read {name}_FILE ({error.strerror})") from None
    return None


def keyring_from_env(environ: Mapping[str, str] | None = None) -> Keyring | None:
    """The keyring `FEATHER_MASTER_KEY` (or `_FILE`) and `FEATHER_MASTER_KEY_PREVIOUS` (or `_FILE`, comma-separated)
    give, or None when no master key is set."""
    environ = os.environ if environ is None else environ
    current, previous = _read(environ, "FEATHER_MASTER_KEY"), _read(environ, "FEATHER_MASTER_KEY_PREVIOUS")
    if current is None:
        if previous is not None:
            raise VaultError("FEATHER_MASTER_KEY_PREVIOUS is set without FEATHER_MASTER_KEY")
        return None
    older = tuple(MasterKey.parse(p) for p in previous.split(",") if p.strip()) if previous else ()
    return Keyring(MasterKey.parse(current), older)


def check_name(name: str) -> str:
    if (
        not name
        or len(name) > 200
        or not set(name) <= _NAME_CHARS
        or any(part in ("", ".", "..") for part in name.split("/"))
    ):
        raise VaultError(
            f"{name!r} is not a secret name: use letters, digits, '-', '_' and '.', in parts such as "
            "<connection>/<field>"
        )
    return name


def _aad(tenant: str, env: str, name: str) -> bytes:
    return f"feather-secret/2\0{tenant}\0{env}\0{name}".encode()


def _legacy_aad(env: str, name: str) -> bytes:
    """The associated data of a secret sealed before the tenant was in it: opened, and sealed anew by a rotation."""
    return f"feather-secret\0{env}\0{name}".encode()


class Vault:
    """The secrets of one environment of a store's tenant. `keys` may be None: then listing and deleting work, and
    setting or opening raises `NoMasterKey`. A process makes one and shares it."""

    def __init__(self, store: SecretStore, env: str, keys: Keyring | MasterKey | None) -> None:
        self._store, self._env = store, env
        self._ring = Keyring(keys) if isinstance(keys, MasterKey) else keys

    @property
    def keyring(self) -> Keyring | None:
        return self._ring

    def _need_ring(self) -> Keyring:
        if self._ring is None:
            raise NoMasterKey(
                f"environment {self._env} has no master key, so its db secrets can't be used: set FEATHER_MASTER_KEY "
                "or FEATHER_MASTER_KEY_FILE (make one with `feather secret new-key`)"
            )
        return self._ring

    def _seal(self, key: MasterKey, name: str, value: bytes, by: str, at: datetime) -> SealedSecret:
        nonce = os.urandom(_NONCE)
        sealed = AESGCM(key.key).encrypt(nonce, value, _aad(self._store.tenant, self._env, name))
        return SealedSecret(self._env, name, sealed, nonce, key.key_id, by, at)

    def _conflict(self, error: KeyMismatch) -> KeyConflict:
        return KeyConflict(
            f"environment {self._env}'s secrets are sealed with master key {error.active}, and this process's "
            f"current key is {self._need_ring().current.key_id}: restart it with the active key as "
            "FEATHER_MASTER_KEY, or rotate from a process whose current key is the new one"
        )

    def set(self, name: str, value: str, by: str, at: datetime) -> bool:
        """Seal and keep a secret, replacing one of that name. Returns whether it replaced one."""
        check_name(name)
        data = value.encode("utf-8")
        if not data or len(data) > MAX_SECRET:
            raise VaultError(f"secret {name}: a value has 1 to {MAX_SECRET} bytes")
        try:
            return self._store.put(self._seal(self._need_ring().current, name, data, by, at))
        except KeyMismatch as error:
            raise self._conflict(error) from None

    def names(self) -> list[SealedSecret]:
        return self._store.names(self._env)

    def delete(self, name: str) -> bool:
        return self._store.remove(self._env, check_name(name))

    def open(self, name: str) -> str:
        """The value of a secret, to resolve `${secret:db:<name>}` when a connection is used, and for nothing else."""
        ring = self._need_ring()
        sealed = self._store.get(self._env, name)
        if sealed is None:
            raise VaultError(
                f"secret {name} is not set in environment {self._env}: feather secret set {name}"
            )
        return self._open(ring, sealed)

    def _open(self, ring: Keyring, sealed: SealedSecret) -> str:
        key = ring.find(sealed.key_id)
        if key is None:
            raise VaultError(
                f"secret {sealed.name} was sealed with master key {sealed.key_id}, but this process has "
                f"{', '.join(ring.key_ids)}: add it as FEATHER_MASTER_KEY_PREVIOUS, or set the secret again"
            )
        cipher = AESGCM(key.key)
        for aad in (_aad(self._store.tenant, sealed.env, sealed.name), _legacy_aad(sealed.env, sealed.name)):
            try:
                return cipher.decrypt(sealed.nonce, sealed.ciphertext, aad).decode()
            except (InvalidTag, UnicodeDecodeError):
                continue
        raise VaultError(f"secret {sealed.name} doesn't open with master key {key.key_id}")

    def rotate(self, by: str, at: datetime) -> int:
        """Seal every secret of the environment again under this process's current key, and make it the active one,
        in one transaction: every secret must open with the keyring, or nothing changes. Returns how many."""
        ring = self._need_ring()
        expected = self._store.active_key(self._env)

        def reseal(sealed: SealedSecret) -> SealedSecret:
            value = self._open(ring, sealed).encode()
            return replace(self._seal(ring.current, sealed.name, value, by, at), updated_by=sealed.updated_by)

        try:
            return self._store.rotate(self._env, expected, ring.current.key_id, reseal)
        except KeyMismatch as error:
            raise KeyConflict(
                f"environment {self._env}'s active key changed to {error.active} during the rotation: run it again"
            ) from None

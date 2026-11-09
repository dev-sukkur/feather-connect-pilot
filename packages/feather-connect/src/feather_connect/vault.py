"""The environment's encrypted secrets: `${secret:db:<name>}` (flow packages §8, sponsor decision of 2026-11-09).

A secret is sealed with AES-256-GCM under the environment's master key, with a fresh 12-byte nonce each time and the
environment and name as associated data, so that a ciphertext copied to another name or environment doesn't open. The
store keeps only the ciphertext, the nonce and the key's id; the key comes from `FEATHER_MASTER_KEY` (32 bytes, base64)
or the file `FEATHER_MASTER_KEY_FILE` names, and is never in the database.

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

from feather_connect.storage.port import SealedSecret, SecretStore

__all__ = ["MasterKey", "NoMasterKey", "Vault", "VaultError", "master_key_from_env", "new_master_key"]

_NONCE = 12
_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
MAX_SECRET = 64 * 1024  # bytes of a secret's value


class VaultError(Exception):
    """A secret can't be set or opened. The message names the secret, never its value."""


class NoMasterKey(VaultError):
    """The environment has no master key, so its `db` secrets can't be set or opened."""


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


def master_key_from_env(environ: Mapping[str, str] | None = None) -> MasterKey | None:
    """The master key `FEATHER_MASTER_KEY` or `FEATHER_MASTER_KEY_FILE` gives, or None when neither is set."""
    environ = os.environ if environ is None else environ
    if text := environ.get("FEATHER_MASTER_KEY"):
        return MasterKey.parse(text)
    if where := environ.get("FEATHER_MASTER_KEY_FILE"):
        try:
            return MasterKey.parse(Path(where).read_text(encoding="utf-8"))
        except OSError as error:
            raise VaultError(f"can't read FEATHER_MASTER_KEY_FILE ({error.strerror})") from None
    return None


def check_name(name: str) -> str:
    if not name or len(name) > 200 or not set(name) <= _NAME_CHARS:
        raise VaultError(f"{name!r} is not a secret name: use letters, digits, '-', '_' and '.'")
    return name


def _aad(env: str, name: str) -> bytes:
    return f"feather-secret\0{env}\0{name}".encode()


class Vault:
    """The secrets of one environment of a store's tenant, sealed with `key`. `key` may be None: then listing and
    deleting work, and setting or opening raises `NoMasterKey`."""

    def __init__(self, store: SecretStore, env: str, key: MasterKey | None) -> None:
        self._store, self._env, self._key = store, env, key

    def _need_key(self) -> MasterKey:
        if self._key is None:
            raise NoMasterKey(
                f"environment {self._env} has no master key, so its db secrets can't be used: set FEATHER_MASTER_KEY "
                "or FEATHER_MASTER_KEY_FILE (make one with `feather secret new-key`)"
            )
        return self._key

    def _seal(self, key: MasterKey, name: str, value: bytes, by: str, at: datetime) -> SealedSecret:
        nonce = os.urandom(_NONCE)
        sealed = AESGCM(key.key).encrypt(nonce, value, _aad(self._env, name))
        return SealedSecret(self._env, name, sealed, nonce, key.key_id, by, at)

    def set(self, name: str, value: str, by: str, at: datetime) -> bool:
        """Seal and keep a secret, replacing one of that name. Returns whether it replaced one."""
        check_name(name)
        data = value.encode("utf-8")
        if not data or len(data) > MAX_SECRET:
            raise VaultError(f"secret {name}: a value has 1 to {MAX_SECRET} bytes")
        return self._store.put(self._seal(self._need_key(), name, data, by, at))

    def names(self) -> list[SealedSecret]:
        return self._store.names(self._env)

    def delete(self, name: str) -> bool:
        return self._store.remove(self._env, check_name(name))

    def open(self, name: str) -> str:
        """The value of a secret, to resolve `${secret:db:<name>}` when a connection is used, and for nothing else."""
        key = self._need_key()
        sealed = self._store.get(self._env, name)
        if sealed is None:
            raise VaultError(
                f"secret {name} is not set in environment {self._env}: feather secret set {name}"
            )
        return self._open(key, sealed)

    def _open(self, key: MasterKey, sealed: SealedSecret) -> str:
        if sealed.key_id != key.key_id:
            raise VaultError(
                f"secret {sealed.name} was sealed with master key {sealed.key_id}, but this server has {key.key_id}: "
                "set the key it was sealed with, or set the secret again"
            )
        try:
            return (
                AESGCM(key.key)
                .decrypt(sealed.nonce, sealed.ciphertext, _aad(sealed.env, sealed.name))
                .decode()
            )
        except (InvalidTag, UnicodeDecodeError):
            raise VaultError(f"secret {sealed.name} doesn't open with master key {key.key_id}") from None

    def rotate(self, new: MasterKey, by: str, at: datetime) -> int:
        """Seal every secret again under `new`, in one transaction, and return how many. Every secret must open
        with the current key first, so a rotation never half happens."""
        key = self._need_key()
        opened = [(s, self._open(key, s).encode()) for s in self._store.names(self._env)]
        sealed = [replace(self._seal(new, s.name, value, by, at)) for s, value in opened]
        self._store.replace_all(self._env, sealed)
        self._key = new
        return len(sealed)

"""Signed flow packages (flow packages §11, task 11): a wheel signed at build with an ed25519 key, and an environment
that takes only packages signed with a key it trusts.

The signature is over the wheel's `RECORD`, which lists every other file of the wheel with its sha256 and size, so it
covers the whole package; it is kept in the wheel itself, as `<dist-info>/feather-signature.json`, which `RECORD`
doesn't list (as PEP 427 does for `RECORD.jws`). So the signature travels with the wheel through every registry and
every promotion, and nothing else needs to store it. ed25519 signatures are deterministic, so a signed build is as
reproducible as an unsigned one.

A key is named by its id, the first 16 hex digits of the sha256 of its raw public key. A public key is written
`ed25519:<base64 of its 32 bytes>`, which is what an environment's `trusted_package_keys` setting lists, and what
`feather package keygen` prints. A private key is a PEM file (PKCS #8), as `feather package keygen` writes it.

`verify_wheel` checks, in order: the wheel has a signature, by a key the environment trusts, that is valid for its
`RECORD`, and that every file of the wheel is in `RECORD` with the sha256 and size it says, and nothing more.
"""

import base64
import binascii
import hashlib
import io
import json
import zipfile
import zlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

__all__ = [
    "PREFIX",
    "SIGNATURE_FILE",
    "SignatureError",
    "Signer",
    "check_trusted_keys",
    "key_id",
    "parse_public_key",
    "public_key_text",
    "read_private_key",
    "signature_entry",
    "verify_wheel",
]

SIGNATURE_FILE = "feather-signature.json"  # in the wheel's .dist-info, beside RECORD and not listed in it
PREFIX = "ed25519:"
_ALG = "ed25519"
MAX_SIGNATURE = 4 * 1024  # bytes the signature file may have
MAX_RECORD = 1024 * 1024  # bytes RECORD may have
_MAX_TOTAL = (
    200 * 1024 * 1024
)  # bytes a wheel may unzip to while its RECORD is checked: a zip bomb stops here


class SignatureError(ValueError):
    """A wheel isn't signed as the environment needs, or a key can't be read. The message says why."""


def key_id(public: Ed25519PublicKey) -> str:
    raw = public.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return hashlib.sha256(raw).hexdigest()[:16]


def public_key_text(public: Ed25519PublicKey) -> str:
    """`ed25519:<base64>`, as `trusted_package_keys` lists a key."""
    raw = public.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return PREFIX + base64.b64encode(raw).decode()


def parse_public_key(text: str) -> Ed25519PublicKey:
    """A public key written `ed25519:<base64 of 32 bytes>`. Raises `SignatureError`."""
    if not isinstance(text, str) or not text.startswith(PREFIX):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise SignatureError(f"{str(text)[:40]!r} is not a public key: write it as {PREFIX}<base64>")
    try:
        raw = base64.b64decode(text.removeprefix(PREFIX), validate=True)
        return Ed25519PublicKey.from_public_bytes(raw)
    except (binascii.Error, ValueError):
        raise SignatureError(f"{text[:40]!r} is not an ed25519 public key of 32 bytes in base64") from None


def check_trusted_keys(value: Any) -> dict[str, Ed25519PublicKey]:
    """The keys of a `trusted_package_keys` setting, a list of public keys, by id. Raises `SignatureError`."""
    if not isinstance(value, list):
        raise SignatureError("trusted_package_keys is a list of public keys, such as [ed25519:...]")
    keys: dict[str, Ed25519PublicKey] = {}
    for item in cast(list[Any], value):
        public = parse_public_key(item)
        keys[key_id(public)] = public
    return keys


def read_private_key(data: bytes) -> Ed25519PrivateKey:
    """An ed25519 private key from a PEM file. Raises `SignatureError`."""
    try:
        key = serialization.load_pem_private_key(data, password=None)
    except (ValueError, TypeError):
        raise SignatureError("the signing key is not a PEM private key without a password") from None
    if not isinstance(key, Ed25519PrivateKey):
        raise SignatureError("the signing key is not an ed25519 key")
    return key


@dataclass(frozen=True)
class Signer:
    """A private key that signs a wheel's RECORD."""

    private: Ed25519PrivateKey

    @classmethod
    def generate(cls) -> "Signer":
        return cls(Ed25519PrivateKey.generate())

    @classmethod
    def from_pem(cls, data: bytes) -> "Signer":
        return cls(read_private_key(data))

    def pem(self) -> bytes:
        return self.private.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )

    @property
    def public(self) -> Ed25519PublicKey:
        return self.private.public_key()

    @property
    def key_id(self) -> str:
        return key_id(self.public)

    def sign(self, record: bytes) -> bytes:
        """The signature file's bytes for a wheel whose RECORD is `record`."""
        signature = base64.b64encode(self.private.sign(record)).decode()
        return signature_entry(self.key_id, signature)


def signature_entry(kid: str, signature: str) -> bytes:
    return (json.dumps({"alg": _ALG, "key_id": kid, "signature": signature}, sort_keys=True) + "\n").encode()


def _record_hash(data_hash: bytes) -> str:
    return "sha256=" + base64.urlsafe_b64encode(data_hash).rstrip(b"=").decode()


def _dist_info(names: Iterable[str]) -> str:
    found = sorted({n.split("/", 1)[0] for n in names if n.split("/", 1)[0].endswith(".dist-info")})
    if len(found) != 1:
        raise SignatureError("the wheel has no single .dist-info directory")
    return found[0]


def verify_wheel(content: bytes, trusted: Mapping[str, Ed25519PublicKey]) -> str:
    """Check that the wheel is signed by one of the `trusted` keys (by id) and that its files are the ones its signed
    RECORD lists. Returns the id of the key that signed it. Raises `SignatureError` saying why not, for a zip that
    can't be read as for a bad signature: never anything else."""
    try:
        return _verify(content, trusted)
    except SignatureError:
        raise
    except (
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
        RuntimeError,
        NotImplementedError,
        zlib.error,
        EOFError,
        OSError,
    ):
        raise SignatureError("the wheel can't be unzipped to check its signature") from None


def _bounded(archive: zipfile.ZipFile, name: str, most: int) -> bytes:
    """An entry's bytes, refused past `most` by what the zip says and by what it unzips to."""
    if archive.getinfo(name).file_size > most:
        raise SignatureError(f"{name} is more than {most} bytes")
    with archive.open(name) as entry:
        data = entry.read(most + 1)
    if len(data) > most:
        raise SignatureError(f"{name} is more than {most} bytes")
    return data


def _verify(content: bytes, trusted: Mapping[str, Ed25519PublicKey]) -> str:
    archive = zipfile.ZipFile(io.BytesIO(content))
    with archive:
        names = archive.namelist()
        info = _dist_info(names)
        where, record_name = f"{info}/{SIGNATURE_FILE}", f"{info}/RECORD"
        if where not in names:
            raise SignatureError("the package is not signed: build it with feather package build --sign-key")
        if record_name not in names:
            raise SignatureError("the wheel has no RECORD to check its signature against")
        raw = _bounded(archive, where, MAX_SIGNATURE)
        try:
            found: Any = json.loads(raw)
            kid, signature = str(found["key_id"]), base64.b64decode(str(found["signature"]), validate=True)
            if found.get("alg") != _ALG:
                raise SignatureError(f"the signature is not {_ALG}")
        except (ValueError, KeyError, TypeError, binascii.Error):
            raise SignatureError(f"{where} is not a signature this server can read") from None
        public = trusted.get(kid)
        if public is None:
            raise SignatureError(f"the package is signed by key {kid}, which this environment doesn't trust")
        record = _bounded(archive, record_name, MAX_RECORD)
        try:
            public.verify(signature, record)
        except InvalidSignature:
            raise SignatureError(f"the signature by key {kid} is not valid for this package") from None
        _check_record(archive, record.decode("utf-8", "replace"), {where, record_name})
    return kid


def _check_record(archive: zipfile.ZipFile, record: str, unlisted: set[str]) -> None:
    """Every file of the archive but `unlisted` is in RECORD with its sha256 and size, and RECORD lists no other."""
    listed: dict[str, tuple[str, str]] = {}
    for line in record.splitlines():
        if not line.strip():
            continue
        path, digest, size = (line.rsplit(",", 2) + ["", ""])[:3]
        listed[path] = (digest, size)
    total = 0
    for entry in archive.infolist():
        if entry.is_dir() or entry.filename in unlisted:
            continue
        expected = listed.pop(entry.filename, None)
        if expected is None:
            raise SignatureError(f"{entry.filename} is in the wheel but not in its signed RECORD")
        digest = hashlib.sha256()
        size = 0
        with archive.open(entry) as stream:
            while chunk := stream.read(1024 * 1024):
                size += len(chunk)
                total += len(chunk)
                if total > _MAX_TOTAL:
                    raise SignatureError(f"the wheel unzips to more than {_MAX_TOTAL} bytes")
                digest.update(chunk)
        if (_record_hash(digest.digest()), str(size)) != expected:
            raise SignatureError(f"{entry.filename} is not the file its signed RECORD names")
    listed = {p: v for p, v in listed.items() if p not in unlisted}
    if listed:
        raise SignatureError(f"the signed RECORD lists {min(listed)}, which the wheel doesn't have")

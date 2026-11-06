"""Records, and the two things every record needs: a key that identifies it and a hash of its content."""

import base64
import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal, cast

from feather_connect.core.errors import MissingKeyError

Op = Literal["upsert", "delete"]


def _json_default(value: Any) -> Any:
    """Encode what `json` can't. Anything else is an error, as it is for `json` itself."""
    if isinstance(value, datetime):  # before date, because a datetime is also a date
        if value.utcoffset() is not None:
            value = value.astimezone(UTC)  # equal instants must give equal text
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Mapping):
        return dict(cast(Mapping[str, Any], value))  # a mapping that isn't a dict, such as a Record
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


# One encoder for every row: `json.dumps` builds a new one per call for settings like these, and this runs per row.
_CANONICAL = json.JSONEncoder(sort_keys=True, separators=(",", ":"), default=_json_default)


def canonical_json(value: Any) -> bytes:
    """Return `value` as JSON bytes that are the same for the same data: sorted keys, no spaces.

    Beyond the JSON types it accepts `Decimal` (as text), `date` and `datetime` (as ISO-8601, with an
    aware datetime converted to UTC first) and `bytes` (as base64).
    """
    return _CANONICAL.encode(value).encode("utf-8")


def row_hash(payload: Mapping[str, Any]) -> int:
    """Return a 64-bit hash of a payload's content, to tell whether a record has changed.

    The value is stored and compared with later reads, so it must never change for the same data:
    it is the 8-byte BLAKE2b digest of the canonical JSON, read as a big-endian unsigned integer.
    """
    digest = hashlib.blake2b(canonical_json(payload), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def _key_value(payload: Mapping[str, Any], name: str) -> Any:
    value = payload.get(name)
    if value is None:
        raise MissingKeyError(f"record has no value for key field {name!r}")
    return value


def make_key(payload: Mapping[str, Any], key: str | tuple[str, ...]) -> str:
    """Return the key of a record: the value as text for one key field, a JSON list for several.

    Raises `MissingKeyError` when a key field is absent or None.
    """
    if isinstance(key, str):
        return str(_key_value(payload, key))
    return canonical_json([_key_value(payload, name) for name in key]).decode("utf-8")


def decode_key(key: str, spec: str | tuple[str, ...]) -> Any:
    """Undo `make_key` for the key fields in `spec`: the text itself, or a tuple of the parts."""
    if isinstance(spec, str):
        return key
    return tuple(json.loads(key))


@dataclass(frozen=True)
class Record(Mapping[str, Any]):
    """One keyed document. It reads like a mapping over its payload.

    `row_hash` is the hash of the payload. `moved_from` names the partition the key was in before,
    on the upsert of a record that moved to another partition.
    """

    key: str
    op: Op
    payload: Mapping[str, Any]
    row_hash: int
    moved_from: str | None = None

    def __getitem__(self, name: str) -> Any:
        return self.payload[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self.payload)

    def __len__(self) -> int:
        return len(self.payload)

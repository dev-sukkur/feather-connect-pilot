"""Identifiers: `new_id` for new things, `stable_id` for things that must get the same id every time."""

import base64
import hashlib
import threading
from secrets import randbits
from time import time_ns

# Crockford's base32, in ASCII order so that ids sort the same way as the numbers they encode.
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

_lock = threading.Lock()
_last = 0  # the last id handed out, as a 128-bit integer


def _encode(value: int) -> str:
    """Render a 128-bit integer as 26 characters, most significant first."""
    return "".join(_ALPHABET[(value >> shift) & 31] for shift in range(125, -1, -5))


def new_id() -> str:
    """Return a new ULID: 26 characters that sort by creation time.

    The top 48 bits are the Unix time in milliseconds and the other 80 are random. Ids from one
    process strictly increase, even within a millisecond or after the system clock steps back.
    """
    global _last
    candidate = ((time_ns() // 1_000_000) << 80) | randbits(80)
    with _lock:
        _last = max(candidate, _last + 1)
        value = _last
    return _encode(value)


def stable_id(*parts: str) -> str:
    """Return a 26-character id that depends only on `parts`, in order.

    Each part is length-prefixed before hashing, so ("ab", "c") and ("a", "bc") differ.
    """
    digest = hashlib.blake2b()
    for part in parts:
        data = part.encode("utf-8")
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return base64.b32encode(digest.digest()).decode("ascii")[:26]

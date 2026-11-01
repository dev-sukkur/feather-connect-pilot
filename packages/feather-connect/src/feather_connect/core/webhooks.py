"""Webhook signatures: HMAC-SHA256 over the timestamp and the raw body, with a tolerance against replays (design 13.5)."""

import hashlib
import hmac
from datetime import datetime, timedelta

_PREFIX = "sha256="
_MAX_STAMP_DIGITS = 12  # seconds since 1970 have 10 digits until the year 2286; more is noise, or an overflow


class SignatureInvalid(Exception):
    """A webhook is not what it says. The message says which check failed, for the server's log, and is never sent to
    the caller. It holds neither the secret nor the body."""


def sign(secret: str, body: bytes, timestamp: str | None) -> str:
    """Return the hex HMAC-SHA256 of `body`, bound to `timestamp` when there is one, so that it can't be moved."""
    signed = body if timestamp is None else timestamp.encode("utf-8") + b"." + body
    return hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()


def _check_timestamp(timestamp: str, now: datetime, tolerance: timedelta) -> None:
    if not (timestamp.isascii() and timestamp.isdigit() and len(timestamp) <= _MAX_STAMP_DIGITS):
        raise SignatureInvalid("the timestamp is not a number of seconds")
    if abs(now.timestamp() - int(timestamp)) > tolerance.total_seconds():
        raise SignatureInvalid("the timestamp is outside the tolerance")


def verify_hmac(
    secret: str,
    body: bytes,
    signature: str,
    timestamp: str | None,
    *,
    now: datetime,
    tolerance: timedelta = timedelta(minutes=5),
    require_timestamp: bool = True,
) -> None:
    """Raise `SignatureInvalid` unless `signature` is the one `sign` gives. It compares in constant time.

    A timestamp must be within `tolerance` of `now`, either way. A source whose scheme has no timestamp, and so no
    protection against replays, says so with `require_timestamp=False`. An empty secret never passes, since anybody
    can sign with one.
    """
    if not secret:
        raise SignatureInvalid("the secret is empty")
    if timestamp is None:
        if require_timestamp:
            raise SignatureInvalid("the timestamp is missing")
    else:
        _check_timestamp(timestamp, now, tolerance)
    expected = sign(secret, body, timestamp).encode("ascii")
    # As bytes, since `compare_digest` refuses text that isn't ASCII, and a header can hold anything.
    sent = signature.removeprefix(_PREFIX).encode("utf-8", "replace")
    if not hmac.compare_digest(expected, sent):
        raise SignatureInvalid("the signature does not match")

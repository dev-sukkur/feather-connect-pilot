"""Webhooks a flow package declares (flow packages §7.2 and §12, sponsor decision of 2026-11-09, options 1 and 2).

A package can't bring a webhook receiver written as code, because ingress runs in the server, which never loads
package code (the install's `webhook_connector` check). Instead it declares its webhooks in `feather.yaml`, and the
server's own ingress verifies, deduplicates and keeps each raw event; the package's transform parses it on a worker::

    webhooks:
      partner-orders:
        dataset: orders                       # the push dataset whose transform parses the events
        verify: {scheme: hmac-sha256, header: X-Signature, encoding: hex, prefix: "sha256="}
        secret: ${secret:db:partner-orders}
        key: $.id                             # what an event is kept once by
        stamp: $.updated_at                   # optional: the event's own time
        ack: 202                              # the status a kept event is answered with

The schemes are `hmac-sha256` and `hmac-sha1` (a header, `hex` or `base64`, an optional prefix), `shopify`
(`X-Shopify-Hmac-Sha256`, base64), `stripe` (`Stripe-Signature`, `t=…,v1=…`, with a timestamp tolerance against
replays), `zoho` and `bearer` (`Authorization: Bearer <secret>`). Every comparison takes constant time; a hex
signature compares without regard to case.

`zoho` is *unverified*: Zoho documents no one signature format for every product, so it is assumed to be HMAC-SHA256
of the body, hex, in `X-Zoho-Webhook-Signature`; check it against the product's own documentation before relying on
it. Any HMAC scheme may also name a `timestamp_header` (epoch seconds), which is then held to `tolerance` against
replays.

The header that carries the signature (or the bearer token) is never kept with the event.
"""

import base64
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

__all__ = [
    "SCHEMES",
    "UNVERIFIED_SCHEMES",
    "Malformed",
    "Rejected",
    "Unauthentic",
    "Webhook",
    "parse_webhooks",
    "pick",
    "signature_header",
    "verify",
]

Scheme = Literal["hmac-sha256", "hmac-sha1", "shopify", "stripe", "zoho", "bearer"]
SCHEMES: tuple[Scheme, ...] = ("hmac-sha256", "hmac-sha1", "shopify", "stripe", "zoho", "bearer")
UNVERIFIED_SCHEMES = frozenset({"zoho"})  # its format is assumed, not confirmed against every product
_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_DIGITS = re.compile(r"[0-9]+")
_PATH = re.compile(r"^\$(\.[A-Za-z_][A-Za-z0-9_-]*)+$")
DEFAULT_TOLERANCE_S = 300


class Rejected(Exception):
    """A request that isn't a genuine event of the webhook. The message is for the server's log, never sent back."""


class Unauthentic(Rejected):
    """Not signed by the sender, or replayed: answered 401."""


class Malformed(Rejected):
    """Signed, but not an event the webhook can keep (not JSON, or no key): answered 400."""


@dataclass(frozen=True)
class Webhook:
    """A declared webhook, checked."""

    name: str
    dataset: str
    scheme: Scheme
    secret: str  # a secret reference
    key: str
    stamp: str | None = None
    header: str | None = None
    encoding: Literal["hex", "base64"] = "hex"
    prefix: str = ""
    tolerance_s: int = DEFAULT_TOLERANCE_S
    ack: int = 202
    timestamp_header: str | None = None

    @property
    def unverified(self) -> bool:
        return self.scheme in UNVERIFIED_SCHEMES


def parse_webhooks(raw: Mapping[str, Any]) -> dict[str, Webhook]:
    """The webhooks of a package's `webhooks:` section, checked. Raises `ValueError` naming the first problem."""
    found: dict[str, Webhook] = {}
    for name, body in raw.items():
        where = f"webhooks.{name}"
        if not _NAME.match(name):
            raise ValueError(f"{where}: a webhook name has lower-case letters, digits and '-'")
        if not isinstance(body, dict):
            raise ValueError(f"{where}: must be a mapping")  # noqa: TRY004 - a config problem, as the others
        spec = cast(dict[str, Any], body)
        verify_spec = spec.get("verify")
        if not isinstance(verify_spec, dict):
            raise ValueError(  # noqa: TRY004 - a config problem
                f"{where}.verify: give the scheme, such as {{scheme: hmac-sha256, header: X-Signature}}"
            )
        v = cast(dict[str, Any], verify_spec)
        scheme = v.get("scheme")
        if scheme not in SCHEMES:
            raise ValueError(f"{where}.verify.scheme: use one of {', '.join(SCHEMES)}")
        if scheme in ("hmac-sha256", "hmac-sha1") and not isinstance(v.get("header"), str):
            raise ValueError(f"{where}.verify.header: name the header that holds the signature")
        encoding = v.get("encoding", "hex")
        if encoding not in ("hex", "base64"):
            raise ValueError(f"{where}.verify.encoding: hex or base64")
        secret = spec.get("secret")
        if not isinstance(secret, str) or not secret.startswith("${secret:"):
            raise ValueError(f"{where}.secret: a reference such as ${{secret:db:{name}}}")
        key, stamp = spec.get("key"), spec.get("stamp")
        if not isinstance(key, str) or not _PATH.match(key):
            raise ValueError(f"{where}.key: a path into the event such as $.id")
        if stamp is not None and (not isinstance(stamp, str) or not _PATH.match(stamp)):
            raise ValueError(f"{where}.stamp: a path into the event such as $.updated_at")
        ack = spec.get("ack", 202)
        if not isinstance(ack, int) or not 200 <= ack < 300:
            raise ValueError(f"{where}.ack: a 2xx status")
        dataset = spec.get("dataset")
        if not isinstance(dataset, str) or not dataset:
            raise ValueError(f"{where}.dataset: name the push dataset that parses its events")
        tolerance = v.get("tolerance", DEFAULT_TOLERANCE_S)
        if not isinstance(tolerance, int) or tolerance <= 0:
            raise ValueError(f"{where}.verify.tolerance: seconds, more than 0")
        stamp_header = v.get("timestamp_header")
        if stamp_header is not None and (not isinstance(stamp_header, str) or scheme in ("stripe", "bearer")):
            raise ValueError(f"{where}.verify.timestamp_header: a header name, for an HMAC scheme")
        found[name] = Webhook(
            name,
            dataset,
            scheme,
            secret,
            key,
            stamp,
            cast(str | None, v.get("header")),
            encoding,
            str(v.get("prefix") or ""),
            tolerance,
            ack,
            stamp_header,
        )
    return found


def signature_header(hook: Webhook) -> str:
    """The header that carries the webhook's signature or token, lower-case: never kept with an event."""
    default = {
        "shopify": "X-Shopify-Hmac-Sha256",
        "zoho": "X-Zoho-Webhook-Signature",
        "stripe": "Stripe-Signature",
        "bearer": "Authorization",
    }
    return (hook.header or default.get(hook.scheme) or "").lower()


def _mac(secret: str, payload: bytes, digest: str) -> bytes:
    return hmac.new(secret.encode(), payload, getattr(hashlib, digest)).digest()


def _encoded(mac: bytes, encoding: str) -> str:
    return mac.hex() if encoding == "hex" else base64.b64encode(mac).decode()


def _same(given: str, expected: str, *, hex_digits: bool = False) -> bool:
    """Compare in constant time; hex without regard to case."""
    given = given.strip()
    if hex_digits:
        given, expected = given.lower(), expected.lower()
    return hmac.compare_digest(given.encode(), expected.encode())


def _header(headers: Mapping[str, str], name: str) -> str:
    value = headers.get(name.lower())
    if value is None:
        raise Unauthentic(f"no {name} header")
    return value


def _fresh(hook: Webhook, stamp: str, now: datetime) -> None:
    if not _DIGITS.fullmatch(stamp):
        raise Unauthentic("the event's timestamp is not a number of seconds")
    if abs(now.timestamp() - int(stamp)) > hook.tolerance_s:
        raise Unauthentic(
            f"the event's timestamp is more than {hook.tolerance_s} s away: a replay, or a clock off"
        )


def verify(hook: Webhook, secret: str, headers: Mapping[str, str], body: bytes, now: datetime) -> None:
    """Raise `Rejected` unless the request is signed as the webhook's scheme says. `headers` have lower-case names."""
    match hook.scheme:
        case "hmac-sha256" | "hmac-sha1":
            assert hook.header is not None
            given = _header(headers, hook.header)
            if hook.prefix:
                if not given.startswith(hook.prefix):
                    raise Unauthentic(f"{hook.header} doesn't start with {hook.prefix}")
                given = given[len(hook.prefix) :]
            digest = "sha256" if hook.scheme == "hmac-sha256" else "sha1"
            ok = _same(
                given, _encoded(_mac(secret, body, digest), hook.encoding), hex_digits=hook.encoding == "hex"
            )
        case "shopify":
            given = _header(headers, hook.header or "X-Shopify-Hmac-Sha256")
            ok = _same(given, _encoded(_mac(secret, body, "sha256"), "base64"))
        case "zoho":
            given = _header(headers, hook.header or "X-Zoho-Webhook-Signature")
            ok = _same(
                given,
                _encoded(_mac(secret, body, "sha256"), hook.encoding),
                hex_digits=hook.encoding == "hex",
            )
        case "stripe":
            ok = _stripe(hook, secret, _header(headers, hook.header or "Stripe-Signature"), body, now)
        case "bearer":
            given = _header(headers, hook.header or "Authorization")
            ok = _same(given, f"Bearer {secret}")
    if not ok:
        raise Unauthentic("the signature doesn't match")
    if hook.timestamp_header is not None:
        _fresh(hook, _header(headers, hook.timestamp_header).strip(), now)


def _stripe(hook: Webhook, secret: str, header: str, body: bytes, now: datetime) -> bool:
    parts = [p.split("=", 1) for p in header.split(",") if "=" in p]
    stamps = [v for k, v in parts if k.strip() == "t"]
    signatures = [v for k, v in parts if k.strip() == "v1"]
    if len(stamps) != 1 or not _DIGITS.fullmatch(stamps[0].strip()) or not signatures:
        raise Unauthentic("Stripe-Signature has no t= and v1=")
    stamp = stamps[0].strip()
    _fresh(hook, stamp, now)
    expected = _mac(secret, f"{stamp}.".encode() + body, "sha256").hex()
    matched = False
    for signature in signatures:  # every one compared, in constant time each
        matched = _same(signature, expected, hex_digits=True) or matched
    return matched


def pick(body: bytes, path: str) -> str:
    """The value at `path` (`$.a.b`) of a JSON event, as text. Raises `Rejected` when it isn't there."""
    try:
        node: Any = json.loads(body)
    except ValueError:
        raise Malformed("the event is not JSON") from None
    for part in path[2:].split("."):
        if not isinstance(node, dict) or part not in node:
            raise Malformed(f"the event has no {path}")
        node = cast(dict[str, Any], node)[part]
    if node is None or isinstance(node, dict | list):
        raise Malformed(f"{path} is not a value")
    return str(node)

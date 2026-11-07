"""Previews of mass actions. A preview says what an action would do and hands out a token that does it, once.

The token is signed with the server's secret key and expires after ten minutes. It carries a fingerprint of the
selection as the preview saw it, and the service computes the fingerprint again when the token is redeemed: if what the
action would touch has moved on, the token is refused with `PreviewStale`, and nobody acts on a picture that is out of
date. The selection itself is kept server side, in `store.overrides` under the target `preview`, and a token that was
used is marked there under `preview-used`, claimed atomically so that two requests with one token cannot both win.
"""

import base64
import hashlib
import hmac
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from feather_connect.clock import Clock
from feather_connect.ids import new_id
from feather_connect.storage.port import Store

TOKEN_TTL = timedelta(minutes=10)
_SELECTION = "preview"
_USED = "preview-used"


class PreviewError(Exception):
    """A confirmation token can't be used."""


class PreviewInvalid(PreviewError):
    """The token is not one this server issued for this action."""


class PreviewExpired(PreviewError):
    """The token is older than ten minutes."""


class PreviewUsed(PreviewError):
    """The token has been used already."""


class PreviewStale(PreviewError):
    """What the action would touch has changed since the preview. Preview again."""


@dataclass(frozen=True)
class Impact:
    """What an action would touch. `estimated_budget_units` is None when the size or the cost is not known."""

    partitions: int
    deliveries: int
    estimated_budget_units: int | None
    samples: tuple[str, ...]


@dataclass(frozen=True)
class Preview:
    token: str
    action: str
    impact: Impact


def fingerprint(*parts: Any) -> str:
    """A hash of `parts`, which must be JSON (a datetime or the like is written as text)."""
    text = json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode()).hexdigest()


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


class PreviewTokens:
    def __init__(self, store: Store, secret_key: str, clock: Clock) -> None:
        if not secret_key:
            raise ValueError("the server has no secret key to sign confirmation tokens with")
        self._store, self._key, self._clock = store, secret_key.encode(), clock

    def _sign(self, body: str) -> str:
        return _b64(hmac.new(self._key, b"preview:" + body.encode(), hashlib.sha256).digest())

    def issue(self, action: str, actor: str, selection: Mapping[str, Any], fingerprint: str) -> str:
        """Make a token for `action` on `selection`, as the fingerprint says it is now."""
        jti, expires = new_id(), self._clock.now() + TOKEN_TTL
        self._store.overrides.set(
            _SELECTION, jti, {"selection": dict(selection)}, actor=actor, reason=action, expires_at=expires
        )
        claims = {"a": action, "j": jti, "h": fingerprint, "e": int(expires.timestamp())}
        body = _b64(json.dumps(claims, separators=(",", ":")).encode())
        return f"{body}.{self._sign(body)}"

    @staticmethod
    def action_of(token: str) -> str:
        """The action a token says it confirms, before it is checked: redeeming it for that action checks it."""
        try:
            return str(json.loads(_unb64(token.partition(".")[0]))["a"])
        except (ValueError, KeyError, TypeError):
            raise PreviewInvalid("this is not a confirmation token from this server") from None

    def redeem(
        self, token: str, action: str, actor: str, fingerprint_of: Callable[[Mapping[str, Any]], str]
    ) -> Mapping[str, Any]:
        """Check the token for `action`, use it up and return the selection it was made for.

        `fingerprint_of` computes the fingerprint of a selection as it stands now. Raises a `PreviewError`; a token
        that fails any check but `PreviewUsed` is left unused.
        """
        body, _, signature = token.partition(".")
        sent = signature.encode("utf-8", "surrogateescape")  # as bytes: text that isn't ASCII raises (M3)
        if not body or not hmac.compare_digest(sent, self._sign(body).encode()):
            raise PreviewInvalid("this is not a confirmation token from this server")
        try:
            claims = json.loads(_unb64(body))
            token_action, jti, expected, expires = claims["a"], claims["j"], claims["h"], claims["e"]
        except (ValueError, KeyError, TypeError):
            raise PreviewInvalid("this is not a confirmation token from this server") from None
        if token_action != action:
            raise PreviewInvalid(f"this token confirms {token_action!r}, not {action!r}")
        expires_at = datetime.fromtimestamp(expires, UTC)
        if self._clock.now() >= expires_at:
            raise PreviewExpired("the confirmation token has expired: preview again")
        kept = self._store.overrides.get(_SELECTION, jti)
        if kept is None:
            raise PreviewInvalid("the server doesn't know this preview")
        selection: Mapping[str, Any] = kept["selection"]
        if fingerprint_of(selection) != expected:
            raise PreviewStale("what this would affect has changed since the preview: preview again")
        if not self._store.overrides.claim(_USED, jti, True, actor=actor, expires_at=expires_at):
            raise PreviewUsed("this confirmation token has been used already")
        return selection

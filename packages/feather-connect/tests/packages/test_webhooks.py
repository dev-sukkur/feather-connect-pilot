"""Declared webhooks (flow packages §7.2): each scheme checked against a known vector, in constant time, with replay
protection where the scheme has a timestamp."""

import base64
from datetime import UTC, datetime

import pytest

from feather_connect.packages.webhooks import (
    Malformed,
    Rejected,
    Unauthentic,
    Webhook,
    parse_webhooks,
    pick,
    signature_header,
    verify,
)

NOW = datetime(2026, 11, 9, 9, tzinfo=UTC)
# RFC 4231 test case 2 and RFC 2202 test case 2: key "Jefe".
KEY, DATA = "Jefe", b"what do ya want for nothing?"
SHA256 = "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
SHA1 = "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79"


def _hook(scheme: str, **more: object) -> Webhook:
    raw = {
        "dataset": "orders",
        "verify": {"scheme": scheme, **more},
        "secret": "${secret:db:h}",
        "key": "$.id",
    }
    return parse_webhooks({"h": raw})["h"]


@pytest.mark.parametrize(
    ("hook", "headers"),
    [
        (_hook("hmac-sha256", header="X-Signature"), {"x-signature": SHA256}),
        (
            _hook("hmac-sha256", header="X-Hub-Signature-256", prefix="sha256="),
            {"x-hub-signature-256": f"sha256={SHA256}"},
        ),
        (
            _hook("hmac-sha256", header="X-Signature", encoding="base64"),
            {"x-signature": base64.b64encode(bytes.fromhex(SHA256)).decode()},
        ),
        (_hook("hmac-sha1", header="X-Signature"), {"x-signature": SHA1}),
        (_hook("shopify"), {"x-shopify-hmac-sha256": base64.b64encode(bytes.fromhex(SHA256)).decode()}),
        (_hook("zoho"), {"x-zoho-webhook-signature": SHA256}),
        (_hook("bearer"), {"authorization": "Bearer Jefe"}),
    ],
    ids=["hmac-sha256", "prefixed", "base64", "hmac-sha1", "shopify", "zoho", "bearer"],
)
def test_each_scheme_takes_its_known_vector_and_refuses_a_changed_one(hook: Webhook, headers: dict[str, str]):
    verify(hook, KEY, headers, DATA, NOW)
    with pytest.raises(Rejected):
        verify(hook, KEY, headers, DATA + b"!", NOW) if hook.scheme != "bearer" else verify(
            hook, "other", headers, DATA, NOW
        )
    with pytest.raises(Rejected, match="header"):
        verify(hook, KEY, {}, DATA, NOW)


def test_stripe_signs_the_timestamp_and_refuses_a_replay():
    import hashlib
    import hmac

    hook = _hook("stripe", tolerance=300)
    stamp = int(NOW.timestamp())
    body = b'{"id": "evt_1"}'
    good = hmac.new(b"whsec_test", f"{stamp}.".encode() + body, hashlib.sha256).hexdigest()
    verify(
        hook, "whsec_test", {"stripe-signature": f"t={stamp},v1=00ff,v1={good}"}, body, NOW
    )  # any v1 may match
    with pytest.raises(Rejected, match="doesn't match"):
        verify(hook, "whsec_test", {"stripe-signature": f"t={stamp + 1},v1={good}"}, body, NOW)
    late = datetime.fromtimestamp(stamp + 301, UTC)
    with pytest.raises(Rejected, match="replay"):
        verify(hook, "whsec_test", {"stripe-signature": f"t={stamp},v1={good}"}, body, late)
    with pytest.raises(Rejected, match="t= and v1="):
        verify(hook, "whsec_test", {"stripe-signature": "v1=abc"}, body, NOW)


@pytest.mark.parametrize(
    ("raw", "why"),
    [
        ({"verify": {"scheme": "md5"}}, "scheme"),
        ({"verify": {"scheme": "hmac-sha256"}}, "header"),
        ({"verify": {"scheme": "bearer"}, "secret": "plain"}, "secret"),
        ({"verify": {"scheme": "bearer"}, "key": "id"}, "key"),
        ({"verify": {"scheme": "bearer"}, "ack": 500}, "ack"),
        ({"verify": {"scheme": "bearer"}, "dataset": ""}, "dataset"),
    ],
)
def test_a_declaration_that_isnt_valid_is_named(raw: dict[str, object], why: str):
    base = {"dataset": "orders", "secret": "${secret:db:h}", "key": "$.id"}
    with pytest.raises(ValueError, match=why):
        parse_webhooks({"h": {**base, **raw}})


def test_a_key_is_picked_from_the_event():
    assert pick(b'{"order": {"id": 42}}', "$.order.id") == "42"
    with pytest.raises(Rejected, match="no"):
        pick(b'{"order": {}}', "$.order.id")
    with pytest.raises(Rejected, match="not JSON"):
        pick(b"<xml/>", "$.id")


def test_a_refusal_says_whether_it_is_unauthentic_or_malformed():
    hook = _hook("hmac-sha256", header="X-Signature")
    with pytest.raises(Unauthentic):
        verify(hook, KEY, {"x-signature": "00"}, DATA, NOW)
    with pytest.raises(Malformed):
        pick(b"<xml/>", "$.id")
    assert issubclass(Unauthentic, Rejected) and issubclass(Malformed, Rejected)


def test_hex_compares_without_regard_to_case():
    verify(_hook("hmac-sha256", header="X-Signature"), KEY, {"x-signature": SHA256.upper()}, DATA, NOW)
    verify(_hook("zoho"), KEY, {"x-zoho-webhook-signature": SHA256.upper()}, DATA, NOW)


def test_a_stripe_timestamp_is_ascii_digits_only():
    hook = _hook("stripe")
    arabic = "".join(chr(0x0660 + int(d)) for d in str(int(NOW.timestamp())))  # str.isdigit() takes these
    with pytest.raises(Unauthentic, match="t= and v1="):
        verify(hook, "whsec", {"stripe-signature": f"t={arabic},v1=00"}, b"{}", NOW)


def test_zoho_is_labelled_unverified_and_may_hold_a_timestamp_to_a_tolerance():
    assert _hook("zoho").unverified and not _hook("shopify").unverified
    hook = _hook("zoho", timestamp_header="X-Zoho-Timestamp", tolerance=60)
    now = str(int(NOW.timestamp()))
    verify(hook, KEY, {"x-zoho-webhook-signature": SHA256, "x-zoho-timestamp": now}, DATA, NOW)
    with pytest.raises(Unauthentic, match="replay"):
        late = str(int(NOW.timestamp()) - 61)
        verify(hook, KEY, {"x-zoho-webhook-signature": SHA256, "x-zoho-timestamp": late}, DATA, NOW)
    with pytest.raises(Unauthentic, match="header"):
        verify(hook, KEY, {"x-zoho-webhook-signature": SHA256}, DATA, NOW)
    with pytest.raises(ValueError, match="timestamp_header"):
        _hook("bearer", timestamp_header="X-T")


def test_each_scheme_names_the_header_that_is_never_kept():
    assert signature_header(_hook("bearer")) == "authorization"
    assert signature_header(_hook("bearer", header="X-Token")) == "x-token"
    assert signature_header(_hook("shopify")) == "x-shopify-hmac-sha256"
    assert signature_header(_hook("stripe")) == "stripe-signature"
    assert signature_header(_hook("hmac-sha1", header="X-Hub-Signature")) == "x-hub-signature"

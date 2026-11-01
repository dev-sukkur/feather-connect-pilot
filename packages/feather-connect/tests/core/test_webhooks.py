from datetime import UTC, datetime, timedelta

import pytest

from feather_connect.core.webhooks import SignatureInvalid, sign, verify_hmac

NOW = datetime(2026, 10, 18, 9, 0, tzinfo=UTC)
STAMP = str(int(NOW.timestamp()))
BODY = b'{"id": 1}'


def test_a_good_signature_passes():
    verify_hmac("s3cret", BODY, sign("s3cret", BODY, STAMP), STAMP, now=NOW)


def test_the_common_sha256_prefix_is_accepted():
    verify_hmac("s3cret", BODY, "sha256=" + sign("s3cret", BODY, STAMP), STAMP, now=NOW)


@pytest.mark.parametrize(
    ("secret", "body", "stamp"),
    [("other", BODY, STAMP), ("s3cret", b"changed", STAMP), ("s3cret", BODY, str(int(NOW.timestamp()) + 1))],
)
def test_anything_changed_fails(secret: str, body: bytes, stamp: str):
    signature = sign("s3cret", BODY, STAMP)
    with pytest.raises(SignatureInvalid):
        verify_hmac(secret, body, signature, stamp, now=NOW)


@pytest.mark.parametrize("age", [timedelta(minutes=6), timedelta(minutes=-6)])
def test_a_timestamp_outside_the_tolerance_fails_even_when_signed(age: timedelta):
    stamp = str(int((NOW - age).timestamp()))
    with pytest.raises(SignatureInvalid, match="tolerance"):
        verify_hmac("s3cret", BODY, sign("s3cret", BODY, stamp), stamp, now=NOW)


def test_a_timestamp_within_the_tolerance_passes():
    stamp = str(int((NOW - timedelta(minutes=4)).timestamp()))
    verify_hmac("s3cret", BODY, sign("s3cret", BODY, stamp), stamp, now=NOW)


def test_a_missing_timestamp_is_allowed_only_when_configured():
    signature = sign("s3cret", BODY, None)
    with pytest.raises(SignatureInvalid, match="timestamp"):
        verify_hmac("s3cret", BODY, signature, None, now=NOW)
    verify_hmac("s3cret", BODY, signature, None, now=NOW, require_timestamp=False)


@pytest.mark.parametrize("stamp", ["soon", "", "1e9", "nan", "٣", "9" * 40])
def test_a_timestamp_that_is_no_number_of_seconds_fails(stamp: str):
    with pytest.raises(SignatureInvalid):
        verify_hmac("s3cret", BODY, sign("s3cret", BODY, stamp), stamp, now=NOW)


@pytest.mark.parametrize("signature", ["", "wrong", "é" * 64, "sha256="])
def test_a_malformed_signature_fails_rather_than_raising_anything_else(signature: str):
    with pytest.raises(SignatureInvalid):
        verify_hmac("s3cret", BODY, signature, STAMP, now=NOW)


def test_an_empty_secret_never_passes():
    with pytest.raises(SignatureInvalid):
        verify_hmac("", BODY, sign("", BODY, STAMP), STAMP, now=NOW)


def test_the_message_holds_neither_the_secret_nor_the_body():
    with pytest.raises(SignatureInvalid) as error:
        verify_hmac("s3cret", BODY, "wrong", STAMP, now=NOW)
    assert "s3cret" not in str(error.value) and '"id"' not in str(error.value)

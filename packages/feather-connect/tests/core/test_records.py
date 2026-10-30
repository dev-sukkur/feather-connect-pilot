from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType

import pytest
from hypothesis import given
from hypothesis import strategies as st

from feather_connect.core.errors import MissingKeyError
from feather_connect.core.records import Record, canonical_json, decode_key, make_key, row_hash


def test_row_hash_ignores_key_order_and_detects_change():
    assert row_hash({"a": 1, "b": 2}) == row_hash({"b": 2, "a": 1})
    assert row_hash({"a": 1}) != row_hash({"a": 2})
    assert 0 <= row_hash({"a": 1}) < 2**64


def test_hashes_and_canonical_json_never_change_between_releases():
    # Row hashes are stored and compared with later reads, so these values must stay the same for good.
    # The hash is the 8-byte BLAKE2b digest of b'{"a":1}' (hex 1f8c00645b7df6ce) as a big-endian integer.
    assert row_hash({"a": 1}) == 2273192342946969294
    assert canonical_json({"n": "é"}) == b'{"n":"\\u00e9"}'  # ASCII only, whatever the text


def test_canonical_json_handles_non_json_types():
    v = {
        "d": Decimal("1.10"),
        "t": datetime(2026, 10, 18, 5, tzinfo=UTC),
        "day": date(2026, 10, 18),
        "b": b"\x01",
        "n": [1, {"x": None}],
    }
    assert canonical_json(v) == (
        b'{"b":"AQ==","d":"1.10","day":"2026-10-18","n":[1,{"x":null}],"t":"2026-10-18T05:00:00+00:00"}'
    )


def test_canonical_json_leaves_a_naive_datetime_as_it_is():
    assert canonical_json({"t": datetime(2026, 10, 18, 5)}) == b'{"t":"2026-10-18T05:00:00"}'


def test_canonical_json_rejects_types_it_cannot_encode():
    with pytest.raises(TypeError, match="set"):
        canonical_json({"s": {1, 2}})


def test_equal_instants_hash_the_same():
    ist = timezone(timedelta(hours=5, minutes=30))
    assert row_hash({"t": datetime(2026, 10, 18, 5, tzinfo=UTC)}) == row_hash(
        {"t": datetime(2026, 10, 18, 10, 30, tzinfo=ist)}
    )


def test_row_hash_accepts_any_mapping():
    payload = {"a": 1, "inner": {"x": Decimal("2.5")}}
    proxies = MappingProxyType({"a": 1, "inner": MappingProxyType({"x": Decimal("2.5")})})
    assert row_hash(proxies) == row_hash(payload)
    assert row_hash(Record("k", "upsert", payload, 0)) == row_hash(payload)


def test_make_and_decode_key():
    assert make_key({"id": 7}, "id") == "7"
    assert make_key({"m": "M2", "p": "RET"}, ("m", "p")) == '["M2","RET"]'
    assert decode_key('["M2","RET"]', ("m", "p")) == ("M2", "RET")
    assert decode_key("7", "id") == "7"


@pytest.mark.parametrize("payload", [{}, {"id": None}])
def test_make_key_rejects_missing(payload):
    with pytest.raises(MissingKeyError):
        make_key(payload, "id")


@pytest.mark.parametrize("payload", [{"m": "M2"}, {"m": "M2", "p": None}])
def test_make_key_rejects_a_composite_key_with_a_missing_part(payload):
    with pytest.raises(MissingKeyError, match="'p'"):
        make_key(payload, ("m", "p"))


def test_composite_key_parts_are_encoded_like_payload_values():
    assert make_key({"d": date(2026, 10, 18), "n": 3}, ("d", "n")) == '["2026-10-18",3]'


def test_record_reads_like_a_mapping():
    r = Record("k", "upsert", {"material": "M1"}, 1)
    assert r["material"] == "M1" and list(r) == ["material"]


def test_record_is_as_long_as_its_payload():
    r = Record("k", "upsert", {"material": "M1", "plant": "P1"}, 1)
    assert len(r) == 2 and dict(r) == {"material": "M1", "plant": "P1"}


def test_record_equality_compares_every_field_not_only_the_payload():
    r = Record("a", "upsert", {"x": 1}, 7)
    assert r == Record("a", "upsert", {"x": 1}, 7)
    assert r != Record("b", "upsert", {"x": 1}, 7)
    assert r != Record("a", "delete", {"x": 1}, 7)
    assert r != Record("a", "upsert", {"x": 1}, 7, moved_from="p")


def test_record_is_frozen():
    r = Record("k", "upsert", {}, 1)
    with pytest.raises(FrozenInstanceError):
        r.op = "delete"  # pyright: ignore[reportAttributeAccessIssue]


@given(
    st.recursive(
        st.none() | st.booleans() | st.integers() | st.text(),
        lambda c: st.lists(c) | st.dictionaries(st.text(), c),
    )
)
def test_canonical_json_is_deterministic(v):
    assert canonical_json({"v": v}) == canonical_json({"v": v})

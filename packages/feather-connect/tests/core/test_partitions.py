from datetime import UTC, date, datetime

import pytest

from feather_connect.core.errors import PartitionError
from feather_connect.core.partitions import Bound, by_bucket, by_day, by_fields, by_month, none

IST = "Asia/Kolkata"
NOW = datetime(2026, 10, 18, 9, 30, tzinfo=UTC)  # 15:00 on 18 Oct in India


def test_none_has_one_partition():
    assert none().key_of({"x": 1}) == "_" and none().recent(NOW) == ["_"]


def test_by_day_uses_the_explicit_timezone():
    s = by_day("date", tz=IST)
    assert s.key_of({"date": "2026-10-17T20:00:00Z"}) == "date=2026-10-18"
    assert s.key_of({"date": date(2026, 10, 18)}) == "date=2026-10-18"
    assert s.key_of({"date": datetime(2026, 10, 18, 1, 0)}) == "date=2026-10-18"  # naive means scheme tz


def test_by_fields_composes_and_labels_in_business_words():
    s = by_fields("store_id", by_day("date", tz=IST))
    key = s.key_of({"store_id": 1509, "date": "2026-10-18"})
    assert key == "store_id=1509/date=2026-10-18"
    assert s.label_of(key) == "Store 1509 · 18 Oct 2026"
    assert s.recent(NOW) is None  # stores can't be enumerated


def test_by_bucket_is_stable():
    s = by_bucket("id", 8)
    assert s.key_of({"id": "A"}) == s.key_of({"id": "A"})
    assert s.key_of({"id": "A"}).startswith("bucket=")
    assert s.recent(NOW) == [f"bucket={i}" for i in range(8)]


def test_by_day_recent_is_today_and_yesterday_in_scheme_tz():
    now = datetime(2026, 10, 17, 20, 0, tzinfo=UTC)  # already 18 Oct in India
    assert by_day("date", tz=IST).recent(now) == ["date=2026-10-17", "date=2026-10-18"]


def test_bounds_for_pushdown():
    b = by_day("date", tz=IST).bounds("date=2026-10-18")["date"]
    assert b.low == date(2026, 10, 18) and b.high == date(2026, 10, 19)


@pytest.mark.parametrize("payload", [{}, {"date": None}, {"date": "not a date"}])
def test_bad_partition_field_raises(payload):
    with pytest.raises(PartitionError):
        by_day("date", tz=IST).key_of(payload)


# Beyond the brief: none


def test_none_labels_its_one_partition_and_pushes_nothing_down():
    assert none().label_of("_") == "All" and none().bounds("_") == {}


# Beyond the brief: by_day


def test_by_day_converts_an_aware_datetime_to_the_scheme_timezone():
    s = by_day("date", tz=IST)
    assert s.key_of({"date": datetime(2026, 10, 17, 20, 0, tzinfo=UTC)}) == "date=2026-10-18"
    assert s.key_of({"date": datetime(2026, 10, 18, 20, 0, tzinfo=UTC)}) == "date=2026-10-19"


def test_by_day_label_has_no_leading_zero():
    assert by_day("date", tz=IST).label_of("date=2026-10-05") == "5 Oct 2026"


def test_by_day_recent_takes_a_naive_now_as_scheme_time():
    # Late evening in India, which would already be tomorrow if the naive time were read as UTC.
    assert by_day("date", tz=IST).recent(datetime(2026, 10, 18, 22, 0)) == [
        "date=2026-10-17",
        "date=2026-10-18",
    ]


@pytest.mark.parametrize("value", [20261018, True, "", "2026-13-45", "9999-12-31T23:00:00Z"])
def test_a_value_that_is_not_a_date_raises(value):
    # The last one is a real day in UTC, but past the last day that datetime can hold in India.
    with pytest.raises(PartitionError, match="'date'"):
        by_day("date", tz=IST).key_of({"date": value})


# Beyond the brief: by_month


def test_by_month_keys_and_labels_by_month_in_the_scheme_timezone():
    s = by_month("date", tz=IST)
    assert s.key_of({"date": "2026-09-30T20:00:00Z"}) == "date=2026-10"  # already 1 Oct in India
    assert s.key_of({"date": date(2026, 10, 18)}) == "date=2026-10"
    assert s.label_of("date=2026-10") == "Oct 2026"


def test_by_month_bounds_run_to_the_first_of_the_next_month():
    s = by_month("date", tz=IST)
    assert s.bounds("date=2026-10") == {"date": Bound(date(2026, 10, 1), date(2026, 11, 1))}
    assert s.bounds("date=2026-12") == {"date": Bound(date(2026, 12, 1), date(2027, 1, 1))}


def test_by_month_recent_is_this_month_and_last_month_in_scheme_tz():
    s = by_month("date", tz=IST)
    assert s.recent(datetime(2026, 10, 31, 20, 0, tzinfo=UTC)) == ["date=2026-10", "date=2026-11"]
    assert s.recent(datetime(2026, 1, 15, tzinfo=UTC)) == ["date=2025-12", "date=2026-01"]


# Beyond the brief: by_fields


def test_a_plain_field_is_keyed_by_its_text():
    s = by_fields("store_id")
    assert s.key_of({"store_id": 1509}) == s.key_of({"store_id": "1509"}) == "store_id=1509"
    assert s.bounds("store_id=1509") == {"store_id": Bound("1509", "1509")}  # equality: low == high


@pytest.mark.parametrize("payload", [{}, {"store_id": None}])
def test_a_missing_plain_field_raises(payload):
    with pytest.raises(PartitionError, match="'store_id'"):
        by_fields("store_id").key_of(payload)


def test_by_fields_bounds_limit_every_part():
    s = by_fields("store_id", by_day("date", tz=IST))
    assert s.bounds("store_id=1509/date=2026-10-18") == {
        "store_id": Bound("1509", "1509"),
        "date": Bound(date(2026, 10, 18), date(2026, 10, 19)),
    }


def test_by_fields_recent_lists_every_combination_when_each_part_can_list_its_own():
    s = by_fields(by_day("date", tz=IST), by_bucket("id", 2))
    assert s.recent(NOW) == [
        "date=2026-10-17/bucket=0",
        "date=2026-10-17/bucket=1",
        "date=2026-10-18/bucket=0",
        "date=2026-10-18/bucket=1",
    ]


def test_by_fields_flattens_a_nested_scheme_into_its_parts():
    s = by_fields(by_fields("a_id", "b"), "c")
    key = s.key_of({"a_id": 1, "b": 2, "c": 3})
    assert key == "a_id=1/b=2/c=3"
    assert s.label_of(key) == "A 1 · B 2 · C 3"


def test_labels_read_underscores_as_spaces():
    assert by_fields("sales_region").label_of("sales_region=North") == "Sales Region North"


def test_a_slash_or_percent_in_a_value_stays_inside_its_part():
    s = by_fields("region", by_day("date", tz=IST))
    key = s.key_of({"region": "north/east 100%", "date": "2026-10-18"})
    assert key == "region=north%2Feast 100%25/date=2026-10-18"
    assert s.label_of(key) == "Region north/east 100% · 18 Oct 2026"
    assert s.bounds(key)["region"] == Bound("north/east 100%", "north/east 100%")


def test_by_fields_needs_a_part():
    with pytest.raises(ValueError, match="at least one"):
        by_fields()


# Beyond the brief: by_bucket


def test_bucket_numbers_never_change_between_releases():
    # Bucket numbers are stored in keys, so a value must land in the same bucket for good. These were
    # worked out with hashlib alone: blake2b with an 8-byte digest of the value's JSON, read as a
    # big-endian integer, modulo n.
    eight = by_bucket("id", 8)
    assert [eight.key_of({"id": v}) for v in ("A", "B", "C", 1509)] == [
        "bucket=4",
        "bucket=5",
        "bucket=2",
        "bucket=1",
    ]
    assert by_bucket("id", 5).key_of({"id": "A"}) == "bucket=2"


def test_every_bucket_key_is_one_that_recent_lists():
    s = by_bucket("id", 8)
    assert {s.key_of({"id": i}) for i in range(200)} == set(s.recent(NOW) or [])


@pytest.mark.parametrize("payload", [{}, {"id": None}, {"id": {1, 2}}])
def test_a_bucket_field_that_cannot_be_hashed_raises(payload):
    with pytest.raises(PartitionError, match="'id'"):
        by_bucket("id", 8).key_of(payload)


def test_by_bucket_needs_at_least_one_bucket():
    with pytest.raises(ValueError, match="at least one"):
        by_bucket("id", 0)


def test_by_bucket_labels_its_buckets_and_pushes_nothing_down():
    s = by_bucket("id", 8)
    assert s.label_of("bucket=3") == "Bucket 3" and s.bounds("bucket=3") == {}
    with pytest.raises(ValueError, match="other=1"):
        s.label_of("other=1")


# Beyond the brief: keys that belong to another scheme


@pytest.mark.parametrize("scheme", [by_day("date", tz=IST), by_month("date", tz=IST), by_fields("store_id")])
def test_a_key_of_another_scheme_is_refused(scheme):
    with pytest.raises(ValueError, match="other=1"):
        scheme.label_of("other=1")
    with pytest.raises(ValueError, match="other=1"):
        scheme.bounds("other=1")


def test_a_composite_key_with_the_wrong_number_of_parts_is_refused():
    with pytest.raises(ValueError, match="2 parts"):
        by_fields("a", "b").label_of("a=1")

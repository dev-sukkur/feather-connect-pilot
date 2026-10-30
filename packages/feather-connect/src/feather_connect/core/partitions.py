"""Partition schemes: how a dataset is cut into slices such as store and day, and how each slice is named."""

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import product
from typing import Any, Protocol
from urllib.parse import unquote
from zoneinfo import ZoneInfo

from feather_connect.core.errors import PartitionError
from feather_connect.core.records import canonical_json

_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_BUCKET = "bucket"


@dataclass(frozen=True)
class Bound:
    """A limit on one field. `low` is included and `high` is not.

    `low == high` means the field equals that value.
    """

    low: Any
    high: Any


class PartitionScheme(Protocol):
    """How a dataset is cut into partitions, each named by a key such as `store_id=1509/date=2026-10-18`."""

    def key_of(self, payload: Mapping[str, Any]) -> str:
        """Return the key of the partition a record belongs to. Raises `PartitionError` if it has none."""
        ...

    def label_of(self, key: str) -> str:
        """Return `key` in business words, such as `Store 1509` for `store_id=1509`."""
        ...

    def bounds(self, key: str) -> Mapping[str, Bound]:
        """Return the limits on record fields that select this partition, for a source to apply as it reads.

        A field that can't be limited is left out, so a source must still check `key_of` on what it reads.
        """
        ...

    def recent(self, now: datetime) -> list[str] | None:
        """Return the keys to plan by default at `now`, or None when the scheme can't list its own partitions."""
        ...


def _field_value(payload: Mapping[str, Any], field: str) -> Any:
    value = payload.get(field)
    if value is None:
        raise PartitionError(f"record has no value for partition field {field!r}")
    return value


def _not_a_date(field: str, value: Any) -> PartitionError:
    return PartitionError(f"partition field {field!r} can't be read as a date: {value!r}")


def _local_date(moment: datetime, zone: ZoneInfo) -> date:
    """Return the calendar day of `moment` in `zone`. A datetime without a timezone is taken to be in `zone`."""
    if moment.utcoffset() is None:
        return moment.date()
    return moment.astimezone(zone).date()


def _date_field(payload: Mapping[str, Any], field: str, zone: ZoneInfo) -> date:
    """Read a day from a date, a datetime or an ISO-8601 string."""
    value = _field_value(payload, field)
    try:
        moment = datetime.fromisoformat(value) if isinstance(value, str) else value
        if isinstance(moment, datetime):  # before date, because a datetime is also a date
            return _local_date(moment, zone)
        if isinstance(moment, date):
            return moment
    except (ValueError, OverflowError) as e:  # an overflow: 9999-12-31T23:00Z has no day east of UTC
        raise _not_a_date(field, value) from e
    raise _not_a_date(field, value)


def _escape(text: str) -> str:
    """Make `text` safe to use as the value in a key, where `/` separates the parts of a composite key."""
    return text.replace("%", "%25").replace("/", "%2F")


def _value_of(key: str, name: str) -> str:
    """Return what follows `name=` in `key`, which must be a key of that form."""
    prefix = f"{name}="
    if not key.startswith(prefix):
        raise ValueError(f"{key!r} is not a {name!r} partition key")
    return key.removeprefix(prefix)


def _words(field: str) -> str:
    """Return a field name in business words: `store_id` becomes `Store` and `sales_region` `Sales Region`."""
    stem = field.removesuffix("_id") or field
    return stem.replace("_", " ").strip().title()


@dataclass(frozen=True)
class _Unpartitioned(PartitionScheme):
    """The whole dataset in one partition."""

    def key_of(self, payload: Mapping[str, Any]) -> str:
        return "_"

    def label_of(self, key: str) -> str:
        return "All"

    def bounds(self, key: str) -> Mapping[str, Bound]:
        return {}

    def recent(self, now: datetime) -> list[str] | None:
        return ["_"]


@dataclass(frozen=True)
class _Field(PartitionScheme):
    """One partition per value of a field, taken as text."""

    field: str

    def key_of(self, payload: Mapping[str, Any]) -> str:
        return f"{self.field}={_escape(str(_field_value(payload, self.field)))}"

    def label_of(self, key: str) -> str:
        return f"{_words(self.field)} {unquote(_value_of(key, self.field))}"

    def bounds(self, key: str) -> Mapping[str, Bound]:
        value = unquote(_value_of(key, self.field))
        return {self.field: Bound(value, value)}

    def recent(self, now: datetime) -> list[str] | None:
        return None  # the values a field can take aren't known


@dataclass(frozen=True)
class _Day(PartitionScheme):
    """One partition per calendar day of a date field, with days counted in `zone`."""

    field: str
    zone: ZoneInfo

    def _key(self, day: date) -> str:
        return f"{self.field}={day.isoformat()}"

    def key_of(self, payload: Mapping[str, Any]) -> str:
        return self._key(_date_field(payload, self.field, self.zone))

    def label_of(self, key: str) -> str:
        day = date.fromisoformat(_value_of(key, self.field))
        return f"{day.day} {_MONTHS[day.month - 1]} {day.year}"

    def bounds(self, key: str) -> Mapping[str, Bound]:
        day = date.fromisoformat(_value_of(key, self.field))
        return {self.field: Bound(day, day + timedelta(days=1))}

    def recent(self, now: datetime) -> list[str] | None:
        today = _local_date(now, self.zone)
        return [self._key(today - timedelta(days=1)), self._key(today)]


@dataclass(frozen=True)
class _Month(PartitionScheme):
    """One partition per calendar month of a date field, with months counted in `zone`."""

    field: str
    zone: ZoneInfo

    def _key(self, day: date) -> str:
        return f"{self.field}={day.year:04d}-{day.month:02d}"

    def _first_day(self, key: str) -> date:
        return date.fromisoformat(f"{_value_of(key, self.field)}-01")

    def key_of(self, payload: Mapping[str, Any]) -> str:
        return self._key(_date_field(payload, self.field, self.zone))

    def label_of(self, key: str) -> str:
        first = self._first_day(key)
        return f"{_MONTHS[first.month - 1]} {first.year}"

    def bounds(self, key: str) -> Mapping[str, Bound]:
        first = self._first_day(key)
        following = date(first.year + 1, 1, 1) if first.month == 12 else date(first.year, first.month + 1, 1)
        return {self.field: Bound(first, following)}

    def recent(self, now: datetime) -> list[str] | None:
        this_month = _local_date(now, self.zone).replace(day=1)
        last_month = (this_month - timedelta(days=1)).replace(day=1)
        return [self._key(last_month), self._key(this_month)]


@dataclass(frozen=True)
class _Bucket(PartitionScheme):
    """A field's value hashed into one of `n` buckets, so that equal values always share a bucket."""

    field: str
    n: int

    def key_of(self, payload: Mapping[str, Any]) -> str:
        value = _field_value(payload, self.field)
        try:
            data = canonical_json(value)
        except TypeError as e:
            raise PartitionError(f"partition field {self.field!r} can't be hashed: {value!r}") from e
        # Bucket numbers are stored in keys, so this must never change for the same value: it is the
        # 8-byte BLAKE2b digest of the canonical JSON, read as a big-endian unsigned integer.
        digest = hashlib.blake2b(data, digest_size=8).digest()
        return f"{_BUCKET}={int.from_bytes(digest, 'big') % self.n}"

    def label_of(self, key: str) -> str:
        return f"Bucket {_value_of(key, _BUCKET)}"

    def bounds(self, key: str) -> Mapping[str, Bound]:
        return {}  # a hash bucket isn't a range of the field

    def recent(self, now: datetime) -> list[str] | None:
        return [f"{_BUCKET}={i}" for i in range(self.n)]


@dataclass(frozen=True)
class _Fields(PartitionScheme):
    """The combination of several schemes: one partition for each combination of their partitions."""

    parts: tuple[PartitionScheme, ...]

    def _split(self, key: str) -> list[str]:
        segments = key.split("/")
        if len(segments) != len(self.parts):
            raise ValueError(f"{key!r} does not have the {len(self.parts)} parts of this scheme")
        return segments

    def key_of(self, payload: Mapping[str, Any]) -> str:
        return "/".join(part.key_of(payload) for part in self.parts)

    def label_of(self, key: str) -> str:
        return " · ".join(part.label_of(s) for part, s in zip(self.parts, self._split(key), strict=True))

    def bounds(self, key: str) -> Mapping[str, Bound]:
        merged: dict[str, Bound] = {}
        for part, segment in zip(self.parts, self._split(key), strict=True):
            merged.update(part.bounds(segment))
        return merged

    def recent(self, now: datetime) -> list[str] | None:
        windows: list[list[str]] = []
        for part in self.parts:
            keys = part.recent(now)
            if keys is None:
                return None
            windows.append(keys)
        return ["/".join(combination) for combination in product(*windows)]


def none() -> PartitionScheme:
    """Return the scheme with a single partition, which has the key `_`. It is the default."""
    return _Unpartitioned()


def by_day(field: str, tz: str) -> PartitionScheme:
    """Return a scheme with one partition per calendar day of `field`, with days counted in time zone `tz`.

    `tz` is an IANA name such as `Asia/Kolkata`. The field may hold a date, a datetime or an ISO-8601
    string. A datetime without a timezone is taken to be in `tz` already.
    """
    return _Day(field, ZoneInfo(tz))


def by_month(field: str, tz: str) -> PartitionScheme:
    """Return a scheme with one partition per calendar month of `field`. Otherwise it is like `by_day`."""
    return _Month(field, ZoneInfo(tz))


def by_fields(*parts: str | PartitionScheme) -> PartitionScheme:
    """Return a scheme that combines `parts` into one partition for each combination of their partitions.

    A part is a field name, which is keyed by the field's text, or another scheme. The key of a scheme
    used as a part must not contain `/`, because that separates the parts of the combined key.
    """
    if not parts:
        raise ValueError("by_fields needs at least one part")
    schemes: list[PartitionScheme] = []
    for part in parts:
        if isinstance(part, str):
            schemes.append(_Field(part))
        elif isinstance(part, _Fields):
            schemes.extend(part.parts)  # a combination inside a combination is one combination
        else:
            schemes.append(part)
    return _Fields(tuple(schemes))


def by_bucket(field: str, n: int) -> PartitionScheme:
    """Return a scheme that hashes `field` into `n` buckets, for parallel streams that keep per-key order.

    A value always lands in the same bucket, so the versions of one key stay in order within it.
    """
    if n < 1:
        raise ValueError(f"by_bucket needs at least one bucket, got {n}")
    return _Bucket(field, n)

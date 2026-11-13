"""Availability windows: the hours a connection's system may be read or written (M3.1 plan 1 F3).

A window is configuration, on a connection in feather.yaml:

    connections:
      erp:
        connector: sap-cpi
        windows:
          tz: Asia/Kolkata
          open: ["22:00-05:00"]      # one or more spans; a span may cross midnight
          applies: [read]            # read, write, or both (the default)
          max_overrun: 30m           # optional: how long a delivery may go on sending chunks past the close

Windows gate starts: a task that needs the connection outside its window waits until the window next opens, and a
task that started inside it runs to its end, unless `max_overrun` is set, when a delivery stops at its next chunk
once the window has been closed that long. Times are wall-clock times in `tz`, so a window keeps its hours across a
change to or from daylight saving; on the night of the change it is an hour longer or shorter.
"""

import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

Use = Literal["read", "write"]
USES: frozenset[Use] = frozenset({"read", "write"})
_SPAN = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$")
_DURATION = re.compile(r"^\s*(\d+)\s*([smhd])\s*$")
_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
_KEYS = frozenset({"tz", "open", "applies", "max_overrun"})
_DAYS_AHEAD = 3  # a window opens every day, so the next opening is at most a day or so away


@dataclass(frozen=True)
class Span:
    """One open span, from `start` to `end` in minutes of the day. An `end` at or before `start` is on the next day."""

    start: int
    end: int

    @property
    def crosses_midnight(self) -> bool:
        return self.end <= self.start

    def __str__(self) -> str:
        return f"{_hhmm(self.start)}-{_hhmm(self.end)}"


@dataclass(frozen=True)
class AvailabilityWindow:
    """When a connection may be used: open in any of `spans`, told in `tz`, for the uses in `applies`."""

    tz: str
    spans: tuple[Span, ...]
    applies: frozenset[Use] = USES
    max_overrun: timedelta | None = None

    def gates(self, use: Use) -> bool:
        return use in self.applies

    def is_open(self, now: datetime) -> bool:
        return any(start <= now < end for start, end in self._openings(now))

    def opens_at(self, now: datetime) -> datetime:
        """When the window is next open: `now` if it is open now."""
        if self.is_open(now):
            return now
        return min(start for start, _ in self._openings(now) if start > now)

    def closed_since(self, now: datetime) -> datetime | None:
        """When the window last closed, if it is closed now; None if it is open."""
        if self.is_open(now):
            return None
        return max(end for _, end in self._openings(now) if end <= now)

    def overrun(self, now: datetime) -> bool:
        """Whether a task that started inside the window has gone on too long past its close: `max_overrun` is set,
        and the window has been closed at least that long."""
        if self.max_overrun is None:
            return False
        closed = self.closed_since(now)
        return closed is not None and now - closed >= self.max_overrun

    def describe(self) -> str:
        spans = ", ".join(str(s) for s in self.spans)
        uses = " and ".join(sorted(self.applies))
        overrun = "" if self.max_overrun is None else f", overrun up to {self.max_overrun}"
        return f"open {spans} {self.tz}, for {uses}{overrun}"

    def spec_dict(self) -> dict[str, Any]:
        return {
            "tz": self.tz,
            "open": [str(s) for s in self.spans],
            "applies": sorted(self.applies),
            "max_overrun": None if self.max_overrun is None else self.max_overrun.total_seconds(),
        }

    def _openings(self, now: datetime) -> Iterator[tuple[datetime, datetime]]:
        """Every opening that starts from the day before `now` (its date in `tz`) to a few days after, as UTC times."""
        zone = ZoneInfo(self.tz)
        today = now.astimezone(zone).date()
        for offset in range(-1, _DAYS_AHEAD):
            day = today + timedelta(days=offset)
            for span in self.spans:
                start = _at(day, span.start, zone)
                end = _at(day + timedelta(days=1) if span.crosses_midnight else day, span.end, zone)
                if end > start:
                    yield start, end


def _at(day: date, minutes: int, zone: ZoneInfo) -> datetime:
    """The moment of `minutes` past midnight on `day` in `zone`. A time the clocks skip (in a change to daylight
    saving) is taken as the same span after the change, and a time they show twice as its first showing."""
    day = day + timedelta(days=minutes // 1440)
    local = datetime.combine(day, time(minutes % 1440 // 60, minutes % 60), tzinfo=zone)
    return local.astimezone(ZoneInfo("UTC"))


def _hhmm(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def parse_duration(value: object) -> timedelta:
    """A duration written as a number of seconds or as `30s`, `15m`, `2h` or `1d`."""
    if isinstance(value, bool):
        raise ValueError("give a duration such as 30m")  # noqa: TRY004 - a config error, caught as one
    if isinstance(value, int | float):
        seconds = float(value)
    elif isinstance(value, str) and (match := _DURATION.match(value)):
        return timedelta(**{_UNITS[match.group(2)]: int(match.group(1))})
    else:
        raise ValueError(f"{value!r} is not a duration: write 30m, 2h or a number of seconds")
    if seconds < 0:
        raise ValueError("a duration can't be negative")
    return timedelta(seconds=seconds)


def parse_span(text: object) -> Span:
    match = _SPAN.match(text) if isinstance(text, str) else None
    if match is None:
        raise ValueError(f"{text!r} is not a span: write HH:MM-HH:MM, such as 22:00-05:00")
    h1, m1, h2, m2 = (int(g) for g in match.groups())
    if h1 > 23 or m1 > 59 or h2 > 24 or m2 > 59 or (h2 == 24 and m2 != 0):
        raise ValueError(f"{text!r} is not a time of day: hours go to 23 (24:00 ends a day), minutes to 59")
    span = Span(h1 * 60 + m1, h2 * 60 + m2)
    if span.start == span.end % 1440 and span.end != 24 * 60:
        raise ValueError(f"{text!r} opens and closes at the same time: leave windows out to be always open")
    if span.end == 24 * 60 and span.start == 0:
        raise ValueError(f"{text!r} is the whole day: leave windows out to be always open")
    return span


def parse_window(raw: object) -> AvailabilityWindow:
    """The window that a connection's `windows:` setting describes. Raises ValueError, saying what is wrong."""
    if not isinstance(raw, Mapping):
        raise ValueError(  # noqa: TRY004 - a config error, caught as one
            "must be a mapping with tz and open, such as {tz: Asia/Kolkata, open: ['22:00-05:00']}"
        )
    body = {str(k): v for k, v in cast(Mapping[Any, Any], raw).items()}
    unknown = sorted(set(body) - _KEYS)
    if unknown:
        raise ValueError(f"unknown setting {unknown[0]!r}: a window takes {', '.join(sorted(_KEYS))}")
    tz = body.get("tz")
    if not isinstance(tz, str):
        raise ValueError("tz is required: the IANA time zone the spans are told in, such as Asia/Kolkata")  # noqa: TRY004
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"tz {tz!r} is not an IANA time zone, such as Asia/Kolkata") from None
    spans_raw = body.get("open")
    if isinstance(spans_raw, str):
        spans_raw = [spans_raw]
    if not isinstance(spans_raw, list) or not spans_raw:
        raise ValueError("open is required: one or more spans, such as ['22:00-05:00']")
    spans = tuple(parse_span(s) for s in cast(Sequence[object], spans_raw))
    applies_raw = body.get("applies", sorted(USES))
    if isinstance(applies_raw, str):
        applies_raw = [applies_raw]
    if not isinstance(applies_raw, list) or not applies_raw:
        raise ValueError("applies must name read, write or both")
    applies = {str(a) for a in cast(Sequence[object], applies_raw)}
    if not applies <= USES:
        raise ValueError(f"applies can name read and write, not {min(applies - USES)!r}")
    overrun = body.get("max_overrun")
    return AvailabilityWindow(
        tz,
        spans,
        cast(frozenset[Use], frozenset(applies)),
        None if overrun is None else parse_duration(overrun),
    )

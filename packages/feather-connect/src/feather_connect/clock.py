"""Time sources. Code that needs the time takes a `Clock`, so tests can control it."""

from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    """A source of the current time. `now` returns a timezone-aware UTC datetime."""

    def now(self) -> datetime: ...


class SystemClock(Clock):
    """The real time."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class FrozenClock(Clock):
    """A clock that stays put until `advance` is called, for tests and the in-memory engine."""

    def __init__(self, start: datetime) -> None:
        if start.utcoffset() is None:
            raise ValueError("FrozenClock needs a timezone-aware start time")
        self._now = start.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        """Move the clock forward by `delta`."""
        self._now += delta

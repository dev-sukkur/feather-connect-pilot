from datetime import UTC, datetime, timedelta, timezone

import pytest

from feather_connect.clock import FrozenClock, SystemClock


def test_system_clock_reads_the_current_time_in_utc():
    now = SystemClock().now()
    assert now.tzinfo is UTC
    assert abs(now - datetime.now(UTC)) < timedelta(minutes=1)


def test_frozen_clock_stays_put_until_advanced():
    start = datetime(2026, 10, 18, 9, 30, tzinfo=UTC)
    clock = FrozenClock(start)
    assert clock.now() == start
    assert clock.now() == start
    clock.advance(timedelta(minutes=45))
    clock.advance(timedelta(minutes=45))
    assert clock.now() == datetime(2026, 10, 18, 11, 0, tzinfo=UTC)


def test_frozen_clock_reports_utc_whatever_the_zone_of_its_start():
    india = timezone(timedelta(hours=5, minutes=30))
    clock = FrozenClock(datetime(2026, 10, 18, 5, 30, tzinfo=india))
    assert clock.now().tzinfo is UTC
    assert clock.now() == datetime(2026, 10, 18, tzinfo=UTC)


def test_frozen_clock_rejects_a_start_without_a_timezone():
    with pytest.raises(ValueError, match="timezone-aware"):
        FrozenClock(datetime(2026, 10, 18))

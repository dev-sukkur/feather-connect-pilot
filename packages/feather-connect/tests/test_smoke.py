import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.ids import new_id, stable_id


def test_version():
    assert fc.__version__ == "0.1.0"


def test_ids():
    assert len(new_id()) == 26 and new_id() != new_id()
    assert stable_id("run", "ds", "p") == stable_id("run", "ds", "p") != stable_id("run", "ds", "q")


def test_frozen_clock_advances():
    from datetime import UTC, datetime, timedelta

    c = FrozenClock(datetime(2026, 10, 18, tzinfo=UTC))
    c.advance(timedelta(hours=1))
    assert c.now().hour == 1

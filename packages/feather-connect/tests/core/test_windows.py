"""Availability windows (M3.1 plan 1 F3; hard case H19)."""

from datetime import UTC, datetime, timedelta

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory
from feather_connect.core.windows import parse_window
from feather_connect.runtime.windows import wait_name
from feather_connect.testing import FlowHarness

NIGHT = parse_window(
    {"tz": "Asia/Kolkata", "open": ["22:00-05:00"]}
)  # IST is UTC+5:30, with no daylight saving


def at(h: int, m: int = 0, day: int = 13) -> datetime:
    return datetime(2026, 11, day, h, m, tzinfo=UTC)


def test_a_span_that_crosses_midnight_is_open_on_both_sides_of_it():
    assert NIGHT.is_open(at(17))  # 22:30 IST
    assert NIGHT.is_open(at(22))  # 03:30 IST
    assert not NIGHT.is_open(at(0))  # 05:30 IST
    assert not NIGHT.is_open(at(12))  # 17:30 IST


def test_the_next_opening_is_tonight_and_now_when_open():
    assert NIGHT.opens_at(at(6)) == at(16, 30)  # 11:30 IST: opens at 22:00 IST
    assert NIGHT.opens_at(at(17)) == at(17)
    assert NIGHT.closed_since(at(6)) == at(23, 30, day=12)  # it closed at 05:00 IST this morning
    assert NIGHT.closed_since(at(17)) is None


def test_a_window_keeps_its_wall_clock_hours_across_daylight_saving():
    # New York goes from EDT (UTC-4) to EST (UTC-5) at 02:00 on 1 Nov 2026.
    w = parse_window({"tz": "America/New_York", "open": "01:00-03:00"})
    before = datetime(2026, 10, 31, 5, 30, tzinfo=UTC)  # 01:30 EDT
    assert w.is_open(before)
    # On the night of the change the window opens at 01:00 EDT and closes at 03:00 EST: three hours, not two.
    assert w.opens_at(datetime(2026, 11, 1, 0, 0, tzinfo=UTC)) == datetime(2026, 11, 1, 5, 0, tzinfo=UTC)
    assert w.is_open(datetime(2026, 11, 1, 7, 30, tzinfo=UTC))  # 02:30 EST
    assert not w.is_open(datetime(2026, 11, 1, 8, 0, tzinfo=UTC))  # 03:00 EST
    # And the next night it is 01:00-03:00 EST, at UTC-5.
    assert w.opens_at(datetime(2026, 11, 1, 12, 0, tzinfo=UTC)) == datetime(2026, 11, 2, 6, 0, tzinfo=UTC)


def test_a_window_in_a_spring_forward_gap_still_opens():
    # Clocks skip 02:00-03:00 on 8 Mar 2026 in New York: a window from 02:30 opens just after the change.
    w = parse_window({"tz": "America/New_York", "open": "02:30-04:00"})
    opens = w.opens_at(datetime(2026, 3, 8, 5, 0, tzinfo=UTC))  # 00:00 EST
    assert datetime(2026, 3, 8, 7, 0, tzinfo=UTC) <= opens <= datetime(2026, 3, 8, 7, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ({"open": ["22:00-05:00"]}, "tz is required"),
        ({"tz": "Mars/Olympus", "open": ["22:00-05:00"]}, "IANA"),
        ({"tz": "UTC", "open": ["22-05"]}, "not a span"),
        ({"tz": "UTC", "open": ["25:00-05:00"]}, "not a time of day"),
        ({"tz": "UTC", "open": ["05:00-05:00"]}, "same time"),
        ({"tz": "UTC", "open": ["00:00-24:00"]}, "whole day"),
        ({"tz": "UTC", "open": []}, "open is required"),
        ({"tz": "UTC", "open": ["01:00-02:00"], "applies": ["delete"]}, "read and write"),
        ({"tz": "UTC", "open": ["01:00-02:00"], "max_overrun": "soon"}, "not a duration"),
        ({"tz": "UTC", "open": ["01:00-02:00"], "hours": 3}, "unknown setting"),
    ],
)
def test_a_window_that_makes_no_sense_is_refused(raw, message):
    with pytest.raises(ValueError, match=message):
        parse_window(raw)


def flow() -> fc.Flow:
    orders = fc.Dataset("orders", source=memory.Source("erp"), key="id", strategy=fc.strategy.snapshot_diff())
    return fc.Flow("orders", reads=orders, deliver=[fc.to(memory.Sink("pos"), name="pos")])


def harness(window, start: datetime) -> FlowHarness:
    h = FlowHarness(flow(), clock=FrozenClock(start), windows=window)
    h.source("orders").load([{"id": 1}, {"id": 2}])
    return h


def test_a_read_outside_its_window_waits_for_it_and_says_so():  # H19
    h = harness({"erp": NIGHT}, at(6))  # 11:30 IST
    h.run()
    assert h.clock.now() >= at(16, 30)  # it slept until 22:00 IST
    assert {r["id"] for r in h.sink("pos").rows} == {1, 2}
    [wait] = h.store.budgets.waits(wait_name("erp")) or [None]
    assert wait is None or wait.until == at(16, 30)  # a wait past its time has gone from the list


def test_a_read_inside_its_window_does_not_wait():
    h = harness({"erp": NIGHT}, at(17))
    h.run()
    assert h.clock.now() < at(17, 5)
    assert len(h.sink("pos").rows) == 2


def test_a_closed_write_window_holds_the_delivery_not_the_read():
    write_only = parse_window({"tz": "Asia/Kolkata", "open": ["22:00-05:00"], "applies": ["write"]})
    h = harness({"pos": write_only}, at(6))
    h.run()
    [version] = h.versions("orders")
    assert version.sealed_at is not None and version.sealed_at < at(7)  # read at once
    assert h.ledger.get("pos", version.version_id).status == "applied"
    assert h.clock.now() >= at(16, 30)  # delivered when the window opened


def test_a_window_on_another_use_does_not_hold():
    read_only = parse_window({"tz": "Asia/Kolkata", "open": ["22:00-05:00"], "applies": ["read"]})
    h = harness({"pos": read_only}, at(6))
    h.run()
    assert h.clock.now() < at(7)
    assert len(h.sink("pos").rows) == 2


def test_waits_are_noted_while_they_last():
    h = harness({"erp": NIGHT}, at(6))
    seen: list[datetime] = []
    original = h.store.budgets.note_wait

    def note(name, task_ref, run_id, until):
        seen.append(until)
        assert name == "window:erp"
        original(name, task_ref, run_id, until)

    h.store.budgets.note_wait = note  # type: ignore[method-assign]
    h.run()
    assert seen == [at(16, 30)]


def test_max_overrun_is_read_as_a_duration():
    w = parse_window({"tz": "UTC", "open": ["01:00-02:00"], "max_overrun": "30m"})
    assert w.max_overrun == timedelta(minutes=30)
    assert not w.overrun(at(2, 10)) and w.overrun(at(2, 30))

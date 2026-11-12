"""Triggers: what starts a run of a flow."""

from dataclasses import dataclass
from datetime import timedelta
from zoneinfo import ZoneInfo

from feather_connect.core.dataset import Dataset
from feather_connect.core.readiness import OnReady
from feather_connect.core.strategies import parse_duration


@dataclass(frozen=True)
class Cron:
    """Starts a run on the cron schedule `expr`, with its times read in the IANA time zone `tz`."""

    expr: str
    tz: str


@dataclass(frozen=True)
class Every:
    """Starts a run each `interval`."""

    interval: timedelta


@dataclass(frozen=True)
class Reconcile:
    """Starts a run each `interval` that reads the `lookback` before it again, for the rows a `by_stamp` read missed:
    those stamped late, such as back-dated into a closed period (design H5). What it finds unchanged is dropped."""

    interval: timedelta
    lookback: timedelta


@dataclass(frozen=True)
class Manual:
    """Starts a run only when someone asks for one."""


@dataclass(frozen=True)
class OnEvent:
    """Starts a run when events arrive for `dataset`."""

    dataset: Dataset


Trigger = Cron | Every | Reconcile | Manual | OnEvent | OnReady

# An interval under this is a high-frequency poller: its ticks are tasks of one daily run, not a run each (design 6.3).
STREAM_BELOW = timedelta(minutes=5)


def is_stream(trigger: Trigger) -> bool:
    """Return whether the trigger's ticks go into the flow's daily stream run. Longer intervals behave like cron."""
    return isinstance(trigger, Every) and trigger.interval < STREAM_BELOW


def cron(expr: str, *, tz: str) -> Cron:
    """Return a trigger that runs on the cron schedule `expr`, such as `*/15 * * * *`, read in time zone `tz`.

    `tz` is an IANA name such as `Asia/Kolkata`. It can't be left out, because a schedule means nothing
    without one.
    """
    ZoneInfo(tz)  # fail now, not at the first tick, if the zone doesn't exist
    return Cron(expr, tz)


def every(interval: str) -> Every:
    """Return a trigger that runs each `interval`, such as `30s` or `15m`. See `parse_duration`."""
    step = parse_duration(interval)
    if step <= timedelta(0):
        raise ValueError(f"every needs an interval above zero, got {interval!r}")
    return Every(step)


def reconcile(*, every: str, lookback: str) -> Reconcile:
    """Return a trigger that runs each `every`, such as `1d`, reading the `lookback` before then again, such as `30d`,
    for a flow whose datasets use `by_stamp`. Its runs find rows whose stamp put them before the watermark."""
    step, back = parse_duration(every), parse_duration(lookback)
    if step <= timedelta(0) or back <= timedelta(0):
        raise ValueError(f"reconcile needs every and lookback above zero, got {every!r} and {lookback!r}")
    return Reconcile(step, back)


def manual() -> Manual:
    """Return a trigger for runs that someone starts."""
    return Manual()


def on_event(dataset: Dataset) -> OnEvent:
    """Return a trigger that runs when events arrive for `dataset`, which must use the `push` strategy."""
    return OnEvent(dataset)

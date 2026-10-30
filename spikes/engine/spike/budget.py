"""Engine-independent shared rate budget with a priority reserve (design §6.6), spike version.

Fixed windows: each window of `period_s` seconds grants at most `limit_units`.
Low-priority work may use at most (1 - reserve_high) of a window; high may use all of it.
"""

from __future__ import annotations

import math

from spike.journal import pool


def define(name: str, limit_units: int, period_s: float, reserve_high: float) -> None:
    with pool().connection() as c:
        c.execute(
            "insert into spike.budget (name, limit_units, period_s, reserve_high) values (%s, %s, %s, %s)"
            " on conflict (name) do update set limit_units = excluded.limit_units,"
            " period_s = excluded.period_s, reserve_high = excluded.reserve_high",
            (name, limit_units, period_s, reserve_high),
        )


def acquire(name: str, units: int, priority: str, run: str) -> tuple[bool, float]:
    """Try to take `units` now. Returns (granted, seconds_until_next_window)."""
    with pool().connection() as c:
        with c.transaction():
            limit_units, period_s, reserve = c.execute(
                "select limit_units, period_s, reserve_high from spike.budget where name = %s",
                (name,),
            ).fetchone()
            now = float(c.execute("select extract(epoch from clock_timestamp())").fetchone()[0])
            window = math.floor(now / period_s) * period_s
            c.execute(
                "insert into spike.budget_use (name, window_start) values (%s, %s) on conflict do nothing",
                (name, window),
            )
            used, used_high = c.execute(
                "select used, used_high from spike.budget_use where name = %s and window_start = %s for update",
                (name, window),
            ).fetchone()
            low_cap = math.floor(limit_units * (1 - reserve))
            allowed = used + units <= limit_units and (
                priority == "high" or (used - used_high) + units <= low_cap
            )
            if not allowed:
                return False, max(0.01, window + period_s - now)
            c.execute(
                "update spike.budget_use set used = used + %s, used_high = used_high + %s"
                " where name = %s and window_start = %s",
                (units, units if priority == "high" else 0, name, window),
            )
            c.execute(
                "insert into spike.budget_grant (name, window_start, priority, run) values (%s, %s, %s, %s)",
                (name, window, priority, run),
            )
            return True, 0.0

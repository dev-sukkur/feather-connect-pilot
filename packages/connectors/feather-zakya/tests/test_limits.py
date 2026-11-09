"""Zoho's limits: a 429 waited out for its `Retry-After`, the daily cap handed to the engine, and the connection's
budget metering every call, the token's included, in both windows."""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import pytest

from feather_connect.clock import FrozenClock
from feather_connect.connectors import zakya  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.connectors.zakya.testing import ZakyaSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import BudgetDeferred, TransientError
from feather_connect.core.partitions import none
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import BudgetSpec, parse_limits

from .conftest import NOW, Clock


def _read(source: Any, ctx: Context) -> list[dict[str, Any]]:
    return [row for batch in source.read(ctx, ReadRequest("*", none())) for row in batch]


def test_a_429_is_waited_out_for_its_retry_after(
    sim: ZakyaSim, ctx: Context, clock: Clock, wire: Callable[[Any], Any]
):
    sim.add(4)
    clock.at = NOW + timedelta(seconds=50)
    sim.per_minute = 3  # the list and two invoices, then the minute is spent
    rows = _read(wire(zakya.Source("zakya", "invoices")), ctx)
    assert len(rows) == 4
    assert clock.sleeps == [10.0]  # to the next minute, as Retry-After said


def test_the_daily_cap_goes_back_to_the_engine_with_the_wait(
    sim: ZakyaSim, ctx: Context, clock: Clock, wire: Callable[[Any], Any]
):
    sim.add(4)
    sim.per_day = 2
    with pytest.raises(TransientError) as caught:
        _read(wire(zakya.Source("zakya", "invoices")), ctx)
    assert caught.value.retry_after is not None and caught.value.retry_after > 3600  # midnight in India
    assert clock.sleeps == []  # no worker thread waits for it


def test_the_budget_meters_every_call_in_both_windows(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):
    sim.add(3)
    store = MemoryStore(FrozenClock(NOW))
    store.budgets.define(BudgetSpec("zoho-api", parse_limits(["100/minute", "4/day"]), account="zakya:60001"))
    budgeted = Context(ctx.config, ctx.log, ctx.now, ctx.tz, budget=BudgetHandle(store.budgets, "zoho-api"))
    with pytest.raises(BudgetDeferred) as caught:  # a token, a list and three invoices: one too many
        _read(wire(zakya.Source("zakya", "invoices")), budgeted)
    assert caught.value.wait > timedelta(hours=1)
    assert [u.used for u in store.budgets.usage("zoho-api")] == [4, 4]
    assert sim.token_requests == 1 and len(sim.calls) == 3

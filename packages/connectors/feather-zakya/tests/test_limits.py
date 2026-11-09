"""Zoho's limits: a 429 waited out for its `Retry-After`, the daily cap handed to the engine, and the connection's
budget metering every call, the token's included, in both windows."""

from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb as duck
import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import (
    duckdb,  # pyright: ignore[reportAttributeAccessIssue]
    zakya,  # pyright: ignore[reportAttributeAccessIssue]
)
from feather_connect.connectors.zakya.testing import IST, ZakyaSim  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.errors import BudgetDeferred, PermanentError, TransientError
from feather_connect.core.partitions import none
from feather_connect.engine.local import LocalEngine
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import BudgetSpec, parse_limits
from feather_connect.storage.spool import MemorySpool

from .conftest import CONFIG, NOW, Clock


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


@pytest.mark.parametrize(
    ("timezone", "hours"), [("Asia/Kolkata", 9.5), (None, 15.0)], ids=["organisation-zone", "flow-zone"]
)
def test_a_daily_cap_with_no_retry_after_waits_until_midnight(
    sim: ZakyaSim,
    make_ctx: Callable[..., Context],
    wire: Callable[[Any], Any],
    timezone: str | None,
    hours: float,
):  # Stage Z review 5: Zoho's code 45 alone
    sim.add(2)
    sim.per_day, sim.daily_retry_after = 1, False
    ctx = make_ctx(**({"timezone": timezone} if timezone else {}))  # 09:00 UTC is 14:30 in India
    with pytest.raises(TransientError) as caught:
        _read(wire(zakya.Source("zakya", "invoices")), ctx)
    assert caught.value.retry_after == hours * 3600


def test_a_token_endpoint_that_turns_the_client_away_is_waited_for(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):  # Stage Z review 7
    sim.add(1)
    sim.limit_tokens()
    with pytest.raises(TransientError) as caught:
        _read(wire(zakya.Source("zakya", "invoices")), ctx)
    assert caught.value.retry_after == 600.0 and sim.calls == []
    assert len(_read(wire(zakya.Source("zakya", "invoices")), ctx)) == 1  # let in again


def test_a_bad_time_zone_is_a_setup_error(make_ctx: Callable[..., Context]):
    with pytest.raises(PermanentError, match="not a time zone"):
        zakya.Source("zakya", "invoices").client(make_ctx(timezone="Mars/Olympus"))


@pytest.mark.parametrize("retry_after", [True, False], ids=["retry-after", "code-45-alone"])
def test_a_run_that_meets_the_daily_cap_sleeps_until_midnight_and_ends_well(
    tmp_path: Path, retry_after: bool
):  # Stage Z review 4: through the runtime and the engine, on its virtual clock
    clock = FrozenClock(NOW)  # 14:30 in India
    sim = ZakyaSim(now=clock.now, per_day=5, start=7, daily_retry_after=retry_after)
    sim.spend(3)  # the organisation's other clients took most of today's
    store, spool, engine = MemoryStore(clock), MemorySpool(clock), LocalEngine(clock)
    store.budgets.define(
        BudgetSpec("zoho-api", parse_limits(["90/minute", "1000/day"]), account="zakya:60001")
    )
    source = zakya.Source("zakya", "invoices", page_size=2, details=False)
    source.transport = sim.transport()
    flow = fc.Flow(
        "zakya-cap",
        reads=fc.Dataset(
            "invoices", source=source, key="invoice_id", strategy=fc.strategy.by_stamp("last_modified_time")
        ),
        deliver=[fc.to(duckdb.Table("warehouse", table="invoices", key_columns=["invoice_id"]))],
        triggers=[fc.every("1h")],
    )
    configs = {
        "zakya": {**CONFIG, "timezone": "Asia/Kolkata"},
        "warehouse": {"database": str(tmp_path / "w.duckdb")},
    }
    runtime = Runtime(
        [flow],
        store,
        spool,
        engine,
        code_sha="cap",
        clock=clock,
        budgets={"zakya": "zoho-api"},
        config_of=lambda connection: configs[connection],
    )
    engine.register(runtime.tasks())
    run = runtime.start_run(flow.name)
    engine.run_until_idle()
    assert store.runs.get_run(run).status == "succeeded"
    assert clock.now() >= datetime(2026, 11, 8, tzinfo=IST)  # it waited for the day to turn in India
    assert clock.now() < datetime(2026, 11, 8, 1, tzinfo=IST)  # and no longer
    # Two pages before the cap (3 of 5 spent elsewhere), then the whole read again on the new day: five pages, as
    # paging by stamp asks again from the stamp all seven share.
    assert sim.calls.count("GET /inventory/v1/invoices") == 2 + 5
    with duck.connect(str(tmp_path / "w.duckdb")) as conn:
        assert conn.execute("select count(*) from invoices").fetchone() == (7,)

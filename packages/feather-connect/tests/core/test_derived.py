"""Derived datasets (M3.1 plan 1 F2; discovery FR-C4, UC-06)."""

from datetime import UTC, datetime, timedelta

import pytest

import feather_connect as fc
from feather_connect.clock import FrozenClock
from feather_connect.connectors import memory
from feather_connect.core.sql import reference_problems
from feather_connect.runtime.references import CACHE
from feather_connect.testing import FlowHarness

# 18:00 UTC on 30 Nov is 23:30 in Kolkata: still 30 Nov there; 19:00 UTC is 00:30 on 1 Dec.
EVENING = datetime(2026, 11, 30, 18, 0, tzinfo=UTC)
PRICES = [
    {"item": "A", "store": "S1", "price": 100, "valid_from": "2026-11-01"},
    {"item": "A", "store": "S1", "price": 120, "valid_from": "2026-12-01"},  # future-dated
    {"item": "B", "store": "S1", "price": 50, "valid_from": "2026-10-15"},
]
EFFECTIVE = """
    select item, store, price
    from {{ dataset('sap.price') }}
    where cast(valid_from as date) <= {{ today }}
    qualify row_number() over (partition by item, store order by valid_from desc) = 1
"""


@pytest.fixture(autouse=True)
def fresh_cache():
    CACHE.clear()
    yield
    CACHE.clear()


def price_flow() -> fc.Flow:
    prices = fc.Dataset(
        "sap.price",
        source=memory.Source("sap"),
        key=("item", "store", "valid_from"),
        strategy=fc.strategy.snapshot_diff(),
        keep_state=True,
    )
    return fc.Flow("prices", reads=prices, deliver=[fc.to(memory.Sink("lake"), name="prices-lake")])


def effective_flow(query: str = EFFECTIVE) -> fc.Flow:
    effective = fc.Dataset(
        "pricing.effective_price",
        source=fc.derived(query, tz="Asia/Kolkata"),
        key=("item", "store"),
        strategy=fc.strategy.snapshot_diff(),
    )
    return fc.Flow("effective-to-pos", reads=effective, deliver=[fc.to(memory.Sink("pos"), name="pos")])


def harness() -> FlowHarness:
    clock = FrozenClock(EVENING)
    h = FlowHarness(effective_flow(), also=[price_flow()], clock=clock)
    h.source("sap.price").load(PRICES)
    h.run(flow="prices")
    return h


def pos(h: FlowHarness) -> dict[tuple[str, str], int]:
    return {(r["item"], r["store"]): r["price"] for r in h.sink("pos").rows}


def test_a_derived_dataset_delivers_what_its_query_gives():
    h = harness()
    h.run()
    assert pos(h) == {("A", "S1"): 100, ("B", "S1"): 50}


def sent(h: FlowHarness) -> list[tuple[str, int]]:
    """Every record the POS was sent, in order, across all its calls."""
    return [
        (r.payload["item"], r.payload["price"])
        for call in h.sink("pos").calls
        for r in call.records
        if r.payload
    ]


def test_a_future_dated_price_takes_effect_on_its_date_with_no_change_at_the_source():  # H18
    h = harness()
    h.run()
    before = len(sent(h))
    assert isinstance(h.clock, FrozenClock)
    h.clock.advance(timedelta(hours=1))  # 00:30 on 1 Dec in Kolkata; nothing changed at SAP
    h.run()
    assert pos(h)[("A", "S1")] == 120
    assert sent(h)[before:] == [("A", 120)]  # exactly that price, once
    h.run()  # recomputed again on the same day: nothing new
    assert len(sent(h)) == before + 1


def test_today_is_the_date_in_the_derived_dataset_s_time_zone():
    h = harness()  # 18:00 UTC is still 30 Nov in Kolkata, so the December price waits
    h.run()
    assert pos(h)[("A", "S1")] == 100


def test_a_derived_dataset_sees_a_change_at_its_input():
    h = harness()
    h.run()
    h.source("sap.price").load([p for p in PRICES if p["item"] != "B"])
    h.run(flow="prices")
    h.run()
    [latest, *_] = h.versions("pricing.effective_price")
    assert latest.deletes == 1  # B left the input, so it leaves the derived dataset


def problems(*flows: fc.Flow) -> dict[str, str]:
    return {p.code: p.severity for p in reference_problems(flows)}


def test_validate_names_a_derived_input_no_flow_reads():
    assert problems(effective_flow()) == {"reference_unknown": "error"}


def test_validate_wants_the_input_to_keep_its_state():
    prices = fc.Dataset(
        "sap.price", source=memory.Source("sap"), key="item", strategy=fc.strategy.snapshot_diff()
    )
    flow = fc.Flow("prices", reads=prices, deliver=[fc.to(memory.Sink("lake"))])
    assert problems(effective_flow(), flow) == {"reference_without_state": "error"}


def test_validate_warns_of_duckdb_s_own_clock():
    query = EFFECTIVE.replace("{{ today }}", "current_date")
    assert problems(effective_flow(query), price_flow()) == {"sql_wall_clock": "warning"}


def test_validate_refuses_a_cycle_of_derived_datasets():
    a = fc.Dataset(
        "a",
        source=fc.derived("select * from {{ dataset('b') }}"),
        key="k",
        strategy=fc.strategy.replace(),
        keep_state=True,
    )
    b = fc.Dataset(
        "b",
        source=fc.derived("select * from {{ dataset('a') }}"),
        key="k",
        strategy=fc.strategy.replace(),
        keep_state=True,
    )
    flows = [
        fc.Flow("fa", reads=a, deliver=[fc.to(memory.Sink("x"))]),
        fc.Flow("fb", reads=b, deliver=[fc.to(memory.Sink("y"))]),
    ]
    assert problems(*flows) == {"derived_cycle": "error"}


def test_a_derived_dataset_needs_a_real_time_zone():
    with pytest.raises(ValueError, match="IANA"):
        fc.derived("select 1", tz="Mars/Olympus")


def test_an_unknown_template_is_refused():
    with pytest.raises(ValueError, match="not a reference"):
        fc.derived("select {{ yesterday }}")

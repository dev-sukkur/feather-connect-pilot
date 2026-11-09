"""`zakya.source`: the datasets, paging by `has_more_page`, `last_modified_time` with overlap and no duplicates, and the
records fetched on their own for their lines."""

from collections.abc import Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest

import feather_connect as fc
from feather_connect.connectors import (
    http,  # pyright: ignore[reportAttributeAccessIssue]
    zakya,  # pyright: ignore[reportAttributeAccessIssue]
)
from feather_connect.connectors.zakya.testing import ZakyaSim, parse_stamp  # pyright: ignore[reportMissingImports]
from feather_connect.core.connector import ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.partitions import none

from .conftest import Clock


def _read(source: Any, ctx: Context, since: Any = None, field: str | None = None) -> list[dict[str, Any]]:
    req = ReadRequest("*", none(), since=since, stamp_field=field)
    return [row for batch in source.read(ctx, req) for row in batch]


def test_each_dataset_names_its_key_and_stamp():
    assert {n: (d.key, d.stamp) for n, d in zakya.DATASETS.items()} == {
        "invoices": ("invoice_id", "last_modified_time"),
        "credit_notes": ("creditnote_id", "last_modified_time"),
        "payments": ("payment_id", "last_modified_time"),
        "items": ("item_id", "last_modified_time"),
        "price_lists": ("pricebook_id", None),
    }
    invoices = zakya.dataset("invoices")
    assert (invoices.key, invoices.strategy) == (
        "invoice_id",
        fc.strategy.by_stamp("last_modified_time", "2m"),
    )
    assert zakya.dataset("items", full=True).strategy.name == "snapshot_diff"
    assert zakya.dataset("price_lists").strategy.name == "snapshot_diff"  # no stamp: always in full
    assert zakya.dataset("payments", "shop", as_name="receipts").name == "receipts"
    with pytest.raises(ValueError, match="no dataset 'orders'"):
        zakya.Source("zakya", "orders")
    with pytest.raises(ValueError, match="page_size"):
        zakya.Source("zakya", "items", page_size=500)


def test_invoices_are_paged_and_fetched_on_their_own_for_their_lines(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):
    sim.add(7)
    source = wire(zakya.Source("zakya", "invoices", page_size=3))
    rows = _read(source, ctx)
    assert sorted(r["invoice_id"] for r in rows) == sorted(sim.invoices)
    assert all(r["line_items"] for r in rows)
    lists = [c for c in sim.calls if c == "GET /inventory/v1/invoices"]
    assert len(lists) == 3 and len(sim.calls) == 3 + 7  # three pages, then each invoice
    sim.calls.clear()
    plain = _read(wire(zakya.Source("zakya", "invoices", page_size=3, details=False)), ctx)
    assert not any("line_items" in r for r in plain) and len(sim.calls) == 3


def test_a_read_by_stamp_asks_from_the_watermark_and_pages_by_stamp(
    sim: ZakyaSim, ctx: Context, clock: Clock, wire: Callable[[Any], Any]
):
    old = sim.add(4)
    clock.at += timedelta(minutes=10)
    new: list[str] = []
    for _ in range(5):  # a second apart, so that the read moves from stamp to stamp
        new += sim.add(1)
        clock.at += timedelta(seconds=1)
    since = parse_stamp(sim.invoices[new[0]]["last_modified_time"])
    source = wire(zakya.Source("zakya", "invoices", page_size=2, details=False))
    rows = _read(source, ctx, since, "last_modified_time")
    assert sorted(r["invoice_id"] for r in rows) == sorted(new) and not set(old) & {
        r["invoice_id"] for r in rows
    }
    assert ctx.checkpoint() is not None  # each page's stamp is saved, for a read that is cut short


def test_a_list_that_ignores_the_stamp_filter_gives_no_old_or_repeated_records(
    sim: ZakyaSim, ctx: Context, clock: Clock, wire: Callable[[Any], Any]
):
    sim.add(3)
    clock.at += timedelta(minutes=10)
    new = sim.add(2)
    sim.stamp_filter = False
    since = sim.invoices[new[0]]["last_modified_time"]  # text, as a watermark read from a record is
    rows = _read(wire(zakya.Source("zakya", "invoices", details=False)), ctx, since, "last_modified_time")
    assert [r["invoice_id"] for r in rows] == new


def test_other_datasets_read_their_own_lists(sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]):
    sim.seed(
        "creditnotes",
        [{"creditnote_id": "cn1", "last_modified_time": "2026-11-07T10:00:00+0530", "line_items": [1]}],
    )
    sim.seed(
        "customerpayments",
        [{"payment_id": "p1", "amount": 10, "last_modified_time": "2026-11-07T10:00:00+0530"}],
    )
    sim.seed(
        "pricebooks", [{"pricebook_id": "pb1", "name": "Retail", "pricebook_items": [{"item_id": "i1"}]}]
    )
    assert _read(wire(zakya.Source("zakya", "credit_notes")), ctx)[0]["line_items"] == [1]
    assert _read(wire(zakya.Source("zakya", "payments")), ctx)[0]["amount"] == 10
    (book,) = _read(wire(zakya.Source("zakya", "price_lists")), ctx)
    assert book["pricebook_items"] == [{"item_id": "i1"}]


def test_discover_names_the_fields_of_the_first_record(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):
    sim.add(1)
    (stream,) = wire(zakya.Source("zakya", "invoices")).discover(ctx)
    assert stream.name == "/inventory/v1/invoices" and stream.fields["last_modified_time"] == "str"


def test_a_record_keeps_the_stamp_it_was_listed_with_not_its_later_one(
    sim: ZakyaSim, ctx: Context, clock: Clock, wire: Callable[[Any], Any]
):  # Stage Z review 6
    [first, second] = sim.add(2)
    listed = sim.invoices[first]["last_modified_time"]
    handle = sim.handle

    def changed_between_list_and_fetch(request: Any) -> Any:
        if request.url.path.endswith(f"/invoices/{first}"):
            clock.at += timedelta(minutes=5)
            sim.change(first)  # changed after the list was read, before its own fetch
        return handle(request)

    source = wire(zakya.Source("zakya", "invoices"))
    source.transport = httpx.MockTransport(changed_between_list_and_fetch)
    rows = _read(source, ctx, since=parse_stamp(listed) - timedelta(minutes=1), field="last_modified_time")
    by_id = {r["invoice_id"]: r for r in rows}
    assert by_id[first]["last_modified_time"] == listed  # the list's stamp: the watermark can't pass `second`
    assert (
        by_id[first]["status"] == "paid" and second in by_id
    )  # the change itself is kept, for the next read


def test_a_key_read_again_is_kept_only_with_a_later_stamp(
    ctx: Context, monkeypatch: pytest.MonkeyPatch
):  # Stage Z review 11
    pages = [
        [
            {"invoice_id": "a", "last_modified_time": "2026-11-07T10:00:00+0530", "v": 1},
            {"invoice_id": "a", "last_modified_time": "2026-11-07T10:05:00+0530", "v": 2},
        ],
        [
            {"invoice_id": "a", "last_modified_time": "2026-11-07T10:01:00+0530", "v": 3},  # older: dropped
            {"invoice_id": "b", "last_modified_time": "2026-11-07T10:01:00+0530", "v": 4},
            {"invoice_id": "a", "last_modified_time": "2026-11-07T10:09:00+0530", "v": 5},
        ],
    ]
    monkeypatch.setattr(http.Rest, "read", lambda self, ctx, req: iter(pages))
    got = list(
        zakya.Source("zakya", "invoices").read(
            ctx, ReadRequest("*", none(), stamp_field="last_modified_time")
        )
    )
    assert [[r["v"] for r in page] for page in got] == [[2], [4, 5]]


def test_the_simulator_sorts_stamps_as_moments_whatever_their_offset(
    sim: ZakyaSim, ctx: Context, wire: Callable[[Any], Any]
):  # Stage Z review 13
    sim.seed(
        "items",
        [
            {"item_id": "late", "sku": "A", "last_modified_time": "2026-11-07T10:00:00+0530"},  # 04:30 UTC
            {"item_id": "early", "sku": "B", "last_modified_time": "2026-11-07T04:00:00+0000"},
        ],
    )
    rows = _read(wire(zakya.Source("zakya", "items")), ctx)
    assert [r["item_id"] for r in rows] == ["early", "late"]

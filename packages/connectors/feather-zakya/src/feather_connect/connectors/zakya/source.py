"""`zakya.source`: one of a Zakya organisation's lists, paged, read by `last_modified_time` or in full.

Each dataset is a list endpoint paged by `page` and `per_page` that says `page_context.has_more_page`. A `by_stamp`
read asks for `last_modified_time` at or after the watermark, sorted by it, and pages by stamp (as `http.Rest` does)
so that a record changed during the read is not skipped. A dataset that has line items (invoices, credit notes, price
lists) fetches each listed record on its own for them, one more call each. Records whose stamp is before the one asked
for are dropped here too, and a record read twice in one read at the same stamp is kept once, so an endpoint that
ignores the filter costs calls but no duplicates.
"""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import feather_connect as fc
from feather_connect.connectors import http  # pyright: ignore[reportAttributeAccessIssue]
from feather_connect.core.connector import ConnectorSpec, ReadRequest
from feather_connect.core.context import Context
from feather_connect.core.strategies import read_stamp

from .config import API_PREFIX, STAMP_FORMAT, Zakya, ZakyaConfig

__all__ = ["DATASETS", "Source", "ZakyaDataset", "dataset"]

DEFAULT_PAGE_SIZE = 200  # the most Zakya serves in a page
DEFAULT_LOOKBACK = "2m"  # how far back each by_stamp read starts again: a record saved as the last read ran


@dataclass(frozen=True)
class ZakyaDataset:
    """One of Zakya's lists: its `path` under /inventory/v1, where its records are in a page (`records`), the `key`
    that identifies a record and the `stamp` it changes by (None: read in full only), and the `detail` path and
    record of each one on its own, for the lines a list leaves out."""

    name: str
    path: str
    records: str
    key: str
    stamp: str | None
    detail: str | None = None
    detail_record: str | None = None


DATASETS: Mapping[str, ZakyaDataset] = {
    d.name: d
    for d in (
        ZakyaDataset(
            "invoices",
            "/invoices",
            "invoices",
            "invoice_id",
            "last_modified_time",
            "/invoices/{invoice_id}",
            "invoice",
        ),
        ZakyaDataset(
            "credit_notes",
            "/creditnotes",
            "creditnotes",
            "creditnote_id",
            "last_modified_time",
            "/creditnotes/{creditnote_id}",
            "creditnote",
        ),
        ZakyaDataset("payments", "/customerpayments", "customerpayments", "payment_id", "last_modified_time"),
        ZakyaDataset("items", "/items", "items", "item_id", "last_modified_time"),
        ZakyaDataset(
            "price_lists",
            "/pricebooks",
            "pricebooks",
            "pricebook_id",
            None,
            "/pricebooks/{pricebook_id}",
            "pricebook",
        ),
    )
}


class Source(Zakya, http.Rest):
    """A source that reads one of a Zakya organisation's lists: `dataset` is a key of `DATASETS` (invoices,
    credit_notes, payments, items or price_lists). `details` fetches each record on its own for its lines; it is on
    for the datasets that have them, and can be turned off to save a call per record. `page_size` is at most 200."""

    spec = ConnectorSpec(
        name="zakya.source",
        config=ZakyaConfig,
        strategies=frozenset({"by_stamp", "replace", "snapshot_diff"}),
        cost={"default": 1, "detail": 1, "token": 1},
        resumable=True,
        maturity="beta",
    )

    def __init__(
        self,
        connection: str = "zakya",
        dataset: str = "invoices",
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        details: bool | None = None,
        params: Mapping[str, Any] | None = None,
        max_pages: int = http.DEFAULT_MAX_PAGES,
    ) -> None:
        if dataset not in DATASETS:
            raise ValueError(f"Zakya has no dataset {dataset!r}: use one of {', '.join(DATASETS)}")
        if not 1 <= page_size <= DEFAULT_PAGE_SIZE:
            raise ValueError(f"page_size is from 1 to {DEFAULT_PAGE_SIZE}, not {page_size}")
        found = DATASETS[dataset]
        fetch = found.detail is not None if details is None else details and found.detail is not None
        sort = {"sort_column": found.stamp, "sort_order": "A"} if found.stamp else {}
        super().__init__(
            connection,
            path=API_PREFIX + found.path,
            records_path=found.records,
            pagination=http.Page(
                "page", "per_page", page_size=page_size, has_more_path="page_context.has_more_page"
            ),
            params={**sort, **(params or {})},
            stamp_param=found.stamp,
            stamp_format=STAMP_FORMAT,
            detail_path=API_PREFIX + found.detail if fetch and found.detail else None,
            detail_records_path=found.detail_record if fetch else None,
            max_pages=max_pages,
        )
        self.dataset = found

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        since = read_stamp(req.since) if req.since is not None and req.stamp_field else None
        field = req.stamp_field
        seen: set[tuple[str, str]] = set()
        for rows in super().read(ctx, req):
            kept: list[dict[str, Any]] = []
            for row in rows:
                stamp = row.get(field) if field else None
                if since is not None and stamp is not None and _before(read_stamp(stamp), since):
                    continue  # the endpoint did not filter by the stamp
                if field:
                    mark = (str(row.get(self.dataset.key)), str(stamp))
                    if mark in seen:
                        continue
                    seen.add(mark)
                kept.append(row)
            if kept:
                yield kept


def _before(stamp: Any, since: Any) -> bool:
    try:
        return bool(stamp < since)
    except TypeError:  # stamps of two kinds: keep the record, for the strategy to judge
        return False


def dataset(
    name: str,
    connection: str = "zakya",
    *,
    full: bool = False,
    lookback: str = DEFAULT_LOOKBACK,
    page_size: int = DEFAULT_PAGE_SIZE,
    details: bool | None = None,
    as_name: str | None = None,
) -> fc.Dataset:
    """A dataset of Zakya's `name` list, keyed by its id. It is read by `last_modified_time`, going back `lookback`
    each time (records read again unchanged are dropped), or in full with `full=True` (`snapshot_diff`, which sees
    deletes too). Price lists have no stamp, and are always read in full. `as_name` names the dataset (`name` by
    default)."""
    found = DATASETS[name]
    source = Source(connection, name, page_size=page_size, details=details)
    if full or found.stamp is None:
        strategy = fc.strategy.snapshot_diff()
    else:
        strategy = fc.strategy.by_stamp(found.stamp, lookback=lookback)
    return fc.Dataset(as_name or name, source=source, key=found.key, strategy=strategy)

"""The Zakya sinks: items, prices in a price list, and stock adjustments. Each is idempotent as far as Zakya allows.

- `zakya.items` upserts an item by its SKU: it looks the SKU up, then updates the item it finds or creates one, and
  sends nothing when the item already holds the record's values.
- `zakya.prices` sets item rates in a price list (a per-item price book): it reads the price list, changes the rates
  that differ and writes it back once per price list in a chunk.
- `zakya.stock` posts an inventory adjustment per record, a change of quantity, with a `reference_number` made from
  the chunk's idempotency key and the record's key; before posting it looks the reference up, so a repeat posts
  nothing.

A 401 or 403 (the connection's credentials or rights), and a 404 of the API itself, fail the chunk. Any other 4xx
rejects the record. Deletes are rejected: an item or a price is ended in Zakya, not by a flow.
"""

import hashlib
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any, ClassVar, cast
from zoneinfo import ZoneInfo

from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyRecord,
    ApplyResult,
    ConnectorSpec,
    Key,
    RecordResponse,
    Rejection,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.records import canonical_json

from .config import Zakya, ZakyaConfig, ZakyaError

__all__ = ["Items", "Prices", "Stock"]

RecordMap = Callable[[Mapping[str, Any]], Mapping[str, Any]]
_FAIL_STATUSES = frozenset(
    {401, 403, 405}
)  # the connection is wrong, not the record: every record would fail


def _key_text(key: Key) -> str:
    return key if isinstance(key, str) else canonical_json(list(key)).decode("utf-8")


class _Sink(Zakya, connector.Sink):
    spec: ClassVar[ConnectorSpec]
    kind: ClassVar[str]

    def __init__(self, connection: str, *, map: RecordMap | None = None, match: str = "sku") -> None:
        super().__init__()
        self.connection = connection
        self.map = map
        self.match = match
        self._item_ids: dict[str, str] = {}

    def _probe(self, ctx: Context) -> None:
        self.client(ctx)  # the settings and the credentials: a call would change the organisation

    def _body(self, record: ApplyRecord) -> dict[str, Any]:
        payload = cast(Mapping[str, Any], record.payload)
        return dict(self.map(payload) if self.map is not None else payload)

    @staticmethod
    def _fails_chunk(error: ZakyaError, path: str) -> bool:
        return error.status in _FAIL_STATUSES or (error.status == 404 and error.code in (None, 5))

    def find_item(self, ctx: Context, value: str) -> dict[str, Any] | None:
        """The item whose `match` field (its SKU) is `value`, or None. Raises `ZakyaError` for two of them."""
        found = self.call(ctx, "GET", "/items", params={self.match: value}, cost_key="lookup")
        items = [
            cast(dict[str, Any], i)
            for i in cast(list[Any], found.get("items") or [])
            if isinstance(i, dict) and str(cast(dict[str, Any], i).get(self.match)) == value
        ]
        if len(items) > 1:
            raise ZakyaError(f"{len(items)} items have {self.match} {value!r}; one is wanted", None, 409)
        return items[0] if items else None

    def item_id(self, ctx: Context, value: str) -> str | None:
        if value not in self._item_ids:
            item = self.find_item(ctx, value)
            if item is None:
                return None
            self._item_ids[value] = str(item["item_id"])
        return self._item_ids[value]

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        applied = 0
        rejected: list[Rejection] = []
        responses: list[RecordResponse] = []
        for record, outcome in self._each(ctx, batch):
            if isinstance(outcome, Rejection):
                rejected.append(outcome)
                continue
            applied += 1
            ctx.answered(outcome)
            responses.append(outcome)
        return ApplyResult(applied, rejected, responses=responses)

    def _each(self, ctx: Context, batch: ApplyBatch) -> list[tuple[ApplyRecord, RecordResponse | Rejection]]:
        results: list[tuple[ApplyRecord, RecordResponse | Rejection]] = []
        for record in batch.records:
            if record.op == "delete" or record.payload is None:
                why = f"{self.spec.name} doesn't delete: end it in Zakya"
                results.append((record, Rejection(record.key, "DeleteNotSupported", why)))
                continue
            try:
                results.append((record, self.one(ctx, batch, record)))
            except ZakyaError as error:
                if self._fails_chunk(error, ""):
                    raise PermanentError(
                        f"{error}; every record would get the same, so the chunk fails"
                    ) from error
                results.append((record, Rejection(record.key, "ZakyaError", str(error))))
        return results

    def one(self, ctx: Context, batch: ApplyBatch, record: ApplyRecord) -> RecordResponse | Rejection:
        raise NotImplementedError


class Items(_Sink):
    """Upsert each record as a Zakya item, matched by `match` (its `sku`). `map` turns a record into the item's
    fields (the record as it is by default); they must hold the SKU. An item that has the record's values already is
    left alone."""

    spec = ConnectorSpec(
        name="zakya.items",
        config=ZakyaConfig,
        modes=frozenset({"upsert"}),
        cost={"default": 1, "lookup": 1, "token": 1},
        maturity="beta",
    )

    def one(self, ctx: Context, batch: ApplyBatch, record: ApplyRecord) -> RecordResponse | Rejection:
        body = self._body(record)
        value = body.get(self.match)
        if value in (None, ""):
            return Rejection(record.key, "MissingField", f"the item has no {self.match}")
        found = self.find_item(ctx, str(value))
        if found is None:
            answer = self.call(ctx, "POST", "/items", body=body)
            item = cast(dict[str, Any], answer.get("item") or {})
            self._item_ids[str(value)] = str(item.get("item_id"))
            return RecordResponse(record.key, 201, {"item_id": item.get("item_id")})
        item_id = str(found["item_id"])
        if all(found.get(k) == v for k, v in body.items()):
            return RecordResponse(record.key, 200, {"item_id": item_id})  # as it is: nothing to send
        self.call(ctx, "PUT", f"/items/{item_id}", body=body)
        return RecordResponse(record.key, 200, {"item_id": item_id})


class Prices(_Sink):
    """Set item rates in Zakya price lists. Each record names the price list (`pricebook_field`, `pricebook_id` by
    default; or `pricebook_id=` names one for all), the item by `match` (its `sku`) and the rate (`rate_field`). A
    chunk reads each price list it touches once and writes it once, if a rate changed. Only per-item price lists
    have rates to set."""

    spec = ConnectorSpec(
        name="zakya.prices",
        config=ZakyaConfig,
        modes=frozenset({"upsert"}),
        cost={"default": 1, "lookup": 1, "token": 1},
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        map: RecordMap | None = None,
        match: str = "sku",
        rate_field: str = "rate",
        pricebook_field: str = "pricebook_id",
        pricebook_id: str | None = None,
    ) -> None:
        super().__init__(connection, map=map, match=match)
        self.rate_field = rate_field
        self.pricebook_field = pricebook_field
        self.pricebook_id = pricebook_id

    def _each(self, ctx: Context, batch: ApplyBatch) -> list[tuple[ApplyRecord, RecordResponse | Rejection]]:
        results: list[tuple[ApplyRecord, RecordResponse | Rejection]] = []
        books: dict[str, list[tuple[ApplyRecord, str, float]]] = {}
        for record in batch.records:
            if record.op == "delete" or record.payload is None:
                why = "zakya.prices doesn't delete: end the price in Zakya"
                results.append((record, Rejection(record.key, "DeleteNotSupported", why)))
                continue
            body = self._body(record)
            book = self.pricebook_id or body.get(self.pricebook_field)
            value, rate = body.get(self.match), body.get(self.rate_field)
            if book in (None, "") or value in (None, "") or not isinstance(rate, int | float | str):
                why = f"a price needs {self.pricebook_field}, {self.match} and {self.rate_field}"
                results.append((record, Rejection(record.key, "MissingField", why)))
                continue
            try:
                books.setdefault(str(book), []).append((record, str(value), float(rate)))
            except ValueError:
                results.append((record, Rejection(record.key, "BadValue", f"{rate!r} is not a rate")))
        for book, wanted in books.items():
            try:
                results += self._book(ctx, book, wanted)
            except ZakyaError as error:
                if self._fails_chunk(error, ""):
                    raise PermanentError(
                        f"{error}; every record would get the same, so the chunk fails"
                    ) from error
                results += [(r, Rejection(r.key, "ZakyaError", str(error))) for r, _, _ in wanted]
        return results

    def _book(
        self, ctx: Context, book: str, wanted: list[tuple[ApplyRecord, str, float]]
    ) -> list[tuple[ApplyRecord, RecordResponse | Rejection]]:
        found = cast(dict[str, Any], self.call(ctx, "GET", f"/pricebooks/{book}").get("pricebook") or {})
        if found.get("pricebook_type", "per_item") != "per_item":
            why = f"price list {book} is {found.get('pricebook_type')}, not per_item: it has no rates to set"
            return [(r, Rejection(r.key, "NotPerItem", why)) for r, _, _ in wanted]
        rates = {
            str(cast(dict[str, Any], i)["item_id"]): cast(dict[str, Any], i).get("pricebook_rate")
            for i in cast(list[Any], found.get("pricebook_items") or [])
            if isinstance(i, dict)
        }
        results: list[tuple[ApplyRecord, RecordResponse | Rejection]] = []
        changed = False
        for record, value, rate in wanted:
            item_id = self.item_id(ctx, value)
            if item_id is None:
                why = f"no item has {self.match} {value!r}"
                results.append((record, Rejection(record.key, "UnknownItem", why)))
                continue
            if rates.get(item_id) != rate:
                rates[item_id], changed = rate, True
            results.append(
                (record, RecordResponse(record.key, 200, {"pricebook_id": book, "item_id": item_id}))
            )
        if changed:
            items = [{"item_id": i, "pricebook_rate": r} for i, r in sorted(rates.items())]
            self.call(ctx, "PUT", f"/pricebooks/{book}", body={"pricebook_items": items})
        return results


class Stock(_Sink):
    """Post a stock adjustment per record: `quantity_field` (`quantity_adjusted`) more or fewer of the item matched
    by `match` (its `sku`), in `warehouse_field`'s warehouse when the record names one, with `reason`. Its
    `reference_number` is made from the chunk's idempotency key and the record's key, and an adjustment with that
    reference is never posted twice."""

    spec = ConnectorSpec(
        name="zakya.stock",
        config=ZakyaConfig,
        modes=frozenset({"append"}),
        cost={"default": 1, "lookup": 1, "token": 1},
        maturity="beta",
    )

    def __init__(
        self,
        connection: str,
        *,
        map: RecordMap | None = None,
        match: str = "sku",
        quantity_field: str = "quantity_adjusted",
        warehouse_field: str = "warehouse_id",
        reason: str = "Feather Connect",
    ) -> None:
        super().__init__(connection, map=map, match=match)
        self.quantity_field = quantity_field
        self.warehouse_field = warehouse_field
        self.reason = reason

    @staticmethod
    def reference(batch: ApplyBatch, record: ApplyRecord) -> str:
        """The adjustment's `reference_number`: the same each time the record is sent again, and short."""
        digest = hashlib.sha256(f"{batch.idempotency_key}:{_key_text(record.key)}".encode()).hexdigest()
        return f"FC-{digest[:20]}"

    def one(self, ctx: Context, batch: ApplyBatch, record: ApplyRecord) -> RecordResponse | Rejection:
        body = self._body(record)
        value, quantity = body.get(self.match), body.get(self.quantity_field)
        if value in (None, "") or not isinstance(quantity, int | float) or isinstance(quantity, bool):
            why = f"a stock adjustment needs {self.match} and a number in {self.quantity_field}"
            return Rejection(record.key, "MissingField", why)
        reference = self.reference(batch, record)
        listed = self.call(
            ctx, "GET", "/inventoryadjustments", params={"reference_number": reference}, cost_key="lookup"
        )
        for adjustment in cast(list[Any], listed.get("inventory_adjustments") or []):
            if (
                isinstance(adjustment, dict)
                and cast(dict[str, Any], adjustment).get("reference_number") == reference
            ):
                held = cast(dict[str, Any], adjustment)
                return RecordResponse(
                    record.key, 200, {"inventory_adjustment_id": held.get("inventory_adjustment_id")}
                )
        item_id = self.item_id(ctx, str(value))
        if item_id is None:
            return Rejection(record.key, "UnknownItem", f"no item has {self.match} {value!r}")
        line: dict[str, Any] = {"item_id": item_id, "quantity_adjusted": quantity}
        if body.get(self.warehouse_field) not in (None, ""):
            line["warehouse_id"] = body[self.warehouse_field]
        day = datetime.now(ZoneInfo(ctx.tz)) if ctx.now is None else ctx.now.astimezone(ZoneInfo(ctx.tz))
        answer = self.call(
            ctx,
            "POST",
            "/inventoryadjustments",
            body={
                "date": str(body.get("date") or day.date().isoformat()),
                "reason": str(body.get("reason") or self.reason),
                "adjustment_type": "quantity",
                "reference_number": reference,
                "line_items": [line],
            },
        )
        made = cast(dict[str, Any], answer.get("inventory_adjustment") or {})
        return RecordResponse(
            record.key, 201, {"inventory_adjustment_id": made.get("inventory_adjustment_id")}
        )

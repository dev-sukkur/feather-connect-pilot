"""The demo's stand-in for Zakya: the `feather-zakya` package's simulator (its sign-in, paged lists by
`last_modified_time`, rate limits and faults), with the demo's own sales on top.

- Master data from `master.json`, shared with the ERP simulator: each material has two barcodes, its EAN-13 and its
  POS code (Zakya's SKU), and each customer is B2B (`gst_treatment: business_gst`, with a GSTIN) or B2C (`consumer`).
- Auto mode raises invoices as time passes, B2B and B2C, pays some open B2B ones, and now and then takes goods back
  (a credit note against an earlier invoice). Manual mode raises nothing on its own.
- The page at `/` is the manual counter: enter an invoice (customer, lines by either barcode, quantity, price), take
  goods back against an invoice, pay an invoice, and switch between the modes. It calls the JSON under `/sim/`.

A B2C invoice is a counter sale, paid as it is made; a B2B one is raised `sent` and paid later. With `--state`, the
invoices and credit notes are kept in a JSON file, so a restart carries on where it stopped.

    python sims/zakya.py --port 29100 --state .data/zakya.json            # auto mode
    python sims/zakya.py --port 29100 --state .data/zakya.json --manual   # manual mode
"""

import argparse
import json
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import httpx

from feather_connect.connectors.zakya.testing import (  # pyright: ignore[reportMissingImports]
    IST,
    running,
    serve,
    stamp,
)
from feather_connect.connectors.zakya.testing import ZakyaSim as BaseZakyaSim  # pyright: ignore[reportMissingImports]

__all__ = ["CUSTOMERS", "MATERIALS", "ZakyaSim", "channel_of", "main", "running"]

MASTER: dict[str, Any] = json.loads(Path(__file__).with_name("master.json").read_text(encoding="utf-8"))
MATERIALS: list[dict[str, Any]] = MASTER["materials"]
CUSTOMERS: list[dict[str, Any]] = MASTER["customers"]
STATE_VERSION = 2  # the invoices of an older state file have no barcodes or GST treatment: it is set aside
TAX = 0.05  # one GST rate on everything, for the demo


def channel_of(document: dict[str, Any]) -> str:
    """B2B for a customer registered for GST, B2C for a consumer."""
    return "B2B" if str(document.get("gst_treatment") or "").startswith("business") else "B2C"


def material_of(code: str) -> dict[str, Any]:
    """The material with `code` as its EAN, its POS code or its ERP number."""
    for material in MATERIALS:
        if code in (material["ean"], material["pos_code"], material["material"]):
            return material
    raise ValueError(f"no material has the barcode or code {code!r}")


def customer_of(customer_id: str) -> dict[str, Any]:
    for customer in CUSTOMERS:
        if customer["customer_id"] == customer_id:
            return customer
    raise ValueError(f"no customer {customer_id!r}")


def _line(number: int, material: dict[str, Any], quantity: float, rate: float) -> dict[str, Any]:
    return {
        "line_item_id": f"{number}",
        "item_id": material["item_id"],
        "sku": material["pos_code"],
        "ean": material["ean"],
        "name": material["description"],
        "quantity": quantity,
        "rate": rate,
        "item_total": round(quantity * rate, 2),
    }


class ZakyaSim(BaseZakyaSim):
    """The package's simulator with the demo's sales: B2B and B2C invoices of the shared materials, and credit notes.

    `every` (seconds) is auto mode, as the package's; None is manual. `b2c_share` is the share of auto invoices that
    are counter sales, `bulk_rate` the share of B2B ones that are bulk orders (over 5 lakh, which the chain holds for
    approval) and `return_rate` the chance, at each tick, that goods come back.
    """

    def __init__(
        self,
        *,
        b2c_share: float = 0.6,
        bulk_rate: float = 0.05,
        return_rate: float = 0.15,
        state: Path | None = None,
        **kwargs: Any,
    ) -> None:
        self.b2c_share = b2c_share
        self.bulk_rate = bulk_rate
        self.return_rate = return_rate
        self._next_note = 1
        self._auto_every: float | None = kwargs.get("every") or 20.0
        if state is not None and state.is_file():
            kept = json.loads(state.read_text(encoding="utf-8"))
            if kept.get("version") != STATE_VERSION:
                state.replace(state.with_suffix(".old.json"))
        super().__init__(state=state, **kwargs)
        if state is not None and state.is_file():
            kept = json.loads(state.read_text(encoding="utf-8"))
            self.data["creditnotes"] = kept.get("creditnotes", {})
            self._next_note = kept.get("next_note", 1)

    # Modes.

    @property
    def mode(self) -> str:
        return "manual" if self.every is None else "auto"

    def set_mode(self, mode: str, every: float | None = None) -> None:
        """Switch to `auto` (raising sales every `every` seconds, or as before) or to `manual`."""
        if mode not in ("auto", "manual"):
            raise ValueError("mode is auto or manual")
        with self._lock:
            if mode == "auto":
                self._auto_every = every or self._auto_every or 20.0
                self.every = self._auto_every
                self._last_tick = time.monotonic()
            else:
                self.every = None

    # The records.

    @property
    def credit_notes(self) -> dict[str, dict[str, Any]]:
        return self.data["creditnotes"]

    def _save(self) -> None:
        if self._state is None:
            return
        self._state.parent.mkdir(parents=True, exist_ok=True)
        draft = self._state.with_suffix(".tmp")
        kept = {
            "version": STATE_VERSION,
            "next": self._next,
            "next_note": self._next_note,
            "invoices": self.invoices,
            "creditnotes": self.data["creditnotes"],
        }
        draft.write_text(json.dumps(kept), encoding="utf-8")
        draft.replace(self._state)

    def raise_invoice(self, customer_id: str, lines: list[dict[str, Any]], *, date: str | None = None) -> str:
        """Raise an invoice now: `lines` are `{"code": EAN or POS code, "quantity": n, "rate": price}`, the rate the
        material's list price when left out. A B2C invoice is paid as it is made."""
        customer = customer_of(customer_id)
        if not lines:
            raise ValueError("an invoice needs at least one line")
        made: list[dict[str, Any]] = []
        for i, line in enumerate(lines, start=1):
            material = material_of(str(line.get("code") or line.get("ean") or line.get("sku") or ""))
            quantity = float(line.get("quantity") or 0)
            if quantity <= 0:
                raise ValueError(f"line {i}: the quantity must be above 0")
            rate = float(line.get("rate") or material["price"])
            made.append(_line(i, material, quantity, rate))
        with self._lock:
            number = self._next
            self._next += 1
            today = self.now().astimezone(IST).date()
            sub_total = round(sum(line["item_total"] for line in made), 2)
            total = round(sub_total * (1 + TAX), 2)
            b2c = channel_of(customer) == "B2C"
            invoice_id = f"46000000{number:05d}"
            self.invoices[invoice_id] = {
                "invoice_id": invoice_id,
                "invoice_number": f"INV-{number:05d}",
                "customer_id": customer["customer_id"],
                "customer_name": customer["customer_name"],
                "gst_treatment": customer["gst_treatment"],
                "gst_no": customer["gst_no"],
                "date": date or today.isoformat(),
                "due_date": (today + timedelta(days=0 if b2c else 14)).isoformat(),
                "status": "paid" if b2c else "sent",
                "currency_code": "INR",
                "sub_total": sub_total,
                "tax_total": round(total - sub_total, 2),
                "total": total,
                "balance": 0.0 if b2c else total,
                "last_modified_time": stamp(self.now()),
                "line_items": made,
            }
            self._sorted.clear()
            self._save()
            return invoice_id

    def returnable(self, invoice_id: str) -> dict[str, float]:
        """What of an invoice can still come back: quantity by EAN, less what credit notes took back already."""
        invoice = self.invoices.get(invoice_id)
        if invoice is None:
            raise ValueError(f"no invoice {invoice_id!r}")
        left: dict[str, float] = {}
        for line in invoice["line_items"]:
            left[line["ean"]] = left.get(line["ean"], 0.0) + float(line["quantity"])
        for note in self.credit_notes.values():
            if note.get("invoice_id") == invoice_id:
                for line in note["line_items"]:
                    left[line["ean"]] = left.get(line["ean"], 0.0) - float(line["quantity"])
        return left

    def return_goods(
        self, invoice_id: str, lines: list[dict[str, Any]] | None = None, *, reason: str = "returned by customer"
    ) -> str:
        """Take goods back against an invoice: a credit note for `lines` (`{"code": ..., "quantity": n}`), or for all
        that is left of the invoice when None. Each line goes back at the invoice's rate, and no more than was sold
        and not yet returned."""
        with self._lock:
            invoice = self.invoices.get(invoice_id)
            if invoice is None:
                raise ValueError(f"no invoice {invoice_id!r}")
            left = self.returnable(invoice_id)
            rates = {line["ean"]: float(line["rate"]) for line in invoice["line_items"]}
            wanted = (
                [{"code": ean, "quantity": quantity} for ean, quantity in left.items() if quantity > 0]
                if lines is None
                else lines
            )
            made: list[dict[str, Any]] = []
            for i, line in enumerate(wanted, start=1):
                material = material_of(str(line.get("code") or line.get("ean") or line.get("sku") or ""))
                quantity = float(line.get("quantity") or 0)
                ean = material["ean"]
                if quantity <= 0:
                    continue
                if quantity > left.get(ean, 0.0) + 1e-9:
                    raise ValueError(
                        f"{material['description']}: {quantity:g} back, but only {left.get(ean, 0.0):g} of "
                        f"{invoice['invoice_number']} is left to return"
                    )
                made.append(_line(i, material, quantity, rates[ean]))
            if not made:
                raise ValueError(f"nothing of {invoice['invoice_number']} is left to return")
            number = self._next_note
            self._next_note += 1
            sub_total = round(sum(line["item_total"] for line in made), 2)
            total = round(sub_total * (1 + TAX), 2)
            note_id = f"47000000{number:05d}"
            self.credit_notes[note_id] = {
                "creditnote_id": note_id,
                "creditnote_number": f"CN-{number:05d}",
                "invoice_id": invoice_id,
                "invoice_number": invoice["invoice_number"],
                "customer_id": invoice["customer_id"],
                "customer_name": invoice["customer_name"],
                "gst_treatment": invoice["gst_treatment"],
                "gst_no": invoice["gst_no"],
                "date": self.now().astimezone(IST).date().isoformat(),
                "status": "open",
                "reason": reason,
                "currency_code": "INR",
                "sub_total": sub_total,
                "tax_total": round(total - sub_total, 2),
                "total": total,
                "last_modified_time": stamp(self.now()),
                "line_items": made,
            }
            self._sorted.clear()
            self._save()
            return note_id

    def add(self, count: int = 1, *, channel: str | None = None) -> list[str]:
        """Raise `count` invoices of random customers and materials now, B2C at `b2c_share` unless `channel` says."""
        made: list[str] = []
        with self._lock:
            for _ in range(count):
                chosen = channel or ("B2C" if self._random.random() < self.b2c_share else "B2B")
                customer = self._random.choice([c for c in CUSTOMERS if channel_of(c) == chosen])
                bulk = chosen == "B2B" and self._random.random() < self.bulk_rate
                most = 5 if chosen == "B2C" else 40
                lines = [
                    {
                        "code": m["ean"] if self._random.random() < 0.5 else m["pos_code"],  # either barcode
                        "quantity": self._random.randint(800, 1200) if bulk else self._random.randint(1, most),
                    }
                    for m in self._random.sample(MATERIALS, self._random.randint(1, 3))
                ]
                made.append(self.raise_invoice(customer["customer_id"], lines))
        return made

    def take_back_some(self) -> str | None:
        """Goods come back: one unit of one line of a random invoice that has something left to return."""
        with self._lock:
            candidates = [i for i in self.invoices if any(q > 0 for q in self.returnable(i).values())]
            if not candidates:
                return None
            invoice_id = self._random.choice(candidates)
            ean = self._random.choice([e for e, q in self.returnable(invoice_id).items() if q > 0])
            return self.return_goods(invoice_id, [{"code": ean, "quantity": 1}], reason="damaged pack")

    def tick(self) -> None:
        """Let the time since the last tick bring its sales: new invoices, an open B2B one paid, now and then a return."""
        if self.every is None:
            return
        now = time.monotonic()
        while now - self._last_tick >= self.every:
            self._last_tick += self.every
            self.add(self._random.randint(1, 2))
            if self._random.random() < 0.5:
                self.change()  # only B2B invoices are open
            if self._random.random() < self.return_rate:
                self.take_back_some()

    # The manual counter: the page and its JSON.

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/" and request.method == "GET":
            return httpx.Response(200, html=PAGE)
        if path.startswith("/sim/"):
            with self._lock:
                try:
                    return self._counter(request, path.removeprefix("/sim/"))
                except (ValueError, KeyError, TypeError) as error:
                    return httpx.Response(400, json={"error": str(error).strip("'\"")})
        return super()._handle(request)

    def _counter(self, request: httpx.Request, action: str) -> httpx.Response:
        if request.method == "GET" and action == "state":
            return httpx.Response(200, json=self.snapshot())
        if request.method != "POST":
            return httpx.Response(405, json={"error": "POST it"})
        body = cast(dict[str, Any], json.loads(request.content or b"{}"))
        if action == "invoices":
            made = self.raise_invoice(str(body["customer_id"]), list(body.get("lines") or []))
            return httpx.Response(201, json={"invoice_id": made})
        if action == "creditnotes":
            lines = body.get("lines")
            made = self.return_goods(str(body["invoice_id"]), None if lines is None else list(lines))
            return httpx.Response(201, json={"creditnote_id": made})
        if action == "pay":
            invoice = self.invoices.get(str(body["invoice_id"]))
            if invoice is None or invoice["status"] == "paid":
                raise ValueError("no open invoice by that id")
            self.change(str(body["invoice_id"]))
            return httpx.Response(200, json={"invoice_id": body["invoice_id"]})
        if action == "mode":
            every = body.get("every")
            self.set_mode(str(body["mode"]), float(every) if every else None)
            return httpx.Response(200, json={"mode": self.mode, "every": self.every})
        return httpx.Response(404, json={"error": f"no action {action!r}"})

    def snapshot(self, newest: int = 40) -> dict[str, Any]:
        """What the page shows: the mode, the master data, and the newest invoices and credit notes."""
        invoices = sorted(self.invoices.values(), key=lambda v: v["invoice_number"], reverse=True)[:newest]
        notes = sorted(self.credit_notes.values(), key=lambda v: v["creditnote_number"], reverse=True)[:newest]
        return {
            "mode": self.mode,
            "every": self.every or self._auto_every,
            "materials": MATERIALS,
            "customers": [{**c, "channel": channel_of(c)} for c in CUSTOMERS],
            "invoices": [
                {**v, "channel": channel_of(v), "returnable": self.returnable(v["invoice_id"])} for v in invoices
            ],
            "creditnotes": [{**v, "channel": channel_of(v)} for v in notes],
        }


PAGE = (Path(__file__).with_name("zakya.html")).read_text(encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="The demo's Zakya: B2B and B2C sales, auto or by hand.")
    parser.add_argument("--port", type=int, default=29100)
    parser.add_argument("--every", type=float, default=20.0, help="auto mode: seconds between new sales")
    parser.add_argument("--manual", action="store_true", help="start in manual mode: sales only from the page")
    parser.add_argument("--fault-rate", type=float, default=0.04, help="share of API calls that answer 429 or 503")
    parser.add_argument("--start", type=int, default=5, help="invoices to begin with")
    parser.add_argument("--state", type=Path, help="a JSON file that keeps the sales across restarts")
    args = parser.parse_args(argv)
    sim = ZakyaSim(start=args.start, every=args.every, fault_rate=args.fault_rate, state=args.state)
    if args.manual:
        sim.set_mode("manual")
    print(f"Zakya simulator on http://127.0.0.1:{args.port}/ ({sim.mode} mode)", flush=True)
    serve(sim, port=args.port).serve_forever()


if __name__ == "__main__":
    main()

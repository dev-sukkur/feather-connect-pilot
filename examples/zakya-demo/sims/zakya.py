"""A small stand-in for Zakya (Zoho Inventory's API), for the demo: OAuth2 refresh-token sign-in, a paged invoice list
filtered by `last_modified_time`, and each invoice on its own with its line items.

New invoices appear as time passes and some existing ones change (sent, then paid), so each run of the flow has
something new to read. A few calls answer 429 or 503 on purpose, as a real API now and then does. With `--state`,
the invoices are kept in a JSON file, so a restart carries on where it stopped instead of raising the same invoices
again with new times.

    python sims/zakya.py --port 29100 --state .data/zakya.json
"""

import argparse
import json
import random
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

IST = timezone(timedelta(hours=5, minutes=30))
STAMP = "%Y-%m-%dT%H:%M:%S%z"
ORGANIZATION_ID = "60001"
CUSTOMERS = [
    ("c-101", "Asha Traders"),
    ("c-102", "Bharat Stores"),
    ("c-103", "Chennai Mart"),
    ("c-104", "Delhi Deli"),
]
ITEMS = [
    ("TEA-250", "Assam tea 250 g", 180.0),
    ("RICE-5", "Basmati rice 5 kg", 640.0),
    ("OIL-1", "Mustard oil 1 l", 210.0),
]


def _stamp(moment: datetime) -> str:
    return moment.astimezone(IST).strftime(STAMP)


def _parse_stamp(text: str) -> datetime:
    # A `+` that a client left unescaped in the query arrives as a space.
    return datetime.strptime(text.replace(" ", "+"), STAMP)


class Zakya:
    """The simulated organisation: its invoices, the access tokens it has issued, and how often it fails a call.

    `every` is how many seconds pass between changes (None: only when `add` or `change` is called, as tests do).
    `state` is a JSON file that keeps the invoices across restarts (None: in memory only).
    """

    def __init__(
        self,
        *,
        seed: int = 7,
        start: int = 5,
        every: float | None = 20.0,
        fault_rate: float = 0.04,
        state: Path | None = None,
    ) -> None:
        self._random = random.Random(seed)
        self._lock = threading.Lock()
        self.invoices: dict[str, dict[str, Any]] = {}
        self.tokens: set[str] = set()
        self.every = every
        self.fault_rate = fault_rate
        self._state = state
        self._next = 1
        self._last_tick = time.monotonic()
        if state is not None and state.is_file():
            kept = json.loads(state.read_text(encoding="utf-8"))
            self.invoices = kept["invoices"]
            self._next = kept["next"]
            self._random.seed(seed + self._next)
        else:
            self.add(start)

    def _save(self) -> None:
        """Write the invoices to the state file, if there is one. Called with the lock held."""
        if self._state is None:
            return
        self._state.parent.mkdir(parents=True, exist_ok=True)
        draft = self._state.with_suffix(".tmp")
        draft.write_text(json.dumps({"next": self._next, "invoices": self.invoices}), encoding="utf-8")
        draft.replace(self._state)

    def add(self, count: int = 1) -> list[str]:
        """Raise `count` new invoices, now."""
        made: list[str] = []
        with self._lock:
            for _ in range(count):
                number = self._next
                self._next += 1
                customer_id, customer_name = self._random.choice(CUSTOMERS)
                lines = []
                for sku, name, rate in self._random.sample(ITEMS, self._random.randint(1, len(ITEMS))):
                    quantity = self._random.randint(1, 5)
                    lines.append(
                        {
                            "sku": sku,
                            "name": name,
                            "quantity": quantity,
                            "rate": rate,
                            "item_total": quantity * rate,
                        }
                    )
                total = round(sum(line["item_total"] for line in lines) * 1.05, 2)
                today = datetime.now(IST).date()
                invoice_id = f"46000000{number:05d}"
                self.invoices[invoice_id] = {
                    "invoice_id": invoice_id,
                    "invoice_number": f"INV-{number:05d}",
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "date": today.isoformat(),
                    "due_date": (today + timedelta(days=14)).isoformat(),
                    "status": "sent",
                    "currency_code": "INR",
                    "total": total,
                    "balance": total,
                    "last_modified_time": _stamp(datetime.now(IST)),
                    "line_items": lines,
                }
                made.append(invoice_id)
            self._save()
        return made

    def change(self, invoice_id: str | None = None) -> str | None:
        """Mark an invoice paid (one still open, at random, when none is named), and stamp it now."""
        with self._lock:
            open_ones = [i for i, v in self.invoices.items() if v["status"] != "paid"]
            chosen = invoice_id or (self._random.choice(open_ones) if open_ones else None)
            if chosen is None:
                return None
            invoice = self.invoices[chosen]
            invoice.update(status="paid", balance=0.0, last_modified_time=_stamp(datetime.now(IST)))
            self._save()
            return chosen

    def tick(self) -> None:
        """Let the time that passed since the last tick bring its new and changed invoices."""
        if self.every is None:
            return
        now = time.monotonic()
        while now - self._last_tick >= self.every:
            self._last_tick += self.every
            self.add(self._random.randint(1, 2))
            if self._random.random() < 0.5:
                self.change()

    def fault(self) -> JSONResponse | None:
        """Now and then, the answer of an API that is busy or down for a moment."""
        roll = self._random.random()
        if roll < self.fault_rate / 2:
            return JSONResponse(
                {"code": 44, "message": "Too many requests"}, status_code=429, headers={"Retry-After": "1"}
            )
        if roll < self.fault_rate:
            return JSONResponse({"code": 503, "message": "Service unavailable"}, status_code=503)
        return None

    def listed(self, since: datetime | None) -> list[dict[str, Any]]:
        with self._lock:
            found = [
                {k: v for k, v in invoice.items() if k != "line_items"}
                for invoice in self.invoices.values()
                if since is None or _parse_stamp(invoice["last_modified_time"]) >= since
            ]
        return sorted(found, key=lambda i: (_parse_stamp(i["last_modified_time"]), i["invoice_id"]))


def create_app(zakya: Zakya) -> Starlette:
    def refused(request: Request) -> JSONResponse | None:
        """The answer to a call that isn't signed in, or not for the organisation; None for one that is."""
        header = request.headers.get("authorization", "")
        scheme, _, token = header.partition(" ")
        if scheme != "Zoho-oauthtoken" or token not in zakya.tokens:
            return JSONResponse(
                {"code": 57, "message": "You are not authorized to perform this operation"}, status_code=401
            )
        if request.query_params.get("organization_id") != ORGANIZATION_ID:
            return JSONResponse(
                {"code": 6041, "message": "Invalid value passed for organization_id"}, status_code=400
            )
        return None

    async def token(request: Request) -> JSONResponse:
        form = await request.form()
        if form.get("grant_type") != "refresh_token" or not all(
            form.get(k) for k in ("client_id", "client_secret", "refresh_token")
        ):
            return JSONResponse({"error": "invalid_client"}, status_code=400)
        issued = secrets.token_hex(16)
        zakya.tokens.add(issued)
        return JSONResponse(
            {
                "access_token": issued,
                "api_domain": "http://127.0.0.1",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )

    async def invoices(request: Request) -> JSONResponse:
        zakya.tick()
        if (answer := refused(request) or zakya.fault()) is not None:
            return answer
        since_text = request.query_params.get("last_modified_time")
        page = int(request.query_params.get("page", "1"))
        per_page = min(int(request.query_params.get("per_page", "200")), 200)
        listed = zakya.listed(_parse_stamp(since_text) if since_text else None)
        chunk = listed[(page - 1) * per_page : page * per_page]
        context = {"page": page, "per_page": per_page, "has_more_page": page * per_page < len(listed)}
        return JSONResponse({"code": 0, "message": "success", "invoices": chunk, "page_context": context})

    async def invoice(request: Request) -> JSONResponse:
        if (answer := refused(request) or zakya.fault()) is not None:
            return answer
        found = zakya.invoices.get(request.path_params["invoice_id"])
        if found is None:
            return JSONResponse({"code": 5, "message": "Invalid URL Passed"}, status_code=404)
        return JSONResponse({"code": 0, "message": "success", "invoice": found})

    return Starlette(
        routes=[
            Route("/oauth/v2/token", token, methods=["POST"]),
            Route("/inventory/v1/invoices", invoices),
            Route("/inventory/v1/invoices/{invoice_id}", invoice),
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--port", type=int, default=29100)
    parser.add_argument("--every", type=float, default=20.0, help="seconds between new or changed invoices")
    parser.add_argument(
        "--fault-rate", type=float, default=0.04, help="share of calls that answer 429 or 503"
    )
    parser.add_argument("--state", type=Path, help="a JSON file that keeps the invoices across restarts")
    args = parser.parse_args()
    app = create_app(Zakya(every=args.every, fault_rate=args.fault_rate, state=args.state))
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

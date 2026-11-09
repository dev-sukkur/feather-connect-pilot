"""A Zakya simulator, for tests and the demo: Zoho's OAuth2 refresh-token sign-in with tokens that expire, the
organisation id on every call, paged lists filtered by `last_modified_time`, each record on its own, the writes the
sinks make, and Zoho's two rate limits, per minute and per day, answered with 429 and `Retry-After`.

In a test, hand `ZakyaSim.transport()` to a connector (`source.transport = sim.transport()`) and nothing listens on a
port; `running(sim)` serves it on a free port instead, for tools that need a URL. `python -m
feather_connect.connectors.zakya.testing --port 29100 --state .data/zakya.json` runs it on its own, raising new
invoices as time passes and paying some, as the demo does.

    sim = ZakyaSim(per_minute=100, per_day=1000)
    sim.add(5)                       # five invoices, now
    sim.expire_tokens()              # the next call gets a 401, and a new token fixes it
    sim.fail_next(503, times=2)      # the next two calls fail
    sim.fail_after_applying()        # the next POST is carried out, and its answer lost
    sim.limit_tokens()               # the next token request is turned away for asking too often
"""

import argparse
import json
import random
import secrets
import socket
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qsl

import httpx

__all__ = ["ORGANIZATION_ID", "ZakyaSim", "running", "serve"]

IST = timezone(timedelta(hours=5, minutes=30))
STAMP = "%Y-%m-%dT%H:%M:%S%z"
ORGANIZATION_ID = "60001"
PREFIX = "/inventory/v1/"
MAX_PAGE = 200
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
# Each list: the record's id field, its name on its own, and the fields a list leaves out.
COLLECTIONS: Mapping[str, tuple[str, str, tuple[str, ...]]] = {
    "invoices": ("invoice_id", "invoice", ("line_items",)),
    "creditnotes": ("creditnote_id", "creditnote", ("line_items",)),
    "customerpayments": ("payment_id", "payment", ()),
    "items": ("item_id", "item", ()),
    "pricebooks": ("pricebook_id", "pricebook", ("pricebook_items",)),
    "inventoryadjustments": ("inventory_adjustment_id", "inventory_adjustment", ("line_items",)),
}
FILTERS = ("sku", "reference_number")  # list filters by a field's value
LIST_KEYS = {"inventoryadjustments": "inventory_adjustments"}  # where a list's records are, if not its name


def stamp(moment: datetime) -> str:
    return moment.astimezone(IST).strftime(STAMP)


def parse_stamp(text: str) -> datetime:
    # A `+` that a client left unescaped in the query arrives as a space.
    return datetime.strptime(text.replace(" ", "+"), STAMP)


def _sort_value(value: Any) -> tuple[int, Any]:
    """A value to sort by: a stamp as the moment it names, whatever its offset; anything else as text."""
    if isinstance(value, str):
        try:
            return (0, parse_stamp(value))
        except ValueError:
            pass
    return (1, "" if value is None else str(value))


def _answer(status: int, body: Mapping[str, Any], headers: Mapping[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, json=dict(body), headers=dict(headers or {}))


class ZakyaSim:
    """One simulated organisation: its records by list, the tokens it has issued, and its limits.

    `per_minute` and `per_day` cap the API calls (None: no cap); a call past either answers 429 with `Retry-After`,
    the seconds to the next minute or to midnight in `zone` (India's), or with none for the day's when
    `daily_retry_after` is False, as Zoho may answer: code 45 alone. `token_lifetime` is the seconds an access token lasts.
    `now` is the clock, real time by default. `every` raises new and changed invoices every that many seconds when a
    list is called (None: only when `add` or `change` is called), `fault_rate` is the share of calls that answer 429
    or 503 by chance, and `state` is a JSON file that keeps the invoices across restarts.
    """

    def __init__(
        self,
        *,
        organization_id: str = ORGANIZATION_ID,
        per_minute: int | None = None,
        per_day: int | None = None,
        token_lifetime: float = 3600.0,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        start: int = 0,
        every: float | None = None,
        fault_rate: float = 0.0,
        seed: int = 7,
        state: Path | None = None,
        zone: tzinfo = timezone(timedelta(hours=5, minutes=30)),
        daily_retry_after: bool = True,
    ) -> None:
        self.organization_id = organization_id
        self.zone = zone
        self.daily_retry_after = daily_retry_after
        self.per_minute = per_minute
        self.per_day = per_day
        self.token_lifetime = token_lifetime
        self.now = now
        self.every = every
        self.fault_rate = fault_rate
        self.data: dict[str, dict[str, dict[str, Any]]] = {name: {} for name in COLLECTIONS}
        self.tokens: dict[str, datetime] = {}
        self.token_requests = 0
        self.calls: list[str] = []  # "GET /inventory/v1/invoices", one per API call that got past sign-in
        self.refresh_tokens: set[str] | None = None  # None: any refresh token is good
        self.stamp_filter = True  # False: lists ignore `last_modified_time`, as some endpoints may
        self._random = random.Random(seed)
        self._lock = threading.RLock()
        self._faults: list[tuple[int, str | None]] = []
        self._lose: list[
            tuple[str, int | None]
        ] = []  # calls of a method whose answer to lose: a status, or None
        self._token_limits = 0
        self._minute: tuple[datetime, int] = (datetime.min.replace(tzinfo=UTC), 0)
        self._day: tuple[str, int] = ("", 0)
        self._state = state
        self._next = 1
        self._ids = 1
        self._sorted: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._last_tick = time.monotonic()
        if state is not None and state.is_file():
            kept = json.loads(state.read_text(encoding="utf-8"))
            self.data["invoices"] = kept["invoices"]
            self._next = kept["next"]
            self._random.seed(seed + self._next)
        elif start:
            self.add(start)

    # The records.

    @property
    def invoices(self) -> dict[str, dict[str, Any]]:
        return self.data["invoices"]

    @property
    def items(self) -> dict[str, dict[str, Any]]:
        return self.data["items"]

    def seed(self, collection: str, rows: list[dict[str, Any]]) -> None:
        """Make `rows` all that `collection` holds, as they are; a row without its id field is keyed by `id`."""
        key = COLLECTIONS[collection][0]
        with self._lock:
            self.data[collection] = {str(r.get(key, r.get("id"))): dict(r) for r in rows}
            self._sorted.clear()

    def _new_id(self) -> str:
        self._ids += 1
        return f"9{self._ids:012d}"

    def _save(self) -> None:
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
                lines: list[dict[str, Any]] = []
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
                today = self.now().astimezone(IST).date()
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
                    "last_modified_time": stamp(self.now()),
                    "line_items": lines,
                }
                made.append(invoice_id)
            self._sorted.clear()
            self._save()
        return made

    def change(self, invoice_id: str | None = None) -> str | None:
        """Mark an invoice paid (one still open, at random, when none is named), and stamp it now."""
        with self._lock:
            open_ones = [i for i, v in self.invoices.items() if v["status"] != "paid"]
            chosen = invoice_id or (self._random.choice(open_ones) if open_ones else None)
            if chosen is None:
                return None
            self.invoices[chosen].update(status="paid", balance=0.0, last_modified_time=stamp(self.now()))
            self._sorted.clear()
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

    # What goes wrong.

    def expire_tokens(self) -> None:
        """Make every access token issued so far expired, as an hour passing would."""
        with self._lock:
            self.tokens = dict.fromkeys(self.tokens, datetime.min.replace(tzinfo=UTC))

    def fail_next(self, status: int, times: int = 1, retry_after: str | None = None) -> None:
        """Make the next `times` API calls answer `status`, with `Retry-After` if given."""
        with self._lock:
            self._faults += [(status, retry_after)] * times

    def fail_after_applying(self, times: int = 1, status: int | None = None, method: str = "POST") -> None:
        """Carry out each of the next `times` API calls of `method`, then lose its answer: the connection drops (a
        read timeout) or, with `status`, the call answers that instead, as a gateway that timed out would."""
        with self._lock:
            self._lose += [(method, status)] * times

    def limit_tokens(self, times: int = 1) -> None:
        """Turn the next `times` token requests away for asking too often, as Zoho's accounts server does: HTTP 400,
        "Access Denied"."""
        with self._lock:
            self._token_limits += times

    def spend(self, calls: int) -> None:
        """Count `calls` against today's cap, as the organisation's other clients would have spent them."""
        with self._lock:
            day = self.now().astimezone(self.zone).date().isoformat()
            self._day = (day, (self._day[1] if self._day[0] == day else 0) + calls)

    def remove(self, collection: str, record_id: str) -> None:
        """Delete a record, as someone in Zakya would."""
        with self._lock:
            self.data[collection].pop(record_id, None)
            self._sorted.clear()

    def _fault(self) -> httpx.Response | None:
        if self._faults:
            status, retry_after = self._faults.pop(0)
            headers = {"Retry-After": retry_after} if retry_after is not None else {}
            return _answer(
                status, {"code": 44 if status == 429 else status, "message": "failing on purpose"}, headers
            )
        roll = self._random.random()
        if roll < self.fault_rate / 2:
            return _answer(429, {"code": 44, "message": "Too many requests"}, {"Retry-After": "1"})
        if roll < self.fault_rate:
            return _answer(503, {"code": 503, "message": "Service unavailable"})
        return None

    def _limited(self) -> httpx.Response | None:
        """Count the call against the per-minute and per-day caps, or answer 429 if it is past one."""
        now = self.now()
        minute = now.replace(second=0, microsecond=0)
        day = now.astimezone(self.zone).date().isoformat()
        used_minute = self._minute[1] if self._minute[0] == minute else 0
        used_day = self._day[1] if self._day[0] == day else 0
        if self.per_day is not None and used_day >= self.per_day:
            midnight = datetime.combine(
                now.astimezone(self.zone).date() + timedelta(days=1), datetime.min.time(), self.zone
            )
            wait = max(1, int((midnight - now).total_seconds()))
            message = "You have reached the maximum number of API calls allowed for a day"
            headers = {"Retry-After": str(wait)} if self.daily_retry_after else {}
            return _answer(429, {"code": 45, "message": message}, headers)
        if self.per_minute is not None and used_minute >= self.per_minute:
            wait = max(1, int((minute + timedelta(minutes=1) - now).total_seconds()))
            return _answer(429, {"code": 44, "message": "Too many requests"}, {"Retry-After": str(wait)})
        self._minute, self._day = (minute, used_minute + 1), (day, used_day + 1)
        return None

    # The API.

    def transport(self) -> httpx.MockTransport:
        """A transport that sends a client's requests here, with no network."""
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Answer a request, with a `Date` of the simulator's clock, as a server's answer has."""
        with self._lock:
            response = self._handle(request)
            response.headers["Date"] = format_datetime(self.now().astimezone(UTC), usegmt=True)
            return response

    def _handle(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            path = request.url.path
            if path == "/oauth/v2/token" and request.method == "POST":
                return self._token(request)
            if not path.startswith(PREFIX):
                return _answer(404, {"code": 5, "message": "Invalid URL Passed"})
            if (refused := self._refused(request)) is not None:
                return refused
            if (limited := self._limited()) is not None:
                return limited
            if (fault := self._fault()) is not None:
                return fault
            self.calls.append(f"{request.method} {path}")
            parts = path[len(PREFIX) :].strip("/").split("/")
            collection = parts[0]
            if collection not in COLLECTIONS:
                return _answer(404, {"code": 5, "message": "Invalid URL Passed"})
            if request.method == "GET" and len(parts) == 1:
                self.tick()
                return self._lost(self._list(collection, dict(request.url.params)), request)
            if request.method == "GET" and len(parts) == 2:
                return self._lost(self._one(collection, parts[1]), request)
            body = cast(dict[str, Any], json.loads(request.content or b"{}"))
            if request.method == "POST" and len(parts) == 1:
                return self._lost(self._create(collection, body), request)
            if request.method == "PUT" and len(parts) == 2:
                return self._lost(self._update(collection, parts[1], body), request)
            return _answer(405, {"code": 5, "message": "Invalid URL Passed"})

    def _lost(self, response: httpx.Response, request: httpx.Request) -> httpx.Response:
        """The answer to a call carried out already, or what came back instead when it is to be lost."""
        if not self._lose or self._lose[0][0] != request.method:
            return response
        _, status = self._lose.pop(0)
        if status is None:
            raise httpx.ReadTimeout("the answer was lost", request=request)
        return _answer(status, {"code": status, "message": "the gateway timed out"})

    def _token(self, request: httpx.Request) -> httpx.Response:
        form = dict(parse_qsl(request.content.decode()))
        self.token_requests += 1
        if self._token_limits:
            self._token_limits -= 1
            description = "You have made too many requests continuously. Please try again after some time."
            return _answer(
                400, {"error_description": description, "error": "Access Denied", "status": "failure"}
            )
        if form.get("grant_type") != "refresh_token" or not all(
            form.get(k) for k in ("client_id", "client_secret", "refresh_token")
        ):
            return _answer(400, {"error": "invalid_client"})
        if self.refresh_tokens is not None and form["refresh_token"] not in self.refresh_tokens:
            return _answer(400, {"error": "invalid_code"})
        issued = secrets.token_hex(16)
        self.tokens[issued] = self.now() + timedelta(seconds=self.token_lifetime)
        return _answer(
            200,
            {
                "access_token": issued,
                "api_domain": "https://www.zohoapis.in",
                "token_type": "Bearer",
                "expires_in": int(self.token_lifetime),
            },
        )

    def _refused(self, request: httpx.Request) -> httpx.Response | None:
        scheme, _, token = request.headers.get("authorization", "").partition(" ")
        expires = self.tokens.get(token)
        if scheme != "Zoho-oauthtoken" or expires is None or expires <= self.now():
            return _answer(401, {"code": 57, "message": "You are not authorized to perform this operation"})
        if request.url.params.get("organization_id") != self.organization_id:
            return _answer(400, {"code": 6041, "message": "Invalid value passed for organization_id"})
        return None

    def _sorted_rows(self, collection: str, column: str) -> list[dict[str, Any]]:
        found = self._sorted.get((collection, column))
        if found is None:
            key = COLLECTIONS[collection][0]
            rows = list(self.data[collection].values())
            if column:
                found = sorted(
                    rows, key=lambda r: (_sort_value(r.get(column)), str(r.get(key, r.get("id", ""))))
                )
            else:
                found = rows
            self._sorted[(collection, column)] = found
        return found

    def _list(self, collection: str, params: Mapping[str, str]) -> httpx.Response:
        _, _, hidden = COLLECTIONS[collection]
        column = params.get("sort_column", "")
        rows = self._sorted_rows(collection, column)
        since_text = params.get("last_modified_time")
        if since_text and self.stamp_filter:
            since = parse_stamp(since_text)
            rows = [
                r for r in rows if "last_modified_time" in r and parse_stamp(r["last_modified_time"]) >= since
            ]
        for name in FILTERS:
            if name in params:
                rows = [r for r in rows if str(r.get(name)) == params[name]]
        page = int(params.get("page", "1"))
        per_page = min(int(params.get("per_page", str(MAX_PAGE))), MAX_PAGE)
        chunk = [
            {k: v for k, v in r.items() if k not in hidden}
            for r in rows[(page - 1) * per_page : page * per_page]
        ]
        context = {"page": page, "per_page": per_page, "has_more_page": page * per_page < len(rows)}
        listed = LIST_KEYS.get(collection, collection)
        return _answer(200, {"code": 0, "message": "success", listed: chunk, "page_context": context})

    def _one(self, collection: str, record_id: str) -> httpx.Response:
        found = self.data[collection].get(record_id)
        if found is None:
            return _answer(404, {"code": 1002, "message": f"{COLLECTIONS[collection][1]} does not exist"})
        return _answer(200, {"code": 0, "message": "success", COLLECTIONS[collection][1]: found})

    def _create(self, collection: str, body: dict[str, Any]) -> httpx.Response:
        key, singular, _ = COLLECTIONS[collection]
        if collection == "items":
            if not body.get("name") and not body.get("sku"):
                return _answer(400, {"code": 4, "message": "Item name is required"})
            if body.get("sku") and any(i.get("sku") == body["sku"] for i in self.items.values()):
                return _answer(400, {"code": 1001, "message": f"An item with SKU {body['sku']} exists"})
        elif collection == "inventoryadjustments":
            for line in cast(list[dict[str, Any]], body.get("line_items") or []):
                item = self.items.get(str(line.get("item_id")))
                if item is None:
                    return _answer(400, {"code": 1002, "message": "Item does not exist"})
                item["stock_on_hand"] = item.get("stock_on_hand", 0) + line.get("quantity_adjusted", 0)
        elif collection not in ("creditnotes", "customerpayments", "invoices"):
            return _answer(405, {"code": 5, "message": "Invalid URL Passed"})
        made = {**body, key: self._new_id(), "last_modified_time": stamp(self.now())}
        self.data[collection][made[key]] = made
        self._sorted.clear()
        return _answer(201, {"code": 0, "message": "created", singular: made})

    def _update(self, collection: str, record_id: str, body: dict[str, Any]) -> httpx.Response:
        _, singular, _ = COLLECTIONS[collection]
        found = self.data[collection].get(record_id)
        if found is None:
            return _answer(404, {"code": 1002, "message": f"{singular} does not exist"})
        if collection == "pricebooks":
            items = cast(list[dict[str, Any]], body.get("pricebook_items") or [])
            changed = {**found, "pricebook_items": [dict(i) for i in items]}
        elif collection == "items":
            changed = {**found, **body}
        else:
            return _answer(405, {"code": 5, "message": "Invalid URL Passed"})
        if changed != found:
            changed["last_modified_time"] = stamp(self.now())
            self.data[collection][record_id] = changed
            self._sorted.clear()
        return _answer(200, {"code": 0, "message": "updated", singular: self.data[collection][record_id]})


# Serving it over HTTP.


def serve(sim: ZakyaSim, host: str = "127.0.0.1", port: int = 29100) -> ThreadingHTTPServer:
    """An HTTP server in front of `sim`, not yet serving: call its `serve_forever`."""

    class Handler(BaseHTTPRequestHandler):
        def _any(self) -> None:
            length = int(self.headers.get("content-length") or 0)
            request = httpx.Request(
                self.command,
                f"http://{host}:{port}{self.path}",
                headers=dict(self.headers.items()),
                content=self.rfile.read(length) if length else b"",
            )
            try:
                response = sim.handle(request)
            except httpx.TransportError:  # an answer to lose: the connection drops with none
                self.close_connection = True
                return
            self.send_response(response.status_code)
            for name, value in response.headers.items():
                if name.lower() not in ("content-length", "transfer-encoding", "connection"):
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = do_POST = do_PUT = _any

        def log_message(self, format: str, *args: Any) -> None:
            pass

    return ThreadingHTTPServer((host, port), Handler)


@contextmanager
def running(sim: ZakyaSim) -> Iterator[str]:
    """Serve `sim` on a free port of 127.0.0.1 while the block runs, and yield its base URL."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = serve(sim, port=port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(10)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="A Zakya simulator: invoices that come and change, over HTTP."
    )
    parser.add_argument("--port", type=int, default=29100)
    parser.add_argument("--every", type=float, default=20.0, help="seconds between new or changed invoices")
    parser.add_argument(
        "--fault-rate", type=float, default=0.04, help="share of calls that answer 429 or 503"
    )
    parser.add_argument("--start", type=int, default=5, help="invoices to begin with")
    parser.add_argument("--state", type=Path, help="a JSON file that keeps the invoices across restarts")
    args = parser.parse_args(argv)
    sim = ZakyaSim(start=args.start, every=args.every, fault_rate=args.fault_rate, state=args.state)
    serve(sim, port=args.port).serve_forever()


if __name__ == "__main__":
    main()

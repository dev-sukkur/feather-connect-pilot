"""The demo's stand-in for SAP through CPI: the protocol of the `feather-sap-cpi` package's simulator (a client
credentials token, a CSRF token, an `Idempotency-Key` on each post, a reversal path), with an ERP behind it that keeps
what it posts in SQLite and shows it.

The iFlows:

- `GET /http/materials`: the material master of `master.json`, as an OData pass-through answers (`d.results`); each
  material has its ERP number, its EAN-13 and its POS code, so Zakya's lines can be matched by either barcode.
- `POST /http/invoices`: a B2B invoice, one billing document (F2) per Zakya invoice, keyed by `invoice_id`.
- `POST /http/creditmemos`: a B2B return, a credit memo (G2) keyed by `creditnote_id`, against the billing document
  of `ReferenceInvoice`. It answers 424 (Failed Dependency) while that invoice isn't in the ERP, for the sender to
  try again later, and turns it down (422) when it gives back more of a material than the invoice sold and earlier
  credit memos took back.
- `POST /http/salessummary`: B2C sales or returns of one material on one day, as the integration summed them,
  keyed by `document_id`. Returns carry a negative quantity and amount.
- `POST /http/<iflow>/reverse`: takes a posted document back, once (Feather's compensation).

A document posted again under the same key, with another idempotency key, answers 409 with the document it has: the
ERP doesn't post a business document twice. A material it doesn't know is turned down (422).

Auto mode posts what arrives at once. Manual mode parks it: the page's **Post** or **Reject** decides, as an
accountant checking each posting would. The page at `/` shows two views of the posted documents: by invoice (each
document and its items) and by material per day (sold, returned and net quantity and amount, accumulated over every
posting of the day, B2B and B2C, with the running total to date), and a form to enter a document by hand.

    python sims/sap.py --port 29102 --db .data/sap.sqlite            # auto mode
    python sims/sap.py --port 29102 --db .data/sap.sqlite --manual   # manual mode
"""

import argparse
import json
import random
import secrets
import sqlite3
import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

__all__ = ["IFLOWS", "MATERIALS", "ErpSim", "serve"]

MASTER: dict[str, Any] = json.loads(Path(__file__).with_name("master.json").read_text(encoding="utf-8"))
MATERIALS: list[dict[str, Any]] = MASTER["materials"]
IST = timezone(timedelta(hours=5, minutes=30))
# Each iFlow that posts: the field that keys its documents, and what kind of document it makes.
IFLOWS: Mapping[str, tuple[str, str]] = {
    "invoices": ("invoice_id", "invoice"),
    "creditmemos": ("creditnote_id", "credit memo"),
    "salessummary": ("document_id", "B2C summary"),
}

_SCHEMA = """
create table if not exists documents (
    doc_no text primary key,
    iflow text not null,
    business_key text not null,
    kind text not null,
    channel text not null,
    posting_date text not null,
    customer text not null,
    reference text,
    total real not null,
    status text not null,
    origin text not null,
    body text not null,
    idempotency_key text,
    received_at text not null,
    decided_at text
);
create index if not exists documents_key on documents (iflow, business_key);
create table if not exists items (
    doc_no text not null,
    line_no integer not null,
    material text not null,
    description text not null,
    ean text not null,
    quantity real not null,
    amount real not null,
    primary key (doc_no, line_no)
);
create table if not exists answers (idempotency_key text primary key, status integer not null, body text not null);
create table if not exists settings (name text primary key, value text not null);
"""


class Turned(Exception):
    """A posting the ERP turns down, with the status to answer."""

    def __init__(self, status: int, message: str, body: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


def _material(code: Any) -> dict[str, Any]:
    for material in MATERIALS:
        if str(code) in (material["material"], material["ean"], material["pos_code"]):
            return material
    raise Turned(422, f"material {code!r} is not in the material master")


def _number(value: Any, what: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise Turned(422, f"{what} is not a number: {value!r}") from None


class ErpSim:
    """The ERP: documents and their items in the SQLite file `db`, the tokens it gave out, and its mode."""

    def __init__(self, db: Path, *, mode: str = "auto", fault_rate: float = 0.0, seed: int = 13) -> None:
        db.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(db, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.executescript(_SCHEMA)
        self._lock = threading.RLock()
        self._tokens: set[str] = set()
        self._csrf: set[str] = set()
        self._random = random.Random(seed)
        self.fault_rate = fault_rate
        self.posts = 0  # postings taken (not repeats), parked ones too
        if self.setting("mode") is None:
            self.set_mode(mode)

    # Settings and modes.

    def setting(self, name: str) -> str | None:
        with self._lock:
            found = self._db.execute("select value from settings where name = ?", (name,)).fetchone()
            return None if found is None else str(found["value"])

    @property
    def mode(self) -> str:
        return self.setting("mode") or "auto"

    def set_mode(self, mode: str) -> None:
        if mode not in ("auto", "manual"):
            raise Turned(400, "mode is auto or manual")
        with self._lock, self._db:
            self._db.execute(
                "insert into settings values ('mode', ?) on conflict (name) do update set value = excluded.value",
                (mode,),
            )

    # The protocol.

    def answer(
        self, method: str, path: str, headers: Mapping[str, str], content: bytes
    ) -> tuple[int, dict[str, str], Any]:
        """Answer one request: the status, the headers and a JSON body."""
        lower = {k.lower(): v for k, v in headers.items()}
        with self._lock:
            try:
                if path.startswith("/sim/"):
                    return 200, {}, self._counter(method, path.removeprefix("/sim/"), content)
                if self._random.random() < self.fault_rate:
                    return 503, {}, {"error": "busy, try again"}
                if path == "/oauth/token":
                    token = secrets.token_hex(8)
                    self._tokens.add(token)
                    return 200, {}, {"access_token": token, "token_type": "bearer", "expires_in": 3600}
                auth = lower.get("authorization", "")
                if not auth.lower().startswith("bearer ") or auth.split(" ", 1)[1] not in self._tokens:
                    return 401, {}, {"error": "unauthorized"}
                if not path.startswith("/http/"):
                    return 404, {}, {"error": "no such iFlow"}
                iflow = path.removeprefix("/http/")
                if method == "GET":
                    if lower.get("x-csrf-token", "").lower() == "fetch":
                        token = secrets.token_hex(8)
                        self._csrf.add(token)
                        return 200, {"X-CSRF-Token": token, "Set-Cookie": "JSESSIONID=sim; Path=/"}, {}
                    if iflow == "materials":
                        return 200, {}, {"d": {"results": MATERIALS}}
                    return 200, {}, {"d": {"results": self.documents(iflow=iflow)}}
                if method != "POST":
                    return 405, {}, {"error": "method"}
                if lower.get("x-csrf-token") not in self._csrf:
                    return 403, {"X-CSRF-Token": "Required"}, {"error": "CSRF token validation failed"}
                return self._post(iflow, lower.get("idempotency-key"), content)
            except Turned as turned:
                return turned.status, {}, turned.body or {"error": str(turned)}

    def _post(self, iflow: str, idem: str | None, content: bytes) -> tuple[int, dict[str, str], Any]:
        if idem:
            seen = self._db.execute(
                "select status, body from answers where idempotency_key = ?", (idem,)
            ).fetchone()
            if seen is not None:
                return int(seen["status"]), {}, json.loads(seen["body"])
        body = json.loads(content or b"{}")
        if not isinstance(body, dict):
            raise Turned(400, "the body is not a JSON object")
        reverse = iflow.endswith("/reverse")
        name = iflow.removesuffix("/reverse")
        if name not in IFLOWS:
            raise Turned(404, f"no iFlow {name!r}")
        key_field = IFLOWS[name][0]
        if key_field not in body:
            raise Turned(400, f"the body has no {key_field}")
        with self._db:
            status, answer = (self._reverse if reverse else self._book)(
                name, str(body[key_field]), body, idem
            )
            if idem:
                self._db.execute("insert into answers values (?, ?, ?)", (idem, status, json.dumps(answer)))
        return status, {}, answer

    def _book(
        self, iflow: str, key: str, body: dict[str, Any], idem: str | None
    ) -> tuple[int, dict[str, Any]]:
        known = self._db.execute(
            "select doc_no, status from documents where iflow = ? and business_key = ? "
            "and status in ('posted', 'parked') order by received_at desc limit 1",
            (iflow, key),
        ).fetchone()
        if known is not None:  # a business document is posted once: the one there is the answer
            return 409, {
                "d": {"BillingDocument": known["doc_no"], "Status": known["status"], "Duplicate": True}
            }
        items = self._items(iflow, body)
        if iflow == "creditmemos":
            self._check_return(body, items)
        doc_no = self._next_number()
        status = "parked" if self.mode == "manual" else "posted"
        self._store(doc_no, iflow, key, body, items, status=status, origin="integration", idem=idem)
        self.posts += 1
        return 201, {"d": {"BillingDocument": doc_no, "Status": status}}

    def _items(self, iflow: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        """The document's items, each signed: a sale positive, a return negative."""
        sign = -1.0 if iflow == "creditmemos" else 1.0
        if iflow == "salessummary":
            raw = [
                {
                    "Material": body.get("Material"),
                    "Quantity": body.get("Quantity"),
                    "NetAmount": body.get("NetAmount"),
                }
            ]
            sign = 1.0  # a summary carries its own sign
        else:
            raw = body.get("Items") or []
        if not raw:
            raise Turned(422, "the document has no items")
        items: list[dict[str, Any]] = []
        for i, item in enumerate(raw, start=1):
            material = _material(item.get("Material") or item.get("EAN"))
            items.append(
                {
                    "line_no": i,
                    "material": material["material"],
                    "description": material["description"],
                    "ean": material["ean"],
                    "quantity": sign * _number(item.get("Quantity"), "Quantity"),
                    "amount": round(sign * _number(item.get("NetAmount"), "NetAmount"), 2),
                }
            )
        return items

    def _check_return(self, body: dict[str, Any], items: list[dict[str, Any]]) -> None:
        """A credit memo must name an invoice the ERP has, and give back no more than it sold, less earlier returns."""
        reference = str(body.get("ReferenceInvoice") or "")
        invoice = self._db.execute(
            "select doc_no from documents where iflow = 'invoices' and business_key = ? "
            "and status in ('posted', 'parked')",
            (reference,),
        ).fetchone()
        if invoice is None:
            raise Turned(
                424, f"invoice {reference or '(none)'} is not in the ERP yet: post it before its credit memo"
            )
        sold = {
            r["material"]: r["quantity"]
            for r in self._db.execute(
                "select i.material, sum(i.quantity) as quantity from items i join documents d using (doc_no) "
                "where d.iflow in ('invoices', 'creditmemos') and d.status in ('posted', 'parked') "
                "and (d.doc_no = ? or (d.iflow = 'creditmemos' and d.reference = ?)) group by i.material",
                (invoice["doc_no"], reference),
            )
        }
        for item in items:
            left = sold.get(item["material"], 0.0)
            if -item["quantity"] > left + 1e-9:
                raise Turned(
                    422,
                    f"{item['description']}: {-item['quantity']:g} back, but invoice {reference} has only "
                    f"{left:g} left to credit",
                )

    def _reverse(
        self, iflow: str, key: str, body: dict[str, Any], idem: str | None
    ) -> tuple[int, dict[str, Any]]:
        found = self._db.execute(
            "select doc_no from documents where iflow = ? and business_key = ? and status in ('posted', 'parked')",
            (iflow, key),
        ).fetchone()
        if found is None:
            raise Turned(404 if not idem else 422, f"no document {key} to reverse")
        self._db.execute(
            "update documents set status = 'reversed', decided_at = ? where doc_no = ?",
            (_now(), found["doc_no"]),
        )
        return 200, {"d": {"Reversed": key, "BillingDocument": found["doc_no"]}}

    def _next_number(self) -> str:
        found = self._db.execute("select max(cast(doc_no as integer)) from documents").fetchone()[0]
        return str(max(int(found or 0), 90_000_000) + 1)

    def _store(
        self,
        doc_no: str,
        iflow: str,
        key: str,
        body: dict[str, Any],
        items: list[dict[str, Any]],
        *,
        status: str,
        origin: str,
        idem: str | None,
    ) -> None:
        channel = "B2C" if iflow == "salessummary" else str(body.get("Channel") or "B2B")
        kind = IFLOWS[iflow][1] if origin == "integration" else str(body.get("Kind") or "manual")
        if iflow == "salessummary" and _number(body.get("Quantity"), "Quantity") < 0:
            kind = "B2C returns summary"
        today = datetime.now(IST).date().isoformat()
        self._db.execute(
            "insert into documents values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null)",
            (
                doc_no,
                iflow,
                key,
                kind,
                channel,
                str(body.get("BillingDocumentDate") or body.get("PostingDate") or today),
                str(
                    body.get("CustomerName")
                    or body.get("SoldToParty")
                    or ("B2C customers" if channel == "B2C" else "")
                ),
                body.get("ReferenceInvoice") or body.get("ReferenceInvoiceNumber"),
                round(sum(i["amount"] for i in items), 2),
                status,
                origin,
                json.dumps(body),
                idem,
                _now(),
            ),
        )
        self._db.executemany(
            "insert into items values (?, ?, ?, ?, ?, ?, ?)",
            [
                (doc_no, i["line_no"], i["material"], i["description"], i["ean"], i["quantity"], i["amount"])
                for i in items
            ],
        )

    # What the page and the tests read.

    def documents(self, *, iflow: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
        """The documents, newest first, each with its items."""
        where, args = [], []
        if iflow is not None:
            where.append("iflow = ?")
            args.append(iflow)
        if status is not None:
            where.append("status = ?")
            args.append(status)
        sql = "select * from documents" + (f" where {' and '.join(where)}" if where else "")
        with self._lock:
            rows = [dict(r) for r in self._db.execute(sql + " order by cast(doc_no as integer) desc", args)]
            for row in rows:
                row["body"] = json.loads(row["body"])
                row["items"] = [
                    dict(i)
                    for i in self._db.execute(
                        "select * from items where doc_no = ? order by line_no", (row["doc_no"],)
                    )
                ]
            return rows

    def by_material(self) -> list[dict[str, Any]]:
        """Per day and material, over the posted documents: sold, returned and net quantity, net amount, B2B and
        B2C quantity, how many documents, and the net quantity and amount to date (the running total over days)."""
        sql = """
            select *, sum(net_qty) over w as qty_to_date, round(sum(net_amount) over w, 2) as amount_to_date
            from (
                select d.posting_date, i.material, i.description,
                       sum(case when i.quantity > 0 then i.quantity else 0 end) as sold_qty,
                       sum(case when i.quantity < 0 then -i.quantity else 0 end) as returned_qty,
                       sum(i.quantity) as net_qty,
                       round(sum(i.amount), 2) as net_amount,
                       sum(case when d.channel = 'B2B' then i.quantity else 0 end) as b2b_qty,
                       sum(case when d.channel = 'B2C' then i.quantity else 0 end) as b2c_qty,
                       count(distinct d.doc_no) as documents
                from documents d join items i using (doc_no)
                where d.status = 'posted'
                group by d.posting_date, i.material, i.description
            )
            window w as (partition by material order by posting_date rows unbounded preceding)
            order by posting_date desc, material
        """
        with self._lock:
            return [dict(r) for r in self._db.execute(sql)]

    def decide(self, doc_no: str, decision: str) -> None:
        """Post or reject a parked document, as an accountant would in manual mode."""
        if decision not in ("post", "reject"):
            raise Turned(400, "decide post or reject")
        with self._lock, self._db:
            changed = self._db.execute(
                "update documents set status = ?, decided_at = ? where doc_no = ? and status = 'parked'",
                ("posted" if decision == "post" else "rejected", _now(), doc_no),
            ).rowcount
            if not changed:
                raise Turned(400, f"document {doc_no} is not parked")

    def enter(self, body: dict[str, Any]) -> str:
        """A document entered by hand in the ERP: `kind` sale or return, `channel`, `customer`, `posting_date`, and
        `lines` of `{"code": material, EAN or POS code, "quantity": n, "amount": net}` (list price when left out)."""
        kind = str(body.get("kind") or "sale")
        if kind not in ("sale", "return"):
            raise Turned(400, "kind is sale or return")
        sign = -1.0 if kind == "return" else 1.0
        lines = body.get("lines") or []
        if not lines:
            raise Turned(400, "enter at least one line")
        items: list[dict[str, Any]] = []
        for i, line in enumerate(lines, start=1):
            material = _material(line.get("code"))
            quantity = _number(line.get("quantity"), "quantity")
            if quantity <= 0:
                raise Turned(400, f"line {i}: the quantity must be above 0")
            amount = line.get("amount")
            net = _number(amount, "amount") if amount not in (None, "") else quantity * material["price"]
            items.append(
                {
                    "line_no": i,
                    "material": material["material"],
                    "description": material["description"],
                    "ean": material["ean"],
                    "quantity": sign * quantity,
                    "amount": round(sign * net, 2),
                }
            )
        with self._lock, self._db:
            doc_no = self._next_number()
            channel = str(body.get("channel") or "B2B")
            record = {
                "Kind": f"manual {kind}",
                "Channel": channel,
                "CustomerName": body.get("customer")
                or ("B2C customers" if channel == "B2C" else "walk-in account"),
                "PostingDate": body.get("posting_date") or datetime.now(IST).date().isoformat(),
            }
            self._store(doc_no, "manual", doc_no, record, items, status="posted", origin="manual", idem=None)
        return doc_no

    def _counter(self, method: str, action: str, content: bytes) -> Any:
        if method == "GET" and action == "state":
            return {
                "mode": self.mode,
                "documents": self.documents()[:200],
                "by_material": self.by_material(),
                "materials": MATERIALS,
            }
        if method != "POST":
            raise Turned(405, "POST it")
        body = json.loads(content or b"{}")
        if action == "mode":
            self.set_mode(str(body.get("mode")))
            return {"mode": self.mode}
        if action == "decide":
            self.decide(str(body.get("doc_no")), str(body.get("decision")))
            return {"doc_no": body.get("doc_no")}
        if action == "documents":
            return {"doc_no": self.enter(body)}
        raise Turned(404, f"no action {action!r}")

    def close(self) -> None:
        self._db.close()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


PAGE = Path(__file__).with_name("sap.html").read_bytes()


def serve(sim: ErpSim, port: int) -> ThreadingHTTPServer:
    """Run the simulator as an HTTP server on 127.0.0.1, on a daemon thread."""

    class Handler(BaseHTTPRequestHandler):
        def _reply(self) -> None:
            path = self.path.split("?", 1)[0]
            if self.command == "GET" and path == "/":
                self._send(200, {"Content-Type": "text/html; charset=utf-8"}, PAGE)
                return
            length = int(self.headers.get("Content-Length") or 0)
            status, headers, body = sim.answer(
                self.command, path, dict(self.headers), self.rfile.read(length)
            )
            self._send(status, {**headers, "Content-Type": "application/json"}, json.dumps(body).encode())

        def _send(self, status: int, headers: Mapping[str, str], data: bytes) -> None:
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = _reply

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, name="erp-sim", daemon=True).start()
    return server


def main() -> None:
    parser = argparse.ArgumentParser(
        description="The demo's SAP through CPI: postings, two views, auto or manual."
    )
    parser.add_argument("--port", type=int, default=29102)
    parser.add_argument("--db", type=Path, default=Path(".data/sap.sqlite"))
    parser.add_argument(
        "--manual", action="store_true", help="park each posting for Post or Reject on the page"
    )
    parser.add_argument("--fault-rate", type=float, default=0.02, help="share of requests answered 503")
    args = parser.parse_args()
    sim = ErpSim(args.db, fault_rate=args.fault_rate)
    sim.set_mode("manual" if args.manual else "auto")
    serve(sim, args.port)
    print(f"ERP (SAP through CPI) simulator on http://127.0.0.1:{args.port}/ ({sim.mode} mode)", flush=True)
    threading.Event().wait()


if __name__ == "__main__":
    main()

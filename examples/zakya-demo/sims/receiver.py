"""A small stand-in for an ERP that takes invoices, for the demo. Every request it takes is kept in SQLite, and the
page at `/` lists them, newest first.

A PUT or a POST to any path is stored with its method, path, headers (without credentials), body, idempotency key and
time, and answered `{"id": ...}`. A PUT is an upsert by its path (`/invoices/{invoice_id}`): the first one for a path
makes an ERP id, answered 201, and each later one is stored too but keeps that id, answered 200. A POST always makes a
new id. A request whose idempotency key was seen before is not stored again: it gets the same id, and the stored one
counts the repeat. A few requests answer 503 on purpose.

    python sims/receiver.py --port 29101 --db .data/receiver.sqlite
"""

import argparse
import html
import json
import random
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route

HIDDEN_HEADERS = frozenset({"authorization", "cookie", "proxy-authorization"})
KEY_HEADER = "idempotency-key"
SHOWN = 200

_SCHEMA = """create table if not exists requests (
    id integer primary key autoincrement,
    erp_id text not null,
    method text not null,
    path text not null,
    headers text not null,
    body text not null,
    idempotency_key text unique,
    received_at text not null,
    repeats integer not null default 0
)"""


class Receiver:
    """The stored requests, in the SQLite file at `db`. `fault_rate` is the share of requests answered 503."""

    def __init__(self, db: Path, *, fault_rate: float = 0.03, seed: int = 11) -> None:
        db.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(db, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute(_SCHEMA)
        self._lock = threading.Lock()
        self._random = random.Random(seed)
        self.fault_rate = fault_rate

    def take(self, method: str, path: str, headers: dict[str, str], body: str) -> tuple[str, bool]:
        """Store a request, and return the id it got and whether it was new."""
        key = headers.get(KEY_HEADER)
        kept = {k: v for k, v in headers.items() if k not in HIDDEN_HEADERS}
        with self._lock, self._db:
            if key is not None:
                seen = self._db.execute(
                    "select erp_id from requests where idempotency_key = ?", (key,)
                ).fetchone()
                if seen is not None:
                    self._db.execute(
                        "update requests set repeats = repeats + 1 where idempotency_key = ?", (key,)
                    )
                    return seen["erp_id"], False
            known = None
            if method == "PUT":
                known = self._db.execute(
                    "select erp_id from requests where method = 'PUT' and path = ? order by id limit 1",
                    (path,),
                ).fetchone()
            if known is not None:
                erp_id = known["erp_id"]
            else:
                number = self._db.execute("select count(distinct erp_id) + 1 from requests").fetchone()[0]
                erp_id = f"ERP-{number:06d}"
            self._db.execute(
                """insert into requests (erp_id, method, path, headers, body, idempotency_key, received_at)
                   values (?, ?, ?, ?, ?, ?, ?)""",
                (
                    erp_id,
                    method,
                    path,
                    json.dumps(kept),
                    body,
                    key,
                    datetime.now(UTC).isoformat(timespec="seconds"),
                ),
            )
            return erp_id, known is None

    def newest(self, limit: int = SHOWN) -> list[sqlite3.Row]:
        with self._lock:
            return self._db.execute("select * from requests order by id desc limit ?", (limit,)).fetchall()

    def count(self) -> tuple[int, int]:
        """How many requests are stored, and how many ERP records they made."""
        with self._lock:
            found = self._db.execute("select count(*), count(distinct erp_id) from requests").fetchone()
            return found[0], found[1]

    def fails_now(self) -> bool:
        with self._lock:
            return self._random.random() < self.fault_rate

    def close(self) -> None:
        self._db.close()


def _summary(body: str) -> dict[str, Any]:
    try:
        parsed = json.loads(body)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _page(receiver: Receiver) -> str:
    rows = []
    for row in receiver.newest():
        sent = _summary(row["body"])
        customer = sent.get("customer") if isinstance(sent.get("customer"), dict) else {}
        cells = [
            row["received_at"].replace("T", " ").removesuffix("+00:00") + " UTC",
            row["erp_id"],
            f"{row['method']} {row['path']}",
            str(sent.get("number") or sent.get("external_id") or ""),
            str(customer.get("name") or ""),
            str(sent.get("status") or ""),
            f"{sent.get('currency') or ''} {sent.get('total') if sent.get('total') is not None else ''}".strip(),
            (row["idempotency_key"] or "")[:12],
            str(row["repeats"]) if row["repeats"] else "",
        ]
        rows.append("<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in cells) + "</tr>")
    requests, records = receiver.count()
    body = "\n".join(rows) or '<tr><td colspan="9" class="empty">No requests yet.</td></tr>'
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="10"><title>ERP receiver</title>
<style>
  body {{ font: 14px/1.5 system-ui, sans-serif; margin: 24px; color: #1f2328; background: #fff; }}
  h1 {{ font-size: 20px; margin: 0 0 4px; }}
  p {{ margin: 0 0 16px; color: #59636e; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid #d1d9e0; white-space: nowrap; }}
  th {{ font-weight: 600; background: #f6f8fa; }}
  td:nth-child(8) {{ font-family: ui-monospace, monospace; color: #59636e; }}
  .empty {{ color: #59636e; text-align: center; padding: 24px; }}
</style></head>
<body>
<h1>ERP receiver</h1>
<p>{requests} requests for {records} ERP invoices, newest first. A PUT for an invoice the ERP has updates it under
the same ERP id. A repeat with the same idempotency key is not stored again.</p>
<table>
<thead><tr><th>Received</th><th>ERP id</th><th>Request</th><th>Invoice</th><th>Customer</th><th>Status</th>
<th>Total</th><th>Idempotency key</th><th>Repeats</th></tr></thead>
<tbody>
{body}
</tbody>
</table>
</body></html>"""


def create_app(receiver: Receiver) -> Starlette:
    async def take(request: Request) -> JSONResponse:
        if receiver.fails_now():
            return JSONResponse({"error": "busy, try again"}, status_code=503)
        body = (await request.body()).decode("utf-8", "replace")
        headers = {k.lower(): v for k, v in request.headers.items()}
        erp_id, new = receiver.take(request.method, request.url.path, headers, body)
        return JSONResponse({"id": erp_id}, status_code=201 if new else 200)

    async def page(request: Request) -> HTMLResponse:
        return HTMLResponse(_page(receiver))

    return Starlette(routes=[Route("/", page), Route("/{path:path}", take, methods=["POST", "PUT"])])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--port", type=int, default=29101)
    parser.add_argument("--db", type=Path, default=Path(".data/receiver.sqlite"))
    parser.add_argument("--fault-rate", type=float, default=0.03, help="share of requests answered 503")
    args = parser.parse_args()
    receiver = Receiver(args.db, fault_rate=args.fault_rate)
    try:
        uvicorn.run(create_app(receiver), host="127.0.0.1", port=args.port, log_level="warning")
    finally:
        receiver.close()


if __name__ == "__main__":
    main()

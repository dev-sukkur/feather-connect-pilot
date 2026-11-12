"""The demo's targets on this machine: it makes the warehouse's table, then stands in for the ERP's HTTP API.

On start, when `FEATHER_DATABASE_URL` is set (`feather dev` sets it to its own Postgres), it runs `sims/demo.sql` there,
so that `postgres.table` finds `demo.orders`. Then it serves:

- `PUT /orders/{order_id}`: keeps the order's newest body, answered `{"order_id": ...}`. A request whose
  idempotency key was seen before is answered the same and not counted again. Each order taken is also appended
  to `<data>/erp.jsonl`, which is read back on a restart.
- `GET /orders`: the orders taken, as `{order_id: body}`. `GET /` lists them as a page.
- `POST /_targets/down` and `POST /_targets/up`: while down, every PUT is answered 503, to show a destination that
  is down holding up only its own route.

    python sims/targets.py --port 29001 --data .data
"""

import argparse
import html
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

import psycopg
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

KEY_HEADER = "idempotency-key"
DDL = Path(__file__).with_name("demo.sql")


class Erp:
    """The orders taken, kept in memory and in the file `log`."""

    def __init__(self, log: Path) -> None:
        log.parent.mkdir(parents=True, exist_ok=True)
        self._log = log
        self._lock = threading.Lock()
        self.orders: dict[str, Any] = {}
        self.keys: set[str] = set()
        self.down = False
        if log.is_file():
            for line in log.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    taken = json.loads(line)
                    self.orders[taken["order_id"]] = taken["body"]
                    if taken.get("key"):
                        self.keys.add(taken["key"])

    def take(self, order_id: str, body: Any, key: str | None) -> None:
        with self._lock:
            if key is not None and key in self.keys:
                return
            self.orders[order_id] = body
            if key is not None:
                self.keys.add(key)
            with self._log.open("a", encoding="utf-8") as out:
                out.write(json.dumps({"order_id": order_id, "body": body, "key": key}) + "\n")


def make_table(url: str, timeout: float = 60) -> None:
    """Run demo.sql on the database at `url`, waiting for it to answer."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            with psycopg.connect(url, autocommit=True, connect_timeout=5) as conn:
                conn.execute(DDL.read_text(encoding="utf-8"))  # pyright: ignore[reportCallIssue, reportArgumentType]
            return
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def make_app(erp: Erp) -> Starlette:
    async def put_order(request: Request) -> Response:
        if erp.down:
            return JSONResponse({"error": "the ERP is down"}, status_code=503)
        order_id = request.path_params["order_id"]
        try:
            body = json.loads(await request.body() or b"null")
        except ValueError:
            return JSONResponse({"error": "the body is not JSON"}, status_code=400)
        if isinstance(body, dict) and body.get(
            "reject"
        ):  # a record the ERP turns down, for a test to quarantine
            return JSONResponse({"error": "the ERP turned this order down"}, status_code=422)
        erp.take(order_id, body, request.headers.get(KEY_HEADER))
        return JSONResponse({"order_id": order_id})

    async def list_orders(request: Request) -> Response:
        return JSONResponse(erp.orders)

    async def page(request: Request) -> Response:
        rows = "".join(
            f"<tr><td>{html.escape(order_id)}</td><td><code>{html.escape(json.dumps(body))}</code></td></tr>"
            for order_id, body in sorted(erp.orders.items())
        )
        state = "down: every PUT is answered 503" if erp.down else "up"
        return HTMLResponse(
            "<!doctype html><html lang='en'><head><meta charset='utf-8'><title>Demo ERP</title></head><body>"
            f"<h1>Demo ERP</h1><p>{len(erp.orders)} orders taken. The ERP is {state}.</p>"
            f"<table><thead><tr><th>Order</th><th>Body</th></tr></thead><tbody>{rows}</tbody></table>"
            "</body></html>"
        )

    async def go_down(request: Request) -> Response:
        erp.down = True
        return JSONResponse({"down": True})

    async def come_up(request: Request) -> Response:
        erp.down = False
        return JSONResponse({"down": False})

    return Starlette(
        routes=[
            Route("/", page),
            Route("/orders", list_orders),
            Route("/orders/{order_id}", put_order, methods=["PUT"]),
            Route("/_targets/down", go_down, methods=["POST"]),
            Route("/_targets/up", come_up, methods=["POST"]),
        ]
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--port", type=int, default=29001)
    parser.add_argument("--data", type=Path, default=Path(".data"))
    parser.add_argument("--no-table", action="store_true", help="do not make the warehouse's table")
    args = parser.parse_args(argv)
    url = os.environ.get("FEATHER_DATABASE_URL")
    if url and not args.no_table:
        make_table(url)
        print("made demo.orders in the warehouse", flush=True)
    elif not args.no_table:
        print(
            "FEATHER_DATABASE_URL is not set: the warehouse's table is left alone",
            file=sys.stderr,
            flush=True,
        )
    erp = Erp(args.data / "erp.jsonl")
    uvicorn.run(make_app(erp), host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

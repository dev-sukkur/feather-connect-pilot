"""A simulated CPI tenant for tests and demos: a token endpoint, a CSRF fetch, iFlows that post and reverse documents
and that list them, deduplicating by `Idempotency-Key` as an iFlow's idempotent process call does.

    sim = CpiSim(key="invoice_id")
    sink.transport = sim.transport()

- `POST /oauth/token` gives a bearer token for client credentials;
- `GET /http/<flow>` with `X-CSRF-Token: Fetch` gives a token and a session cookie; without it, the documents as
  `{"d": {"results": [...]}}`, as an OData pass-through answers;
- `POST /http/<flow>` keeps the body under its `key` field, answering `{"d": {"BillingDocument": ...}}`; the same
  `Idempotency-Key` again answers the first answer and keeps nothing more;
- `POST /http/<flow>/reverse` takes the document back, once.

`fail_next(status, times)` makes the next requests fail, for the conformance kit's faults. `serve(port)` runs it as a
plain HTTP server, for `feather dev`.
"""

import json
import secrets
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx

__all__ = ["CpiSim", "serve"]


class CpiSim:
    def __init__(self, *, key: str = "id", csrf: bool = False) -> None:
        self.key = key
        self.csrf = csrf
        self.documents: dict[str, dict[str, dict[str, Any]]] = {}  # by iFlow, then by key
        self.reversed: dict[str, list[str]] = {}
        self.answers: dict[str, tuple[int, dict[str, Any]]] = {}  # by idempotency key
        self.posts = 0
        self._failing: tuple[int, int] | None = None
        self._tokens: set[str] = set()
        self._csrf: set[str] = set()
        self._numbers = 90_000_000
        self._lock = threading.Lock()

    def fail_next(self, status: int, times: int = 1) -> None:
        self._failing = (status, times)

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        status, headers, body = self.answer(
            request.method, request.url.path, dict(request.headers), request.content
        )
        return httpx.Response(status, headers=headers, json=body)

    def answer(
        self, method: str, path: str, headers: Mapping[str, str], content: bytes
    ) -> tuple[int, dict[str, str], Any]:
        lower = {k.lower(): v for k, v in headers.items()}
        with self._lock:
            if self._failing is not None:
                status, times = self._failing
                self._failing = (status, times - 1) if times > 1 else None
                return status, {}, {"error": "simulated"}
            if path == "/oauth/token":
                found = secrets.token_hex(8)
                self._tokens.add(found)
                return 200, {}, {"access_token": found, "token_type": "bearer", "expires_in": 3600}
            auth = lower.get("authorization", "")
            if not auth.lower().startswith("bearer ") or auth.split(" ", 1)[1] not in self._tokens:
                return 401, {}, {"error": "unauthorized"}
            if not path.startswith("/http/"):
                return 404, {}, {"error": "no such iFlow"}
            flow = path.removeprefix("/http/")
            if method == "GET":
                if lower.get("x-csrf-token", "").lower() == "fetch":
                    found = secrets.token_hex(8)
                    self._csrf.add(found)
                    return 200, {"X-CSRF-Token": found, "Set-Cookie": "JSESSIONID=sim; Path=/"}, {}
                return 200, {}, {"d": {"results": list(self.documents.get(flow, {}).values())}}
            if method != "POST":
                return 405, {}, {"error": "method"}
            if self.csrf and lower.get("x-csrf-token") not in self._csrf:
                return 403, {"X-CSRF-Token": "Required"}, {"error": "CSRF token validation failed"}
            idem = lower.get("idempotency-key")
            if idem and idem in self.answers:
                status, answer = self.answers[idem]
                return status, {}, answer
            body = json.loads(content or b"{}")
            if not isinstance(body, dict) or self.key not in body:
                return 400, {}, {"error": f"the body has no {self.key}"}
            document = str(body[self.key])
            if flow.endswith("/reverse"):
                kept = self.documents.get(flow.removesuffix("/reverse"), {})
                if kept.pop(document, None) is None:
                    return 404 if not idem else 422, {}, {"error": f"no document {document} to reverse"}
                self.reversed.setdefault(flow.removesuffix("/reverse"), []).append(document)
                result: tuple[int, dict[str, Any]] = (200, {"d": {"Reversed": document}})
            else:
                self.posts += 1
                self._numbers += 1
                self.documents.setdefault(flow, {})[document] = dict(body)
                result = (201, {"d": {"BillingDocument": str(self._numbers)}})
            if idem:
                self.answers[idem] = result
            return result[0], {}, result[1]


def serve(sim: CpiSim, port: int) -> ThreadingHTTPServer:
    """Run the simulator as an HTTP server on localhost, on a daemon thread."""

    class Handler(BaseHTTPRequestHandler):
        def _reply(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            status, headers, body = sim.answer(
                self.command, self.path.split("?", 1)[0], dict(self.headers), self.rfile.read(length)
            )
            data = json.dumps(body).encode()
            self.send_response(status)
            for name, value in {**headers, "Content-Type": "application/json"}.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = _reply

        def log_message(self, format: str, *args: Any) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, name="cpi-sim", daemon=True).start()
    return server

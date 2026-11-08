"""An HTTP destination that notes when each call arrived, to run as a process of its own.

    python http_echo.py PORT OUT.jsonl

`PUT /events/{id}` appends `{"id", "sent_at", "arrived_at"}` to OUT, `sent_at` being the body's, and answers 200.
"""

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

LOCK = threading.Lock()


def main(port: int, out: str) -> None:
    sink = open(out, "a", encoding="utf-8")  # noqa: SIM115 - open for the life of the process

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_PUT(self) -> None:
            arrived = time.time()
            body: Any = json.loads(self.rfile.read(int(self.headers.get("content-length") or 0)) or b"{}")
            line = json.dumps(
                {"id": self.path.rsplit("/", 1)[-1], "sent_at": body.get("sent_at"), "arrived_at": arrived}
            )
            with LOCK:
                sink.write(line + "\n")
                sink.flush()
            answer = b'{"ok": true}'
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(answer)))
            self.end_headers()
            self.wfile.write(answer)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main(int(sys.argv[1]), sys.argv[2])

"""Old names after the upgrade from the project mode (flow packages §13, task 12).

The upgrade gives every flow, dataset and route of an M1 project its package's prefix, `<package>/<name>`, and records
each old name in `feather.renamed`. A link, a bookmark or an API call that still uses an old name is redirected to the
new one, so nothing that pointed at a flow is left pointing at nothing:

- `/api/v1/flows/<old>…`, `/api/v1/datasets/<old>…`, `/api/v1/routes/<old>…` go to the same path with the new name,
  with 308, which keeps the method and the body.
- The dashboard's `/flows/<old>` page goes to the package's flow page, `/packages/<package>/flows/<name>`; its
  `/datasets/<old>…` and `/routes/<old>…` pages go to the same path with the new name. GETs get 302, and anything
  else 307.

Old names never clash with new ones, because a new name always has the package's prefix and an old one never does.
Webhooks sent to an old dataset's address are taken by the ingress itself (`package_push`). The table changes only when
an upgrade runs, so it is read at most once a minute.
"""

import re
import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import quote

from fastapi import FastAPI
from starlette.types import ASGIApp, Receive, Scope, Send

if TYPE_CHECKING:
    from feather_server.app import AppState

__all__ = ["install"]

_PATH = re.compile(r"^(?P<api>/api/v1)?/(?P<kinds>flows|datasets|routes)/(?P<name>[^/:]+)(?P<rest>.*)$")
_KIND = {"flows": "flow", "datasets": "dataset", "routes": "route"}
_REFRESH_S = 60.0


class _Renames:
    """The renamed table, read lazily and again at most every `_REFRESH_S` seconds."""

    def __init__(self, state: "AppState") -> None:
        self._state = state
        self._lock = threading.Lock()
        self._names: dict[tuple[str, str], str] = {}
        self._read_at = float("-inf")

    def get(self, kind: str, old: str) -> str | None:
        with self._lock:
            if time.monotonic() - self._read_at >= _REFRESH_S:
                try:
                    self._names = self._state.store.packages.renames()
                except Exception:  # noqa: BLE001 - a store blip must not break every page; try again next time
                    return self._names.get((kind, old))
                self._read_at = time.monotonic()
            return self._names.get((kind, old))


def target(path: str, method: str, renames: "_Renames") -> tuple[str, int] | None:
    """Where `path` goes when it names an old flow, dataset or route, with the redirect's status; None if it doesn't."""
    match = _PATH.match(path)
    if match is None:
        return None
    kind = _KIND[match["kinds"]]
    new = renames.get(kind, match["name"])
    if new is None:
        return None
    api, rest = match["api"] or "", match["rest"]
    if not api and kind == "flow" and rest == "" and "/" in new:
        package, short = new.split("/", 1)
        location = f"/packages/{quote(package, safe='')}/flows/{quote(short, safe='')}"
    else:
        location = f"{api}/{match['kinds']}/{quote(new, safe='/')}{rest}"
    if api:
        return location, 308
    return location, 302 if method in ("GET", "HEAD") else 307


class _Redirect:
    def __init__(self, app: ASGIApp, renames: _Renames) -> None:
        self.app = app
        self.renames = renames

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            found = target(scope["path"], scope["method"], self.renames)
            if found is not None:
                location, status = found
                query = scope.get("query_string", b"")
                if query:
                    location += "?" + query.decode("latin-1")
                await send(
                    {
                        "type": "http.response.start",
                        "status": status,
                        "headers": [(b"location", location.encode("latin-1")), (b"content-length", b"0")],
                    }
                )
                await send({"type": "http.response.body", "body": b""})
                return
        await self.app(scope, receive, send)


def install(app: FastAPI, state: "AppState") -> None:
    """Redirect requests that use an old name to the new one."""
    app.add_middleware(_Redirect, renames=_Renames(state))

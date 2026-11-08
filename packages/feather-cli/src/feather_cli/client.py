"""`ApiClient`: the one client of the JSON API (`/api/v1`), which the CLI uses and the MCP server will in M2.

Every failure is an `ApiError` whose message names the URL and says why: the server can't be reached, refused the
token, or answered with a problem (RFC 9457), whose detail it carries.
"""

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Self
from urllib.parse import quote

import httpx

API = "/api/v1"
TIMEOUT_S = 30.0


class ApiError(Exception):
    """A call that didn't succeed. `status` is the HTTP status, or None when there was no answer."""

    def __init__(
        self, message: str, *, status: int | None = None, problem: Mapping[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.status = status
        self.problem = dict(problem or {})


class ApiClient:
    """Calls the server at `base_url` with the bearer `token`."""

    def __init__(
        self,
        base_url: str,
        token: str | None,
        *,
        timeout: float = TIMEOUT_S,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._http = httpx.Client(
            base_url=self.base_url, headers=headers, timeout=timeout, transport=transport
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _call(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        url = f"{self.base_url}{API}{path}"
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        query = {k: v for k, v in (params or {}).items() if v is not None}
        try:
            answer = self._http.request(method, f"{API}{path}", params=query, json=body, headers=headers)
        except httpx.TimeoutException:
            raise ApiError(f"the server at {url} did not answer within {self._http.timeout.read}s") from None
        except httpx.HTTPError as error:
            raise ApiError(f"can't reach the server at {url}: {error}") from None
        if answer.is_success:
            return answer.json()
        try:
            problem: Mapping[str, Any] = answer.json()
        except ValueError:
            problem = {"detail": answer.text[:200]}
        detail = problem.get("detail") or answer.reason_phrase
        if answer.status_code == 401:
            message = f"the server at {self.base_url} refused the token (401): {detail}"
        elif answer.status_code == 403:
            message = f"the server at {self.base_url} refused the call (403): {detail}"
        else:
            message = f"{method} {url} failed ({answer.status_code}): {detail}"
        raise ApiError(message, status=answer.status_code, problem=problem)

    # Reads.

    def summary(self) -> dict[str, Any]:
        return self._call("GET", "/summary")

    def flows(self) -> list[dict[str, Any]]:
        return self._call("GET", "/flows")["items"]

    def runs(
        self,
        *,
        flow: str | None = None,
        status: str | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return self._call(
            "GET", "/runs", params={"flow": flow, "status": status, "limit": limit, "cursor": cursor}
        )

    def run(self, run_id: str) -> dict[str, Any]:
        return self._call("GET", f"/runs/{run_id}")

    def run_tasks(self, run_id: str, *, limit: int = 500, cursor: str | None = None) -> dict[str, Any]:
        return self._call("GET", f"/runs/{run_id}/tasks", params={"limit": limit, "cursor": cursor})

    def task_logs(self, task_id: str) -> list[str]:
        return self._call("GET", f"/tasks/{task_id}/logs")["lines"]

    # Actions.

    def start_run(
        self,
        flow: str,
        *,
        partitions: list[str] | None = None,
        reason: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self._call(
            "POST",
            f"/flows/{flow}:run",
            body={"partitions": partitions, "reason": reason},
            idempotency_key=idempotency_key,
        )

    def preview_backfill(self, flow: str, start: str, end: str, *, max_concurrent: int = 4) -> dict[str, Any]:
        return self._call(
            "POST",
            f"/flows/{flow}:backfill:preview",
            body={"start": start, "end": end, "max_concurrent": max_concurrent},
        )

    def backfill(self, flow: str, token: str, *, reason: str | None = None) -> dict[str, Any]:
        return self._call(
            "POST", f"/flows/{flow}:backfill", body={"confirmation_token": token, "reason": reason}
        )

    def preview_deliveries(self, action: str, selection: Mapping[str, Any]) -> dict[str, Any]:
        return self._call("POST", f"/deliveries:{action}:preview", body=dict(selection))

    def apply_deliveries(self, action: str, token: str, *, reason: str | None = None) -> dict[str, Any]:
        return self._call(
            "POST", f"/deliveries:{action}", body={"confirmation_token": token, "reason": reason}
        )

    # Registries.

    def registries(self) -> list[dict[str, Any]]:
        return self._call("GET", "/registries")["items"]

    def add_registry(self, name: str, index_url: str, token_ref: str | None) -> dict[str, Any]:
        return self._call(
            "POST", "/registries", body={"name": name, "index_url": index_url, "token_ref": token_ref}
        )

    def remove_registry(self, name: str) -> dict[str, Any]:
        return self._call("DELETE", f"/registries/{quote(name, safe='')}")

    def test_registry(self, name: str) -> dict[str, Any]:
        return self._call("POST", f"/registries/{quote(name, safe='')}:test")

    def search_packages(self, registry: str, query: str = "") -> list[dict[str, Any]]:
        return self._call("GET", f"/registries/{quote(registry, safe='')}/packages", params={"q": query})[
            "items"
        ]

    def publish(self, path: Path) -> None:
        """Upload a wheel to the built-in registry, as `uv publish` does: the legacy upload form at `/registry/`."""
        url = f"{self.base_url}/registry/"
        data = path.read_bytes()
        form = {
            ":action": "file_upload",
            "protocol_version": "1",
            "filetype": "bdist_wheel",
            "pyversion": "py3",
            "sha256_digest": hashlib.sha256(data).hexdigest(),
        }
        try:
            answer = self._http.post(
                url, data=form, files={"content": (path.name, data, "application/octet-stream")}
            )
        except httpx.HTTPError as error:
            raise ApiError(f"can't reach the server at {url}: {error}") from None
        if answer.is_success:
            return
        try:
            detail = answer.json().get("detail") or answer.reason_phrase
        except ValueError:
            detail = answer.text[:200] or answer.reason_phrase
        raise ApiError(f"POST {url} failed ({answer.status_code}): {detail}", status=answer.status_code)

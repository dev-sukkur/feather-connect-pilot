"""Errors of the JSON API, as RFC 9457 `problem+json`. Only paths under `/api/` answer this way; the pages keep theirs."""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exception_handlers import http_exception_handler, request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from feather_connect.core.errors import PermanentError
from feather_connect.logs import redact
from feather_connect.runtime.runtime import FlowPaused
from feather_connect.storage.port import DuplicateUser, InvalidTransition
from feather_connect.vault import KeyConflict, NoMasterKey, VaultError
from feather_server.services.access import Forbidden
from feather_server.services.packages import Busy, InstallFailed, NotReady
from feather_server.services.previews import PreviewError, PreviewInvalid
from feather_server.services.registries import VersionExists

PROBLEM_JSON = "application/problem+json"


class ApiProblem(Exception):
    """An answer that isn't a success. `extra` adds members to the problem, as RFC 9457 allows, and `headers` go
    with the answer, such as Retry-After."""

    def __init__(
        self, status: int, title: str, detail: str, *, headers: dict[str, str] | None = None, **extra: Any
    ) -> None:
        super().__init__(detail)
        self.status, self.title, self.detail, self.extra = status, title, detail, extra
        self.headers = headers or {}

    def body(self) -> dict[str, Any]:
        return {
            "type": "about:blank",
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
            **self.extra,
        }


def problem_response(problem: ApiProblem) -> JSONResponse:
    return JSONResponse(problem.body(), problem.status, headers=problem.headers, media_type=PROBLEM_JSON)


def _message(error: BaseException) -> str:
    text = error.args[0] if isinstance(error, KeyError) and error.args else str(error)
    return redact(str(text))


def translate(error: Exception) -> ApiProblem | None:
    """The problem a service error stands for, or None for an error that is a bug (a 500)."""
    if isinstance(error, ApiProblem):
        return error
    if isinstance(error, Forbidden):
        return ApiProblem(403, "Forbidden", str(error), capability=error.capability, role=error.needed)
    if isinstance(error, Busy):
        return ApiProblem(
            503, "Service Unavailable", str(error), headers={"Retry-After": str(error.retry_after)}
        )
    if isinstance(error, InstallFailed):  # the check that failed, for a client to show or act on
        status, title = (
            (409, "Conflict") if error.check == "not_installed" else (422, "Unprocessable Content")
        )
        return ApiProblem(status, title, _message(error), check=error.check)
    if isinstance(error, NotReady):
        return ApiProblem(409, "Conflict", str(error), check="not_ready", state=error.state)
    if isinstance(error, VersionExists):
        return ApiProblem(409, "Conflict", str(error))
    if isinstance(error, DuplicateUser):
        return ApiProblem(409, "Conflict", f"the user {error.args[0]!r} exists already")
    if isinstance(error, PreviewInvalid):
        return ApiProblem(400, "Bad Request", _message(error))
    if isinstance(error, PreviewError):  # expired, used or stale: preview again
        return ApiProblem(409, "Conflict", _message(error))
    if isinstance(error, FlowPaused | InvalidTransition):
        return ApiProblem(409, "Conflict", _message(error))
    if isinstance(error, NoMasterKey):  # the server runs without one: db secrets wait for it
        return ApiProblem(503, "Service Unavailable", str(error))
    if isinstance(error, KeyConflict):  # another master key is active: rotated meanwhile
        return ApiProblem(409, "Conflict", str(error))
    if isinstance(error, VaultError):
        return ApiProblem(422, "Unprocessable Content", str(error))
    if isinstance(error, KeyError | LookupError):
        return ApiProblem(404, "Not Found", _message(error))
    if isinstance(error, ValueError | PermanentError):
        return ApiProblem(422, "Unprocessable Content", _message(error))
    return None


def _is_api(request: Request) -> bool:
    return request.url.path.startswith("/api/")


def install(app: FastAPI) -> None:
    """Answer the API's own errors, and FastAPI's on API paths, as problems."""

    async def on_problem(request: Request, error: Exception) -> Response:
        assert isinstance(error, ApiProblem)
        return problem_response(error)

    async def on_validation(request: Request, error: Exception) -> Response:
        assert isinstance(error, RequestValidationError)
        if not _is_api(request):
            return await request_validation_exception_handler(request, error)
        found = [{"loc": list(e.get("loc", ())), "msg": str(e.get("msg", ""))} for e in error.errors()]
        detail = (
            "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in found)
            or "the request is not valid"
        )
        return problem_response(ApiProblem(422, "Unprocessable Content", detail, errors=found))

    async def on_http(request: Request, error: Exception) -> Response:
        assert isinstance(error, StarletteHTTPException)
        if not _is_api(request):
            return await http_exception_handler(request, error)
        title = {404: "Not Found", 405: "Method Not Allowed"}.get(error.status_code, "Error")
        return problem_response(ApiProblem(error.status_code, title, str(error.detail)))

    app.add_exception_handler(ApiProblem, on_problem)
    app.add_exception_handler(RequestValidationError, on_validation)
    app.add_exception_handler(StarletteHTTPException, on_http)

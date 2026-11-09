"""The built-in package registry's routes (flow packages §4.3), so that any Python tool reads from it and publishes to it:

- `GET /registry/simple/` and `/registry/simple/<project>/`: the simple index, in PEP 503's HTML or, when asked with
  `Accept: application/vnd.pypi.simple.v1+json`, PEP 691's JSON. Yanked files say so (PEP 592).
- `GET /registry/files/<filename>`: a wheel.
- `POST /registry/` (and `/registry/legacy/`): the upload API that `uv publish` and `twine` call, a multipart form with
  `:action=file_upload` and the file in `content`.

Every call needs a valid token, sent as `Authorization: Bearer` or as Basic credentials with the token as the password
(`uv publish --token`, `__token__:<token>@` in an index URL). An upload needs an admin or a `publish` token. Off when
`FEATHER_REGISTRY=0`.
"""

import html
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from packaging.utils import canonicalize_name
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile
from starlette.types import Message

from feather_connect.packages.manifest import ManifestError
from feather_connect.packages.wheel import MAX_WHEEL
from feather_connect.storage.port import RegistryFile
from feather_server.api.errors import PROBLEM_JSON
from feather_server.auth import AuthError, CurrentActor, require_csrf
from feather_server.services.access import Forbidden
from feather_server.services.registries import Registries, VersionExists

PREFIX = "/registry"
SIMPLE_JSON = "application/vnd.pypi.simple.v1+json"
_META = {"api-version": "1.1"}
MAX_UPLOAD = MAX_WHEEL + 1024 * 1024  # bytes: the wheel and the form around it

router = APIRouter(prefix=PREFIX, include_in_schema=False)


def _registries(request: Request) -> Registries:
    return request.app.state.registries


def _problem(status: int, title: str, detail: str) -> JSONResponse:
    body = {"type": "about:blank", "title": title, "status": status, "detail": detail}
    return JSONResponse(body, status, media_type=PROBLEM_JSON)


def _wants_json(request: Request) -> bool:
    return SIMPLE_JSON in request.headers.get("accept", "")


def _page(title: str, links: str) -> HTMLResponse:
    body = (
        '<!DOCTYPE html>\n<html><head><meta name="pypi:repository-version" content="1.1">'
        f"<title>{html.escape(title)}</title></head><body><h1>{html.escape(title)}</h1>\n{links}</body></html>\n"
    )
    return HTMLResponse(body, headers={"Cache-Control": "no-store"})


@router.get("/simple/")
def projects(request: Request, _: CurrentActor) -> Response:
    names = _registries(request).projects()
    if _wants_json(request):
        return JSONResponse({"meta": _META, "projects": [{"name": n} for n in names]}, media_type=SIMPLE_JSON)
    links = "".join(f'<a href="{html.escape(n)}/">{html.escape(n)}</a><br>\n' for n in names)
    return _page("Simple index", links)


def _file_url(file: RegistryFile) -> str:
    return f"../../files/{quote(file.filename)}"


@router.get("/simple/{project}/")
def project_page(project: str, request: Request, _: CurrentActor) -> Response:
    normal = canonicalize_name(project)
    if project != normal:  # PEP 503: the normalised name is the page's one address
        return RedirectResponse(f"{PREFIX}/simple/{normal}/", status_code=301)
    files = _registries(request).files(normal)
    if not files:
        return _problem(404, "Not Found", f"the registry has no project {normal!r}")
    if _wants_json(request):
        listed: list[dict[str, Any]] = [
            {
                "filename": f.filename,
                "url": _file_url(f),
                "hashes": {"sha256": f.sha256},
                "requires-python": ">=3.12",
                "yanked": (f.yanked_reason or True) if f.yanked_at else False,
                "size": f.size,
                "upload-time": f.uploaded_at.isoformat().replace("+00:00", "Z"),
            }
            for f in files
        ]
        return JSONResponse(
            {"meta": _META, "name": normal, "versions": [f.version for f in files], "files": listed},
            media_type=SIMPLE_JSON,
        )
    links = ""
    for f in files:
        yanked = f' data-yanked="{html.escape(f.yanked_reason or "")}"' if f.yanked_at else ""
        links += (
            f'<a href="{html.escape(_file_url(f))}#sha256={f.sha256}" data-requires-python="&gt;=3.12"{yanked}>'
            f"{html.escape(f.filename)}</a><br>\n"
        )
    return _page(f"Links for {normal}", links)


@router.get("/files/{filename}")
def file(filename: str, request: Request, _: CurrentActor) -> Response:
    try:
        found, data = _registries(request).content(filename)
    except KeyError:
        return _problem(404, "Not Found", f"the registry has no file {filename!r}")
    return Response(
        data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{found.filename}"', "X-Sha256": found.sha256},
    )


def _token_only(request: Request) -> None:
    """An upload comes from a tool with a token, never on a browser's session, which needs no CSRF check then."""
    if "authorization" not in request.headers:
        raise AuthError(401, "Unauthorized", "send a token: uv publish --token, or twine with __token__")


class _TooBig(Exception):
    pass


def _counted(request: Request, limit: int) -> Request:
    """The request, its body refused as it comes once it passes `limit` bytes, whatever Content-Length said."""
    seen = 0
    receive = request.receive

    async def counting() -> Message:
        nonlocal seen
        message = await receive()
        seen += len(message.get("body", b""))
        if seen > limit:
            raise _TooBig
        return message

    return Request(request.scope, counting)


async def _upload(request: Request, actor: CurrentActor) -> Response:
    registries = _registries(request)
    try:
        registries.may_publish(actor)  # before a byte of the body is read
    except Forbidden as error:
        return _problem(403, "Forbidden", str(error))
    too_big = _problem(413, "Content Too Large", f"a package may have at most {MAX_WHEEL} bytes")
    length = request.headers.get("content-length", "")
    if length.isdigit() and int(length) > MAX_UPLOAD:
        return too_big
    try:
        form = await _counted(request, MAX_UPLOAD).form(max_part_size=1024 * 1024)
    except _TooBig:
        return too_big
    if form.get(":action") not in (None, "file_upload"):
        return _problem(400, "Bad Request", "this registry only takes :action=file_upload")
    content = form.get("content")
    if not isinstance(content, UploadFile) or not content.filename:
        return _problem(400, "Bad Request", "send the wheel in the form's content field")
    data = await content.read(MAX_WHEEL + 1)
    if len(data) > MAX_WHEEL:
        return too_big
    digest = form.get("sha256_digest")
    try:
        await run_in_threadpool(
            registries.publish, actor, content.filename, data, digest if isinstance(digest, str) else None
        )
    except Forbidden as error:
        return _problem(403, "Forbidden", str(error))
    except VersionExists as error:
        return _problem(409, "Conflict", str(error))
    except (ManifestError, ValueError) as error:
        return _problem(400, "Bad Request", str(error))
    return Response("OK\n", media_type="text/plain")


router.add_api_route(
    "/", _upload, methods=["POST"], dependencies=[Depends(_token_only), Depends(require_csrf)]
)
router.add_api_route(
    "/legacy/", _upload, methods=["POST"], dependencies=[Depends(_token_only), Depends(require_csrf)]
)


def install(app: FastAPI) -> None:
    """Serve the registry, unless the settings turn it off."""
    if app.state.registries.enabled:
        app.include_router(router)

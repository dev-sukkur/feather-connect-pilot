"""The HTML dashboard: pages, templates and static files, served by `feather-server` with no build step."""

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles

from feather_server.services.access import Forbidden, ReasonRequired
from feather_server.services.actions import Requested
from feather_server.web import package_pages
from feather_server.web.routes import ask_reason, asked_for, refused, router

if TYPE_CHECKING:
    from feather_server.app import AppState

STATIC_DIR = Path(__file__).parent / "static"


def install(app: FastAPI, state: "AppState") -> None:
    """Add the dashboard's pages and its static files to `app`."""
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    app.include_router(router)
    app.include_router(package_pages.router)

    async def on_forbidden(request: Request, error: Exception) -> Response:
        assert isinstance(error, Forbidden)
        return refused(request, error)

    async def on_reason_required(request: Request, error: Exception) -> Response:
        assert isinstance(error, ReasonRequired)
        if request.url.path.startswith("/api/"):  # a route that let it through: answer as the API does
            from feather_server.api.errors import problem_response, translate

            problem = translate(error)
            assert problem is not None
            return problem_response(problem)
        return await ask_reason(request, error)

    app.add_exception_handler(Forbidden, on_forbidden)

    async def on_requested(request: Request, error: Exception) -> Response:
        assert isinstance(error, Requested)
        return asked_for(request, error)

    app.add_exception_handler(ReasonRequired, on_reason_required)
    app.add_exception_handler(Requested, on_requested)

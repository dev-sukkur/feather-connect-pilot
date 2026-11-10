"""The HTML dashboard: pages, templates and static files, served by `feather-server` with no build step."""

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles

from feather_server.services.access import Forbidden
from feather_server.web import package_pages
from feather_server.web.routes import refused, router

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

    app.add_exception_handler(Forbidden, on_forbidden)

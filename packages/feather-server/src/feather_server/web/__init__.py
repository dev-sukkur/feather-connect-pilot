"""The HTML dashboard: pages, templates and static files, served by `feather-server` with no build step."""

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from feather_server.web.routes import router

if TYPE_CHECKING:
    from feather_server.app import AppState

STATIC_DIR = Path(__file__).parent / "static"


def install(app: FastAPI, state: "AppState") -> None:
    """Add the dashboard's pages and its static files to `app`."""
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    app.include_router(router)

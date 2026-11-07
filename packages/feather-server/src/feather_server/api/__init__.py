"""The JSON API: thin routers over the service layer, under `/api/v1`."""

from fastapi import FastAPI

from feather_server.api import errors
from feather_server.api.routes import PREFIX, router

__all__ = ["PREFIX", "install"]


def install(app: FastAPI) -> None:
    errors.install(app)
    app.include_router(router)

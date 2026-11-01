"""The server's settings, read from the environment (design 18.1)."""

import os
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast, get_args

Engine = Literal["dbos", "local"]


class SettingsError(Exception):
    """A setting is missing or has a value the server can't use. The message names the variable."""


@dataclass
class Settings:
    """What the server needs to start. `engine_poll_s` is how often workers poll their queues, `secret_key` signs
    session cookies, and `ingress_max_bytes` and `ingress_rate_per_s` limit each webhook source."""

    env: str = "dev"
    database_url: str = ""
    engine: Engine = "dbos"
    engine_poll_s: float = 0.5
    spool: Literal["postgres"] = "postgres"
    secret_key: str = ""
    base_url: str = "http://localhost:8000"
    project: Path = Path(".")
    ingress_max_bytes: int = 1024 * 1024
    ingress_rate_per_s: float = 1000.0

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        """Read the settings from `environ`, which is the process environment by default. The database and the project
        are required.

        A missing secret key is made up for this process, which signs out everyone whenever the server restarts.
        """

        environ = os.environ if environ is None else environ

        def need(name: str) -> str:
            if not environ.get(name):
                raise SettingsError(f"{name} is not set")
            return environ[name]

        engine = environ.get("FEATHER_ENGINE", "dbos")
        if engine not in get_args(Engine):
            raise SettingsError(f"FEATHER_ENGINE is {engine!r}: use one of {', '.join(get_args(Engine))}")
        spool = environ.get("FEATHER_SPOOL", "postgres")
        if spool != "postgres":
            raise SettingsError(f"FEATHER_SPOOL is {spool!r}: only postgres is available until M2")
        try:
            poll = float(environ.get("FEATHER_ENGINE_POLL_S", "0.5"))
        except ValueError:
            raise SettingsError("FEATHER_ENGINE_POLL_S is not a number") from None
        try:
            max_bytes = int(environ.get("FEATHER_INGRESS_MAX_BYTES", str(cls.ingress_max_bytes)))
            rate = float(environ.get("FEATHER_INGRESS_RATE_PER_S", str(cls.ingress_rate_per_s)))
        except ValueError:
            raise SettingsError(
                "FEATHER_INGRESS_MAX_BYTES or FEATHER_INGRESS_RATE_PER_S is not a number"
            ) from None
        if max_bytes <= 0 or not 0 < rate < float("inf"):
            raise SettingsError("FEATHER_INGRESS_MAX_BYTES and FEATHER_INGRESS_RATE_PER_S must be above zero")
        return cls(
            env=environ.get("FEATHER_ENV", "dev"),
            database_url=need("FEATHER_DATABASE_URL"),
            engine=cast("Engine", engine),
            engine_poll_s=poll,
            secret_key=environ.get("FEATHER_SECRET_KEY") or secrets.token_urlsafe(32),
            base_url=environ.get("FEATHER_BASE_URL", "http://localhost:8000"),
            project=Path(need("FEATHER_PROJECT")),
            ingress_max_bytes=max_bytes,
            ingress_rate_per_s=rate,
        )

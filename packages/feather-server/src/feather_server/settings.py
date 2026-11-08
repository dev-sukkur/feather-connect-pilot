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
    session cookies, and `ingress_max_bytes` and `ingress_rate_per_s` limit each webhook source.

    `ingress_queue_max` is how many webhook requests may wait to be stored, past which one is answered 503 at once: a
    few seconds at the rate limit. `ingress_wait_s` is how long a request waits for the database before it is answered
    503, which is within what senders commonly wait for an answer."""

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
    ingress_queue_max: int = 5000
    ingress_wait_s: float = 5.0
    registry: bool = True  # the built-in package registry at /registry/; FEATHER_REGISTRY=0 turns it off

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
        if (
            engine == "local"
        ):  # nothing in a server process runs its tasks, so runs would stay queued (Stage 2 M10)
            raise SettingsError(
                "FEATHER_ENGINE=local runs no tasks in the server: leave it unset (dbos) and start a worker, "
                "or use feather dev or feather run --local"
            )
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
            queue_max = int(environ.get("FEATHER_INGRESS_QUEUE_MAX", str(cls.ingress_queue_max)))
            wait = float(environ.get("FEATHER_INGRESS_WAIT_S", str(cls.ingress_wait_s)))
        except ValueError:
            raise SettingsError(
                "FEATHER_INGRESS_MAX_BYTES, FEATHER_INGRESS_RATE_PER_S, FEATHER_INGRESS_QUEUE_MAX or "
                "FEATHER_INGRESS_WAIT_S is not a number"
            ) from None
        if max_bytes <= 0 or queue_max <= 0 or not 0 < rate < float("inf") or not 0 < wait < float("inf"):
            raise SettingsError(
                "FEATHER_INGRESS_MAX_BYTES, FEATHER_INGRESS_RATE_PER_S, FEATHER_INGRESS_QUEUE_MAX and "
                "FEATHER_INGRESS_WAIT_S must be above zero"
            )
        registry = environ.get("FEATHER_REGISTRY", "1").strip().lower()
        if registry not in {"0", "1", "true", "false", "on", "off", "yes", "no"}:
            raise SettingsError("FEATHER_REGISTRY is 1 to serve the built-in package registry, or 0 not to")
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
            ingress_queue_max=queue_max,
            ingress_wait_s=wait,
            registry=registry in {"1", "true", "on", "yes"},
        )

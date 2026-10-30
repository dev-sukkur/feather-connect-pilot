"""The context a connector is called with: its settings, a logger, the run's time and the services to use."""

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from feather_connect.core.errors import PermanentError


@dataclass(eq=False)  # a context is a handle that remembers a checkpoint, not a value to compare
class Context:
    """What a source or a sink is given with each call.

    `config` holds the connection's settings, secrets included. `now` is the time of the run, the same for
    every call in it, and `tz` is the IANA name of the time zone the flow works in. `budget` and `http` are
    the metered services of the connection, when it has them.
    """

    config: Mapping[str, Any] = field(repr=False)  # it can hold secrets, which must stay out of logs
    log: logging.Logger
    now: datetime
    tz: str
    budget: Any | None = None
    http: Any | None = None
    _checkpoint: str | None = field(default=None, init=False, repr=False)

    def secret(self, name: str) -> str:
        """Return the secret `name` from the configuration.

        Raises `PermanentError` if there is no such secret, or it isn't text.
        """
        value = self.config.get(name)
        if not isinstance(value, str):
            raise PermanentError(f"no secret named {name!r} is configured")
        return value

    def checkpoint(self, value: str | None = None) -> str | None:
        """Save `value` as the point a read has reached, so that a restarted read can resume from it.

        With no value it saves nothing. Either way it returns the latest checkpoint, or None if there is none.
        """
        if value is not None:
            self._checkpoint = value
        return self._checkpoint

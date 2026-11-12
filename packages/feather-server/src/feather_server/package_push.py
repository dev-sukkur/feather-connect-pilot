"""The push datasets of installed packages at the server's ingress (flow packages §7.2, §13): an M1 project's webhooks
keep coming in after the upgrade made it a package, at the address they had, and at the prefixed one.

The server never imports a package's code. A push dataset is taken here only when its source is Feather Connect's own
(`feather_connect.connectors.*`, such as `http.Webhook`) and its flow spec says the source's settings (`push`, from
`push_options`): the server makes the source again from them. Its connection is configured as the package's files and
the environment say, read from the stored wheel's files, unpacked and never imported, as the worker supervisor reads
them; its secrets are resolved here, as the server resolves the environment's. The events go to the inbox under the
dataset's prefixed name, and the version's package host seals them (`seal_once`).
"""

import importlib
import logging
import tempfile
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from feather_connect.core.connector import PushSource
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.logs import register_secrets
from feather_connect.packages import manifest as m
from feather_connect.packages.check import unpack
from feather_connect.project import ConnectionConfig, ProjectError, connection_configs, environment_layer
from feather_connect.runtime.catalogue import Catalogue, PackagePush
from feather_connect.storage.port import Store

__all__ = ["PackageDataset", "PackagePushes"]

log = logging.getLogger(__name__)

_PLATFORM = "feather_connect.connectors."  # the only modules a source is made from here
_KEPT_S = 5.0


@dataclass(frozen=True)
class _Strategy:
    name: str = "push"


@dataclass(frozen=True)
class PackageDataset:
    """A push dataset of an installed package, as ingress takes it: its prefixed name, its source and how to make the
    context its source verifies with."""

    name: str
    source: PushSource
    package: str
    config: ConnectionConfig
    timezone: str
    strategy: _Strategy = _Strategy()
    tenant: str = "default"

    def context(self, now: datetime) -> Context:
        try:
            resolved = self.config.resolve()
        except ProjectError as error:
            raise PermanentError(f"connection {self.config.name!r}: {error}") from None
        register_secrets(*(resolved[key] for key in self.config.secret_refs))
        return Context(resolved, logging.getLogger(self.config.name), now, self.timezone, tenant=self.tenant)


class PackagePushes:
    """Makes the push datasets of active packages for ingress, keeping each version's unpacked files."""

    def __init__(self, store: Store, catalogue: Catalogue, env: str, cache: Path | None = None) -> None:
        self._store = store
        self._catalogue = catalogue
        self._env = env
        self._cache = cache or Path(tempfile.gettempdir()) / "feather-server" / "config"
        self._lock = threading.Lock()
        self._made_at: dict[str, tuple[float, PackageDataset | None]] = {}

    def dataset(self, name: str) -> PackageDataset | None:
        """The push dataset called `name`, or what an old M1 name was renamed to; None if no active flow pushes it
        or its source isn't one the server can make. Kept for `_KEPT_S`, so a burst reads its files once."""
        now = time.monotonic()
        kept = self._made_at.get(name)
        if kept is not None and now - kept[0] < _KEPT_S:
            return kept[1]
        found = self._dataset(name)
        if len(self._made_at) > 1000:
            self._made_at.clear()
        self._made_at[name] = (now, found)
        return found

    def _dataset(self, name: str) -> PackageDataset | None:
        push = self._catalogue.push(name)
        if push is None:
            renamed = self._store.packages.renames().get(("dataset", name))
            push = None if renamed is None else self._catalogue.push(renamed)
        if push is None:
            return None
        try:
            return self._made(push)
        except Exception:  # a version this server can't read takes no webhooks: say so once per request
            log.exception("ingress %s: can't take its webhooks", push.dataset)
            return None

    def _made(self, push: PackagePush) -> PackageDataset | None:
        module, _, qualname = push.source.partition(":")
        if not module.startswith(_PLATFORM):
            log.warning("ingress %s: its source %s isn't Feather Connect's own", push.dataset, push.source)
            return None
        kind: Any = importlib.import_module(module)
        for part in qualname.split("."):
            kind = getattr(kind, part)
        source = cast(PushSource, kind(push.connection, **dict(push.options)))
        config = self._connections(push.package, push.version).get(push.connection)
        if config is None:
            log.warning("ingress %s: the connection %s isn't configured", push.dataset, push.connection)
            return None
        timezone = str(self._settings().get("timezone") or "UTC")
        return PackageDataset(
            push.dataset, source, push.package, config, timezone, tenant=self._store.secrets.tenant
        )

    def _settings(self) -> Mapping[str, Any]:
        return {e.name: e.value for e in self._store.env_config.entries(self._env, "setting")}

    def _connections(self, package: str, version: str) -> dict[str, ConnectionConfig]:
        row = self._store.packages.version(package, version)
        if row is None:
            return {}
        site = self._cache / row.sha256
        with self._lock:
            if not (site / ".unpacked").is_file():  # its files only: no code of it is imported here
                content = self._store.packages.wheel(row.sha256)
                if content is None:
                    return {}
                site.mkdir(parents=True, exist_ok=True)
                unpack(content, site)
                (site / ".unpacked").write_text(row.sha256, encoding="utf-8")
        return connection_configs(
            site / m.module_of(row.name),
            self._env,
            environment=environment_layer(self._store.env_config.entries(self._env)),
            package=row.name,
        )

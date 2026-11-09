"""Installing flow packages (flow packages design §5): a version fetched from a trusted registry, checked, loaded in a
child process, and recorded with every flow inactive. For the API, the dashboard and the CLI alike.

The checks run in order and an install stops at the first that fails, which `InstallFailed.check` names:

- `not_installed`: the version (by `version_key`) is not installed here yet; a version always means the same code.
- `fetch`: the registry knows the package and serves the version. With none asked for, it is the newest that isn't
  yanked, and not a pre-release unless `pre` says so.
- `size`, `sha256`, `manifest`, `name_version`: the wheel is at most 50 MiB, has the sha256 the registry recorded, holds
  a manifest that matches the schema, and names the package and version asked for.
- `not_yanked`: the registry hasn't yanked the version, as it says at the fetch, unless `allow_yanked` says to take it.
- `compatibility`: the manifest's `fc_versions` takes this Feather Connect.
- `lock`: the locked requirements in the wheel are the ones the manifest's `lock_hash` names, and each is pinned with
  `==` and has its sha256 hashes: no index options, direct URLs or paths.
- `load_check`: in a child process, the code imports, loads for this environment with no error, and defines the flows,
  datasets and routes the manifest says. This process never imports package code. The check is a correctness gate, not
  a security boundary, until task 6 moves it to the package hosts (see `feather_connect.packages.check`).
- `webhook_connector`: no push dataset reads with a source class of the package's own. Webhook ingress runs in the
  server, which never loads package code, so a package can't bring its own webhook connector (§7.2, §12).

At most `MAX_BUSY` installs and wheel fetches run at once, for each holds a worker thread for as long as a load check
takes; one more raises `Busy`, which the API answers 503 with Retry-After.

Then the wheel, the version and its flows (named `<package>/<flow>`, all `inactive`) are stored in one transaction,
and an audit row says who installed what and why. Nothing runs until a flow is activated (§6). Every failure is audited
too, with its check, or as `internal` for an error that is a bug.
"""

import hashlib
import re
import threading
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, cast

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from feather_connect.clock import Clock
from feather_connect.logs import redact
from feather_connect.packages import manifest as m
from feather_connect.packages.build import LOCK_FILE
from feather_connect.packages.check import DEFAULT_TIMEOUT, LoadCheck, load_check
from feather_connect.packages.manifest import ManifestError
from feather_connect.packages.webhooks import parse_webhooks
from feather_connect.packages.wheel import read_entry
from feather_connect.project import environment_layer
from feather_connect.runtime.catalogue import restart_ticks
from feather_connect.storage.port import DuplicateVersion, FlowActivation, PackageVersion, Store
from feather_server.auth import Actor
from feather_server.services.access import require
from feather_server.services.registries import BUILT_IN, Fetched, Registries, RegistryProblem

__all__ = ["CHECKS", "MAX_BUSY", "Busy", "InstallFailed", "Packages", "check_lock"]

MAX_BUSY = 2  # installs and wheel fetches at once
BUSY_RETRY_S = 30

CHECKS = (
    "not_installed",
    "fetch",
    "size",
    "sha256",
    "manifest",
    "name_version",
    "not_yanked",
    "compatibility",
    "lock",
    "load_check",
    "webhook_connector",
)
_PINNED = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*(\[[A-Za-z0-9._, -]+\])?==[A-Za-z0-9.+!_-]+(\s*;.*)?")
_HASH = re.compile(r"--hash=sha256:[0-9a-f]{64}")

Checker = Callable[..., LoadCheck]


class InstallFailed(ValueError):
    """An install stopped at `check`, one of `CHECKS`, for the reason in the message. Nothing was stored."""

    def __init__(self, check: str, message: str) -> None:
        super().__init__(f"{check}: {message}")
        self.check = check


class Busy(Exception):
    """As many installs and wheel fetches as may run at once are running: try again in `retry_after` seconds."""

    def __init__(self, retry_after: int = BUSY_RETRY_S) -> None:
        super().__init__(f"{MAX_BUSY} installs or fetches are running: try again in {retry_after} s")
        self.retry_after = retry_after


def check_lock(lock: bytes) -> None:
    """Every requirement in a package's lock pinned with `==` and carrying sha256 hashes, and nothing else: no
    `--index-url` or other option, no direct URL, no path. Raises `ValueError` naming the first line that isn't."""
    text = re.sub(r"\\\r?\n", " ", lock.decode("utf-8", "replace"))  # a line ending in a backslash goes on
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.split(" #", 1)[0].strip()
        parts = line.split()
        hashes = [p for p in parts if p.startswith("--hash")]
        requirement = " ".join(p for p in parts if not p.startswith("--hash"))
        shown = requirement[:80] or line[:80]
        if requirement.startswith("-"):
            raise ValueError(f"line {number}: {shown!r} is an option; a lock holds only pinned requirements")
        if "://" in requirement or "@" in requirement or not _PINNED.fullmatch(requirement):
            raise ValueError(f"line {number}: {shown!r} is not pinned as name==version (no URLs or paths)")
        if not hashes or not all(_HASH.fullmatch(h) for h in hashes):
            raise ValueError(
                f"line {number}: {shown!r} has no --hash=sha256:<64 hex digits> for each of its files"
            )


@dataclass(frozen=True)
class Installed:
    """A version as installed: what the API answers."""

    name: str
    version: str
    sha256: str
    registry: str
    state: str
    flows: list[str]
    checks: list[str]
    load_check: Mapping[str, Any]


class Packages:
    """The installed packages of one store's tenant, for the environment `env`. `checker` and `timeout` run the load
    check; tests may pass their own checker."""

    def __init__(
        self,
        store: Store,
        clock: Clock,
        registries: Registries,
        *,
        env: str,
        checker: Checker = load_check,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._store = store
        self._clock = clock
        self._registries = registries
        self._env = env
        self._checker = checker
        self._timeout = timeout
        self._slots = threading.BoundedSemaphore(MAX_BUSY)

    # Installing.

    def install(
        self,
        actor: Actor,
        name: str,
        version: str | None = None,
        *,
        registry: str = BUILT_IN,
        reason: str | None = None,
        allow_yanked: bool = False,
        pre: bool = False,
    ) -> Installed:
        """Install a version of a package from one of the environment's registries, its flows inactive. Raises
        `Forbidden` for an actor that isn't an admin, `Busy` when `MAX_BUSY` installs or fetches are running, and
        `InstallFailed` naming the check that failed."""
        project = canonicalize_name(name)
        target = f"package:{project}"
        require(actor, "packages.install", self._store.audit, target)
        request: dict[str, Any] = {
            "version": version,
            "registry": registry,
            "reason": reason,
            "allow_yanked": allow_yanked,
            "pre": pre,
        }
        with self._slot():
            try:
                installed = self._install(actor, project, version, registry, reason, request)
            except InstallFailed as error:
                self._audit(actor, "install_package", target, request, f"failed: {redact(str(error))}")
                raise
            except Exception:
                self._audit(actor, "install_package", target, request, "failed: internal")
                raise
        self._audit(actor, "install_package", target, request, "ok")
        return installed

    def fetch(self, actor: Actor, registry: str, name: str, version: str) -> Fetched:
        """A version's wheel from a registry, its sha256 and manifest checked, for `feather package fetch`: an
        admin's, as an install is, and audited. Raises `Busy` as `install` does."""
        project = canonicalize_name(name)
        target = f"package:{project}"
        require(actor, "packages.install", self._store.audit, target)
        request: dict[str, Any] = {"version": version, "registry": registry}
        with self._slot():
            try:
                fetched = self._registries.fetch(registry, project, version)
            except (KeyError, RegistryProblem) as error:
                text = error.args[0] if isinstance(error, KeyError) and error.args else error
                self._audit(actor, "fetch_package", target, request, f"failed: {redact(str(text))}")
                raise
            except Exception:
                self._audit(actor, "fetch_package", target, request, "failed: internal")
                raise
        request["sha256"] = fetched.sha256
        self._audit(actor, "fetch_package", target, request, "ok")
        return fetched

    @contextmanager
    def _slot(self) -> Generator[None]:
        if not self._slots.acquire(blocking=False):
            raise Busy()
        try:
            yield
        finally:
            self._slots.release()

    def _audit(self, actor: Actor, action: str, target: str, request: dict[str, Any], result: str) -> None:
        self._store.audit.record(actor.kind, actor.name, action, target, request, result)

    def _install(
        self,
        actor: Actor,
        project: str,
        version: str | None,
        registry: str,
        reason: str | None,
        request: dict[str, Any],
    ) -> Installed:
        passed: list[str] = []
        wanted = self._version_of(project, version, registry, pre=bool(request["pre"]))
        request["version"] = wanted
        if self._store.packages.version(project, wanted) is not None:
            raise InstallFailed(
                "not_installed", f"{project} {wanted} is installed here already: install a new version"
            )
        passed.append("not_installed")
        fetched = self._fetch(project, wanted, registry)
        request["sha256"] = fetched.sha256
        passed += ["fetch", "size", "sha256", "manifest", "name_version"]
        if fetched.yanked and not request["allow_yanked"]:
            raise InstallFailed(
                "not_yanked",
                f"registry {registry} has yanked {project} {wanted}: allow a yanked version to take it anyway",
            )
        passed.append("not_yanked")
        manifest = fetched.manifest
        try:
            m.check_fc_versions(manifest["fc_versions"])
        except ManifestError as error:
            raise InstallFailed("compatibility", str(error)) from None
        passed.append("compatibility")
        self._check_lock(fetched)
        passed.append("lock")
        environment = environment_layer(self._store.env_config.entries(self._env))
        found = self._checker(
            fetched.content, manifest, env=self._env, timeout=self._timeout, environment=environment
        )
        if not found.ok:
            raise InstallFailed("load_check", found.error or "the package doesn't load")
        passed.append("load_check")
        if found.own_push_sources:
            raise InstallFailed(
                "webhook_connector",
                f"the push datasets {', '.join(found.own_push_sources)} read with a source of the package's own: "
                "webhooks are verified and parsed in the server, which runs only Feather Connect's connectors",
            )
        try:
            declared = parse_webhooks(found.webhooks)
        except ValueError as error:
            raise InstallFailed("webhook_connector", f"a declared webhook is not valid: {error}") from None
        unknown = sorted(h.dataset for h in declared.values() if h.dataset not in found.datasets)
        if unknown:
            raise InstallFailed(
                "webhook_connector", f"declared webhooks feed datasets it doesn't define: {unknown}"
            )
        passed.append("webhook_connector")
        flows = [f"{project}/{flow}" for flow in sorted(found.flows)]
        row = PackageVersion(
            project,
            str(manifest["version"]),
            fetched.sha256,
            registry,
            fetched.filename,
            len(fetched.content),
            manifest,
            str(manifest["fc_versions"]),
            str(manifest["lock_hash"]),
            "installed",
            found.as_json(),
            actor.name,
            self._clock.now(),
        )
        try:
            self._store.packages.install(row, fetched.content, flows, reason)
        except DuplicateVersion:  # installed meanwhile, by another request
            raise InstallFailed("not_installed", f"{project} {wanted} is installed here already") from None
        return Installed(project, row.version, row.sha256, registry, row.state, flows, passed, row.load_check)

    def _version_of(self, project: str, version: str | None, registry: str, *, pre: bool) -> str:
        """The version asked for, in its normal form, or the registry's newest that isn't yanked, nor a pre-release
        unless `pre`."""
        if version is not None:
            try:
                return str(Version(version))
            except InvalidVersion:
                raise InstallFailed("fetch", f"{version!r} is not a version, such as 1.4.0") from None
        try:
            found = [f for f in self._registries.search(registry, project) if f.name == project]
        except KeyError as error:
            raise InstallFailed("fetch", str(error.args[0] if error.args else error)) from None
        except RegistryProblem as error:
            raise InstallFailed("fetch", str(error)) from None
        listed = found[0] if found else None
        usable = [
            v
            for v in (listed.versions if listed else [])
            if listed and v not in listed.yanked and (pre or not Version(v).is_prerelease)
        ]
        if not usable:
            kind = "version" if pre else "release"
            raise InstallFailed(
                "fetch",
                f"registry {registry} has no {kind} of {project} that isn't yanked: name a version to take one",
            )
        return usable[0]

    def _fetch(self, project: str, version: str, registry: str) -> Fetched:
        try:
            return self._registries.fetch(registry, project, version)
        except KeyError as error:
            raise InstallFailed("fetch", str(error.args[0] if error.args else error)) from None
        except RegistryProblem as error:
            raise InstallFailed(error.check, str(error)) from None

    @staticmethod
    def _check_lock(fetched: Fetched) -> None:
        manifest = fetched.manifest
        where = f"{m.module_of(manifest['name'])}-{manifest['version']}.dist-info/{LOCK_FILE}"
        try:
            lock = read_entry(fetched.content, where)
        except ManifestError as error:
            raise InstallFailed("lock", str(error)) from None
        if lock is None:
            raise InstallFailed("lock", f"the wheel has no {where}")
        if hashlib.sha256(lock).hexdigest() != manifest["lock_hash"]:
            raise InstallFailed("lock", f"{where} is not the lock the manifest's lock_hash names")
        try:
            check_lock(lock)
        except ValueError as error:
            raise InstallFailed("lock", f"{where}: {error}") from None

    # Activating (§6): the minimal step of task 5; package hosts come with task 6.

    def activate(self, actor: Actor, flow: str, version: str, *, reason: str | None = None) -> dict[str, Any]:
        """Make `version` the active one of `flow` (`<package>/<name>`), audited. The scheduler fires its triggers
        from its next tick, with no restart. Raises `KeyError` when the version or the flow isn't installed."""
        target = f"flow:{flow}"
        require(actor, "flows.activate", self._store.audit, target)
        package = canonicalize_name(flow.split("/", 1)[0])
        installed = self._store.packages.version(package, version)
        if installed is None:
            raise KeyError(f"{package} {version} is not installed here")
        now = self._clock.now()
        spec = cast(dict[str, Any], installed.load_check.get("specs") or {}).get(flow)
        if spec is not None:
            restart_ticks(self._store, flow, spec, now)  # no catch-up of the time it was inactive
        done = self._store.packages.activate(flow, package, installed.version, actor.name, now, reason)
        self._sync(package, installed)
        request = {"version": installed.version, "reason": reason}
        self._audit(actor, "activate_flow", target, request, "ok")
        return self._activation(done)

    def deactivate(self, actor: Actor, flow: str, *, reason: str | None = None) -> dict[str, Any]:
        """Take `flow` out of service: its triggers stop at the next tick. Runs in progress finish."""
        target = f"flow:{flow}"
        require(actor, "flows.activate", self._store.audit, target)
        done = self._store.packages.deactivate(flow, actor.name, self._clock.now(), reason)
        installed = self._store.packages.version(done.package, done.version)
        if installed is not None:
            self._sync(done.package, installed)
        self._audit(actor, "deactivate_flow", target, {"version": done.version, "reason": reason}, "ok")
        return self._activation(done)

    def _sync(self, package: str, version: PackageVersion) -> None:
        """Show the package's active flows of `version` in the catalogue, as the dashboard reads it."""
        active = {
            a.flow
            for a in self._store.packages.active()
            if a.package == package and a.version == version.version
        }
        specs = cast(dict[str, Any], version.load_check.get("specs") or {})
        self._store.catalog.sync(package, version.sha256, {f: specs[f] for f in sorted(active) if f in specs})

    # Reading.

    def packages(self) -> list[dict[str, Any]]:
        """Each installed package, with its versions oldest first and how many of their flows are active."""
        versions: dict[str, list[PackageVersion]] = {}
        for row in self._store.packages.versions():
            versions.setdefault(row.name, []).append(row)
        return [
            {
                "name": name,
                "versions": [self._summary(v) for v in rows],
                "active_flows": sum(a.state == "active" for a in self._store.packages.activations(name)),
            }
            for name, rows in versions.items()
        ]

    def package(self, name: str) -> dict[str, Any]:
        """A package's versions and its flows' activations. Raises `KeyError` when it isn't installed."""
        project = canonicalize_name(name)
        rows = self._store.packages.versions(project)
        if not rows:
            raise KeyError(f"{project} is not installed here")
        return {
            "name": project,
            "versions": [self._summary(v) for v in rows],
            "flows": [self._activation(a) for a in self._store.packages.activations(project)],
        }

    def version(self, name: str, version: str) -> dict[str, Any]:
        """One installed version: its manifest, its load check and its flows. Raises `KeyError` when it isn't
        installed."""
        project = canonicalize_name(name)
        row = self._store.packages.version(project, version)
        if row is None:
            raise KeyError(f"{project} {version} is not installed here")
        return {
            **self._summary(row),
            "filename": row.filename,
            "size": row.size,
            "fc_versions": row.fc_versions,
            "lock_hash": row.lock_hash,
            "manifest": row.manifest,
            "load_check": row.load_check,
            "flows": [self._activation(a) for a in self._store.packages.activations(project, row.version)],
        }

    @staticmethod
    def _summary(row: PackageVersion) -> dict[str, Any]:
        return {
            "name": row.name,
            "version": row.version,
            "state": row.state,
            "sha256": row.sha256,
            "registry": row.registry,
            "installed_by": row.installed_by,
            "installed_at": row.installed_at.isoformat(),
        }

    @staticmethod
    def _activation(row: FlowActivation) -> dict[str, Any]:
        return {
            "flow": row.flow,
            "version": row.version,
            "state": row.state,
            "changed_by": row.changed_by,
            "changed_at": row.changed_at.isoformat(),
            "reason": row.reason,
            "error": row.error,
        }

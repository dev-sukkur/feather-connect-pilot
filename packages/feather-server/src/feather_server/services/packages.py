"""Installing flow packages (flow packages design §5): a version fetched from a trusted registry, checked, loaded in a
child process, and recorded with every flow inactive. For the API, the dashboard and the CLI alike.

The checks run in order and an install stops at the first that fails, which `InstallFailed.check` names:

- `not_installed`: the version (by `version_key`) is not installed here yet; a version always means the same code.
- `fetch`: the registry knows the package and serves the version (the newest not yanked when none is asked for).
- `size`, `sha256`, `manifest`, `name_version`: the wheel is at most 50 MiB, has the sha256 the registry recorded, holds
  a manifest that matches the schema, and names the package and version asked for.
- `compatibility`: the manifest's `fc_versions` takes this Feather Connect.
- `lock`: the locked requirements in the wheel are the ones the manifest's `lock_hash` names.
- `load_check`: in a child process, the code imports, loads for this environment with no error, and defines the flows,
  datasets and routes the manifest says. This process never imports package code.

Then the wheel, the version and its flows (named `<package>/<flow>`, all `inactive`) are stored in one transaction,
and an audit row says who installed what and why. Nothing runs until a flow is activated (§6). Every failure is audited
too, with its check.
"""

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from feather_connect.clock import Clock
from feather_connect.logs import redact
from feather_connect.packages import manifest as m
from feather_connect.packages.build import LOCK_FILE
from feather_connect.packages.check import DEFAULT_TIMEOUT, LoadCheck, load_check
from feather_connect.packages.manifest import ManifestError
from feather_connect.packages.wheel import read_entry
from feather_connect.storage.port import DuplicateVersion, FlowActivation, PackageVersion, Store
from feather_server.auth import Actor
from feather_server.services.access import require
from feather_server.services.registries import BUILT_IN, Fetched, Registries, RegistryProblem

__all__ = ["CHECKS", "InstallFailed", "Packages"]

CHECKS = (
    "not_installed",
    "fetch",
    "size",
    "sha256",
    "manifest",
    "name_version",
    "compatibility",
    "lock",
    "load_check",
)

Checker = Callable[..., LoadCheck]


class InstallFailed(ValueError):
    """An install stopped at `check`, one of `CHECKS`, for the reason in the message. Nothing was stored."""

    def __init__(self, check: str, message: str) -> None:
        super().__init__(f"{check}: {message}")
        self.check = check


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

    # Installing.

    def install(
        self,
        actor: Actor,
        name: str,
        version: str | None = None,
        *,
        registry: str = BUILT_IN,
        reason: str | None = None,
    ) -> Installed:
        """Install a version of a package from one of the environment's registries, its flows inactive. Raises
        `Forbidden` for an actor that isn't an admin, and `InstallFailed` naming the check that failed."""
        project = canonicalize_name(name)
        target = f"package:{project}"
        require(actor, "packages.install", self._store.audit, target)
        request: dict[str, Any] = {"version": version, "registry": registry, "reason": reason}
        try:
            installed = self._install(actor, project, version, registry, reason, request)
        except InstallFailed as error:
            self._store.audit.record(
                actor.kind, actor.name, "install_package", target, request, f"failed: {redact(str(error))}"
            )
            raise
        self._store.audit.record(actor.kind, actor.name, "install_package", target, request, "ok")
        return installed

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
        wanted = self._version_of(project, version, registry)
        request["version"] = wanted
        if self._store.packages.version(project, wanted) is not None:
            raise InstallFailed(
                "not_installed", f"{project} {wanted} is installed here already: install a new version"
            )
        passed.append("not_installed")
        fetched = self._fetch(project, wanted, registry)
        request["sha256"] = fetched.sha256
        passed += ["fetch", "size", "sha256", "manifest", "name_version"]
        manifest = fetched.manifest
        try:
            m.check_fc_versions(manifest["fc_versions"])
        except ManifestError as error:
            raise InstallFailed("compatibility", str(error)) from None
        passed.append("compatibility")
        self._check_lock(fetched)
        passed.append("lock")
        found = self._checker(fetched.content, manifest, env=self._env, timeout=self._timeout)
        if not found.ok:
            raise InstallFailed("load_check", found.error or "the package doesn't load")
        passed.append("load_check")
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

    def _version_of(self, project: str, version: str | None, registry: str) -> str:
        """The version asked for, in its normal form, or the registry's newest that isn't yanked."""
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
        usable = [v for v in found[0].versions if v not in found[0].yanked] if found else []
        if not usable:
            raise InstallFailed("fetch", f"registry {registry} has no version of {project} that isn't yanked")
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

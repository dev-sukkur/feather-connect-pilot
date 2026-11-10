"""Installing flow packages (flow packages design §5): a version fetched from a trusted registry, checked, and recorded
`preparing` with every flow inactive; a worker then builds its environment and load-checks it, and records it `ready`
or `failed` (`feather_connect.packages.prepare`). For the API, the dashboard and the CLI alike.

The checks run in order and an install stops at the first that fails, which `InstallFailed.check` names:

- `not_installed`: the version (by `version_key`) is not installed here yet; a version always means the same code.
- `fetch`: the registry knows the package and serves the version. With none asked for, it is the newest that isn't
  yanked, and not a pre-release unless `pre` says so.
- `size`, `sha256`, `manifest`, `name_version`: the wheel is at most 50 MiB, has the sha256 the registry recorded, holds
  a manifest that matches the schema, and names the package and version asked for.
- `not_yanked`: the registry hasn't yanked the version, as it says at the fetch, unless `allow_yanked` says to take it.
- `signature`: where the environment's `require_signed_packages` setting is on, the wheel is signed by one of its
  `trusted_package_keys`, and its files are the ones the signed RECORD lists (`feather_connect.packages.signing`).
- `compatibility`: the manifest's `fc_versions` takes this Feather Connect.
- `lock`: the locked requirements in the wheel are the ones the manifest's `lock_hash` names, and each is pinned with
  `==` and has its sha256 hashes: no index options, direct URLs or paths.

Then the wheel, the version (`preparing`) and its flows as the manifest names them (`<package>/<flow>`, all
`inactive`) are stored in one transaction, and an audit row says who installed what and why. Every failure is audited
too, with its check, or as `internal` for an error that is a bug.

The server never runs package code. The steps that do run on a worker, as its user, in the version's own environment:
`environment`, `load_check` and `webhook_connector` (`PREPARE_STEPS`), and the version's load check names the one that
failed. A flow can be activated only once its version is `ready` (`NotReady` otherwise); nothing runs until then (§6).

Promotion (§6, task 10) installs in this environment exactly the wheel another one tested: `export` gives a `ready`
version's stored wheel, and `promote` takes those bytes with the sha256 the source gave, refuses them unless they hash
to it, keeps a copy in the built-in registry (or refuses if the registry has other bytes of that version), and installs
them through the same checks, with `sha256` and `manifest` checked on the bytes in hand instead of a fetch. The two
environments never talk to each other: the admin's CLI carries the bytes, so neither needs a token for, or a network
path to, the other.

`uninstall` takes a version out (§6): refused while a flow of it is active or activating, or a run of it is queued or
running. Its row and its flows' rows stay, as its history does; its wheel is deleted unless another installed version
has the same bytes, and workers remove its environment once no installed version needs its lock.

At most `MAX_BUSY` installs and wheel fetches run at once, for each holds a worker thread while it fetches; one more
raises `Busy`, which the API answers 503 with Retry-After.
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
from feather_connect.packages.build import LOCK_FILE, WHEELS_NOTE
from feather_connect.packages.manifest import ManifestError
from feather_connect.packages.prepare import PREPARE_STEPS
from feather_connect.packages.signing import SignatureError, check_trusted_keys, verify_wheel
from feather_connect.packages.wheel import read_entry, read_wheel
from feather_connect.runtime.catalogue import restart_ticks, sync_package_catalog
from feather_connect.storage.port import (
    DuplicateVersion,
    FlowActivation,
    InUse,
    PackageVersion,
    Store,
    VersionNotReady,
)
from feather_server.auth import Actor
from feather_server.services.access import require
from feather_server.services.registries import BUILT_IN, Fetched, Registries, RegistryProblem, VersionExists

__all__ = [
    "CHECKS",
    "MAX_BUSY",
    "PREPARE_STEPS",
    "Busy",
    "InstallFailed",
    "NotReady",
    "Packages",
    "check_lock",
    "check_wheels",
]

DEV_ENV = "dev"  # the one environment a development build (`1.2.0.dev3`, of feather dev) installs in
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
    "signature",
    "compatibility",
    "lock",
    "wheels",
)
_PINNED = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*(\[[A-Za-z0-9._, -]+\])?==[A-Za-z0-9.+!_-]+(\s*;.*)?")
_HASH = re.compile(r"--hash=sha256:[0-9a-f]{64}")


class InstallFailed(ValueError):
    """An install stopped at `check`, one of `CHECKS`, for the reason in the message. Nothing was stored."""

    def __init__(self, check: str, message: str) -> None:
        super().__init__(f"{check}: {message}")
        self.check = check


class NotReady(Exception):
    """A flow of a version that isn't `ready` can't be activated: it is still being prepared on a worker, or that
    failed. `state` is the version's."""

    def __init__(self, message: str, state: str) -> None:
        super().__init__(message)
        self.state = state


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


def check_wheels(lock: bytes) -> None:
    """Every requirement of a lock has a wheel among its hashes, as the `# wheels:` comment under it says: a worker
    installs wheels only (`--only-binary :all:`), since building a source distribution runs its code. Raises
    `ValueError` naming the first requirement that has only a source distribution."""
    text = re.sub(r"\\\r?\n", " ", lock.decode("utf-8", "replace"))
    requirement: str | None = None
    hashes: set[str] = set()
    wheels: set[str] | None = None

    def close() -> None:
        if requirement is not None and not (wheels and wheels & hashes):
            raise ValueError(
                f"{requirement} has no wheel in the lock, only a source distribution: a worker installs wheels "
                "only, so publish a wheel of it, or pin a version that has one"
            )

    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith(WHEELS_NOTE):
            wheels = set(line.removeprefix(WHEELS_NOTE).split())
            continue
        if not line or line.startswith("#"):
            continue
        close()
        parts = line.split(" #", 1)[0].split()
        requirement = parts[0]
        hashes = {p.removeprefix("--hash=") for p in parts if p.startswith("--hash=")}
        wheels = None
    close()


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
    signed_by: str | None = None  # the id of the trusted key that signed it, where signing is required


@dataclass(frozen=True)
class Exported:
    """A `ready` version's stored wheel, for a promotion to another environment."""

    name: str
    version: str
    filename: str
    sha256: str
    content: bytes


class Packages:
    """The installed packages of one store's tenant, for the environment `env`."""

    def __init__(self, store: Store, clock: Clock, registries: Registries, *, env: str) -> None:
        self._store = store
        self._clock = clock
        self._registries = registries
        self._env = env
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
        return self._checked_and_kept(actor, fetched, project, wanted, reason, request, passed)

    def _checked_and_kept(
        self,
        actor: Actor,
        fetched: Fetched,
        project: str,
        wanted: str,
        reason: str | None,
        request: dict[str, Any],
        passed: list[str],
        *,
        promoted_from: str | None = None,
        before_keep: Callable[[], str] | None = None,
    ) -> Installed:
        """The checks after the fetch, from `signature` on, then the version stored with its flows inactive.
        `before_keep` runs only once every check has passed, just before the version is stored, and says which
        registry it is kept in: a promotion's copy into the built-in registry."""
        if Version(wanted).is_devrelease and self._env != DEV_ENV:
            raise InstallFailed(
                "name_version",
                f"{project} {wanted} is a development build of feather dev: it installs only in the {DEV_ENV} "
                f"environment. Build a release, such as {Version(wanted).base_version}, to install it here",
            )
        signed_by = self._check_signature(fetched.content)
        if signed_by is not None:
            request["signed_by"] = signed_by
        passed.append("signature")
        manifest = fetched.manifest
        try:
            m.check_fc_versions(manifest["fc_versions"])
        except ManifestError as error:
            raise InstallFailed("compatibility", str(error)) from None
        passed.append("compatibility")
        self._check_lock(fetched)
        passed += ["lock", "wheels"]
        registry = fetched.registry if before_keep is None else before_keep()
        named = cast(list[dict[str, Any]], manifest.get("flows") or [])
        flows = [f"{project}/{flow}" for flow in sorted(str(f["name"]) for f in named)]
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
            "preparing",
            {},
            actor.name,
            self._clock.now(),
            tuple(passed),
            promoted_from,
            actor.name if promoted_from else None,
            signed_by,
        )
        try:
            self._store.packages.install(row, fetched.content, flows, reason)
        except DuplicateVersion:  # installed meanwhile, by another request
            raise InstallFailed("not_installed", f"{project} {wanted} is installed here already") from None
        return Installed(
            project, row.version, row.sha256, registry, row.state, flows, passed, row.load_check, signed_by
        )

    def signing(self) -> tuple[bool, list[str]]:
        """Whether this environment takes only signed packages, and the ids of the keys it trusts."""
        settings = self._settings()
        try:
            keys = sorted(check_trusted_keys(settings.get("trusted_package_keys", [])))
        except SignatureError:
            keys = []
        return settings.get("require_signed_packages") is True, keys

    def _settings(self) -> dict[str, Any]:
        return {e.name: e.value for e in self._store.env_config.entries(self._env, "setting")}

    def _check_signature(self, content: bytes) -> str | None:
        """Where the environment requires signed packages, the id of the trusted key that signed the wheel; None where
        it doesn't. Raises `InstallFailed` at `signature`."""
        settings = self._settings()
        if settings.get("require_signed_packages") is not True:
            return None
        try:
            return verify_wheel(content, check_trusted_keys(settings.get("trusted_package_keys", [])))
        except SignatureError as error:
            raise InstallFailed("signature", str(error)) from None

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
        try:
            check_wheels(lock)
        except ValueError as error:
            raise InstallFailed("wheels", f"{where}: {error}") from None

    # Promoting (§6, task 10): another environment's tested wheel, byte for byte.

    def export(self, actor: Actor, name: str, version: str) -> Exported:
        """A version's stored wheel, to promote to another environment: only a `ready` one, which a worker here built
        and load-checked. An admin's (`packages.promote`), audited. Raises `KeyError` when it isn't installed and
        `NotReady` when it isn't ready."""
        project = canonicalize_name(name)
        target = f"package:{project}"
        require(actor, "packages.promote", self._store.audit, target)
        row = self._store.packages.version(project, version)
        if row is None or row.state == "uninstalled":
            raise KeyError(f"{project} {version} is not installed here")
        if row.state != "ready":
            raise NotReady(
                f"{project} {row.version} is {row.state} here: only a version that is ready, built and "
                "load-checked, can be promoted",
                row.state,
            )
        content = self._store.packages.wheel(row.sha256)
        if content is None:
            raise KeyError(f"the wheel of {project} {row.version} is not kept here")
        self._audit(actor, "export_package", target, {"version": row.version, "sha256": row.sha256}, "ok")
        return Exported(project, row.version, row.filename, row.sha256, content)

    def promote(
        self,
        actor: Actor,
        filename: str,
        content: bytes,
        *,
        sha256: str,
        source: str,
        reason: str | None = None,
        keep_in_registry: bool = True,
    ) -> Installed:
        """Install here the wheel that environment `source` exported, which must hash to `sha256`, the digest the
        source gave: so this environment runs exactly the bytes tested there. A copy goes in the built-in registry
        when it is on. An admin's (`packages.promote`), audited. Raises `InstallFailed` naming the check that
        failed."""
        target = f"package:{filename}"
        require(actor, "packages.promote", self._store.audit, target)
        request: dict[str, Any] = {"filename": filename, "sha256": sha256, "from": source, "reason": reason}
        with self._slot():
            try:
                installed = self._promote(actor, filename, content, sha256, reason, request, keep_in_registry)
            except InstallFailed as error:
                self._audit(actor, "promote_package", target, request, f"failed: {redact(str(error))}")
                raise
            except Exception:
                self._audit(actor, "promote_package", target, request, "failed: internal")
                raise
        self._audit(actor, "promote_package", f"package:{installed.name}", request, "ok")
        return installed

    def _promote(
        self,
        actor: Actor,
        filename: str,
        content: bytes,
        sha256: str,
        reason: str | None,
        request: dict[str, Any],
        keep_in_registry: bool = True,
    ) -> Installed:
        passed: list[str] = []
        actual = hashlib.sha256(content).hexdigest()
        if actual != sha256.lower():
            raise InstallFailed(
                "sha256", f"the wheel's sha256 is {actual}, not the {sha256} the source environment gave"
            )
        try:
            info = read_wheel(filename, content)
        except ManifestError as error:
            raise InstallFailed("manifest", str(error)) from None
        project = canonicalize_name(info.name)
        request["version"] = info.version
        if self._store.packages.version(project, info.version) is not None:
            raise InstallFailed(
                "not_installed", f"{project} {info.version} is installed here already: install a new version"
            )
        passed += ["not_installed", "size", "sha256", "manifest", "name_version"]
        fetched = Fetched("promotion", filename, actual, content, dict(info.manifest))
        return self._checked_and_kept(
            actor,
            fetched,
            project,
            info.version,
            reason,
            request,
            passed,
            promoted_from=str(request["from"]),
            before_keep=(  # once every check passed
                (lambda: self._registry_copy(actor, filename, content, actual))
                if keep_in_registry
                else (lambda: "promotion")
            ),
        )

    def _registry_copy(self, actor: Actor, filename: str, content: bytes, sha256: str) -> str:
        """Keep the promoted wheel in the built-in registry, when it is on, so this environment has it as any other
        package. A registry that has other bytes of the version refuses it at `sha256`. Returns where the version came
        from: the registry's name, or `promotion`."""
        if not self._registries.enabled:
            return "promotion"
        try:
            self._registries.publish(actor, filename, content, sha256)
        except VersionExists:
            try:
                kept, _ = self._registries.content(filename)
            except KeyError:
                kept = None
            if kept is None or kept.sha256 != sha256:
                raise InstallFailed(
                    "sha256",
                    f"the built-in registry has other bytes of {filename}: a version always means the same code",
                ) from None
        return BUILT_IN

    # Uninstalling (§6).

    def uninstall(
        self, actor: Actor, name: str, version: str, *, reason: str | None = None
    ) -> dict[str, Any]:
        """Take a version out of this environment, audited: refused (`InUse`) while a flow of it is active or
        activating, or a run of it is queued or running. Its history stays; its wheel goes unless another installed
        version has the same bytes. Raises `KeyError` when it isn't installed."""
        project = canonicalize_name(name)
        target = f"package:{project}"
        require(actor, "packages.uninstall", self._store.audit, target)
        request: dict[str, Any] = {"version": version, "reason": reason}
        try:
            row = self._store.packages.version(project, version)
            if row is None or row.state == "uninstalled":
                raise KeyError(f"{project} {version} is not installed here")
            request["version"] = row.version
            if (project, row.version) in self._store.runs.tagged_versions(("queued", "running")):
                raise InUse(
                    f"{project} {row.version} has queued or running runs: let them finish, or cancel them"
                )
            deleted = self._store.packages.uninstall(
                project, row.version, actor.name, self._clock.now(), reason
            )
        except (KeyError, InUse) as error:
            text = error.args[0] if error.args else str(error)
            self._audit(actor, "uninstall_package", target, request, f"refused: {redact(str(text))}")
            raise
        request["wheel_deleted"] = deleted
        self._audit(actor, "uninstall_package", target, request, "ok")
        return {"name": project, "version": row.version, "state": "uninstalled", "wheel_deleted": deleted}

    # Activating (§6): each worker supervisor starts a host for an active version, and drains it when no flow of it is
    # active and its runs are over.

    def activate(self, actor: Actor, flow: str, version: str, *, reason: str | None = None) -> dict[str, Any]:
        """Make `version` the active one of `flow` (`<package>/<name>`), audited. The scheduler fires its triggers
        from its next tick, with no restart. Raises `KeyError` when the version or the flow isn't installed."""
        target = f"flow:{flow}"
        require(actor, "flows.activate", self._store.audit, target)
        package = canonicalize_name(flow.split("/", 1)[0])
        installed = self._store.packages.version(package, version)
        if installed is None:
            raise KeyError(f"{package} {version} is not installed here")
        if installed.state != "ready":
            why = (
                "a worker is still preparing it: building its environment and load-checking it"
                if installed.state in ("installed", "preparing")
                else f"it is {installed.state}: {installed.load_check.get('error') or 'see its load check'}"
            )
            raise NotReady(f"{package} {installed.version} can't be activated yet: {why}", installed.state)
        self._signed_still(installed)
        now = self._clock.now()
        try:
            done = self._store.packages.activate(flow, package, installed.version, actor.name, now, reason)
        except VersionNotReady as error:  # uninstalled meanwhile: the store checks in its transaction
            raise NotReady(str(error), error.state) from None
        spec = cast(dict[str, Any], installed.load_check.get("specs") or {}).get(flow)
        if spec is not None and done.state == "activating":
            restart_ticks(self._store, flow, spec, now)  # no catch-up of the time it was inactive
        self._sync(package, installed)
        request = {"version": installed.version, "reason": reason}
        self._audit(actor, "activate_flow", target, request, "ok")
        return self._activation(done)

    def _signed_still(self, installed: PackageVersion) -> None:
        """Where the environment requires signed packages, a version installed before that was turned on, or since
        its keys changed, is checked again before a flow of it goes into service. Raises `InstallFailed` at
        `signature`."""
        if self._settings().get("require_signed_packages") is not True:
            return
        content = self._store.packages.wheel(installed.sha256)
        if content is None:
            raise InstallFailed(
                "signature", f"the wheel of {installed.name} {installed.version} is not kept here"
            )
        self._check_signature(content)

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
        sync_package_catalog(self._store, package, version)

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
            "checks": list(row.checks),
            "promoted_from": row.promoted_from,
            "promoted_by": row.promoted_by,
            "signed_by": row.signed_by,
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
            "last_active_at": None if row.last_active_at is None else row.last_active_at.isoformat(),
        }

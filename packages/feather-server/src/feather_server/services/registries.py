"""Package registries (flow packages §4.3): the built-in one that feather-server serves, and the other Python package
indexes an environment trusts. For the dashboard, the API, the registry's own routes and the CLI alike.

The built-in registry is called `feather`. It keeps its wheels in Postgres, one per version, and never replaces one:
a version is yanked instead. An upload needs an admin, or a token with the `publish` scope; reading needs any valid
token. Another registry is added by an admin, with its token as a secret reference that is resolved only when the
registry is called. Everything is of the store's tenant (ADR-0013).
"""

import hashlib
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from packaging.utils import canonicalize_name
from packaging.version import Version

from feather_connect.clock import Clock
from feather_connect.logs import redact
from feather_connect.packages.index import IndexReadError, SimpleIndex
from feather_connect.packages.manifest import ManifestError
from feather_connect.packages.wheel import MAX_WHEEL, read_wheel
from feather_connect.project import ProjectError, is_secret_ref, resolve_secret
from feather_connect.storage.port import DuplicateFile, RegistryFile, RegistryRow, Store
from feather_server.auth import Actor
from feather_server.services.access import Capability, require, require_reason

BUILT_IN = "feather"
_REGISTRY_NAME = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")
_MAX_FOUND = 50  # packages a search returns with their versions
# A registry's token is read from the environment or a file; the other secret providers arrive in M3.
_TOKEN_REF = re.compile(r"^\$\{secret:(env|file):[^{}]+\}$")
_LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")  # where plain http is allowed, for a test index


class RegistryProblem(ValueError):
    """A registry can't be used for what was asked: it is unknown, can't be reached, or serves no such package.
    `check` names the install check it fails (flow packages §5): `fetch`, `size`, `sha256`, `manifest` or
    `name_version`."""

    def __init__(self, message: str, check: str = "fetch") -> None:
        super().__init__(message)
        self.check = check


class VersionExists(Exception):
    """The built-in registry has that version already, and never replaces one."""


@dataclass(frozen=True)
class Registry:
    """A registry as the API shows it: never a token, only its reference."""

    name: str
    index_url: str
    token_ref: str | None
    built_in: bool
    added_by: str | None
    added_at: str | None


@dataclass(frozen=True)
class Found:
    """A package a search found, with its versions newest first and those yanked."""

    name: str
    versions: list[str]
    yanked: list[str]


@dataclass(frozen=True)
class Fetched:
    """A wheel fetched from a registry, its sha256 checked against the registry's and its manifest read."""

    registry: str
    filename: str
    sha256: str
    content: bytes
    manifest: dict[str, Any]
    yanked: bool = False  # as the registry says at the fetch, which an install refuses unless told otherwise


class Registries:
    """The registries of one store's tenant. `index_url` is the built-in registry's own address, to show."""

    def __init__(
        self, store: Store, clock: Clock, *, enabled: bool = True, index_url: str = "/registry/simple/"
    ):
        self._store = store
        self._clock = clock
        self.enabled = enabled
        self._index_url = index_url

    def _require(self, actor: Actor, capability: Capability, target: str) -> None:
        require(actor, capability, self._store.audit, target)

    def _audit(self, actor: Actor, action: str, target: str, request: dict[str, Any], result: str) -> None:
        self._store.audit.record(actor.kind, actor.name, action, target, request, result)

    # The built-in registry.

    def _built_in(self) -> None:
        if not self.enabled:
            raise RegistryProblem("the built-in registry is off here (FEATHER_REGISTRY=0)")

    def may_publish(self, actor: Actor) -> None:
        """Refuse an upload before its body is read, when the actor can't publish. Raises `Forbidden`."""
        self._built_in()
        self._require(actor, "packages.publish", "package:upload")

    def publish(self, actor: Actor, filename: str, content: bytes, sha256: str | None = None) -> RegistryFile:
        """Keep an uploaded wheel. It must be a flow package whose manifest matches its name; `sha256`, when the
        uploader sends it, must match the bytes. Raises `ManifestError` for what isn't a flow package, `ValueError`
        for a digest that doesn't match, and `VersionExists` for a version the registry has."""
        self._built_in()
        target = f"package:{filename}"
        self._require(actor, "packages.publish", target)
        request = {"filename": filename, "size": len(content)}
        try:
            info = read_wheel(filename, content)
            if sha256 and sha256.lower() != info.sha256:
                raise ValueError(f"the upload's sha256 is {info.sha256}, not the {sha256} sent with it")
            project = canonicalize_name(info.name)
            file = RegistryFile(
                project,
                info.version,
                filename,
                info.sha256,
                info.size,
                dict(info.manifest),
                actor.name,
                self._clock.now(),
            )
            self._store.registry.put_file(file, content)
        except DuplicateFile:
            self._audit(actor, "publish_package", target, request, "refused: the version exists")
            raise VersionExists(
                f"{filename}: this version is published already, and a version is never replaced; "
                "publish a new version number"
            ) from None
        except (ManifestError, ValueError) as error:
            self._audit(actor, "publish_package", target, request, f"failed: {redact(str(error))}")
            raise
        self._audit(actor, "publish_package", f"package:{project}", {**request, "sha256": info.sha256}, "ok")
        return file

    def projects(self) -> list[str]:
        self._built_in()
        return self._store.registry.projects()

    def files(self, project: str) -> list[RegistryFile]:
        self._built_in()
        return self._store.registry.files(canonicalize_name(project))

    def content(self, filename: str) -> tuple[RegistryFile, bytes]:
        """A wheel's record and bytes. Raises `KeyError` when there is no such file."""
        self._built_in()
        found = self._store.registry.file(filename)
        data = self._store.registry.content(filename)
        if found is None or data is None:
            raise KeyError(f"the registry has no file {filename!r}")
        return found, data

    def yank(self, actor: Actor, name: str, version: str, *, reason: str | None = None) -> None:
        """Yank a version of the built-in registry (PEP 592): installers pass over it unless asked for it by its exact
        version, and it is never deleted. Raises `KeyError` when there is no such version."""
        self._built_in()
        project = canonicalize_name(name)
        target = f"package:{project}"
        self._require(actor, "packages.yank", target)
        require_reason(actor, "packages.yank", reason)
        found = self._store.registry.yank(project, version, reason)
        self._audit(
            actor,
            "yank_package",
            target,
            {"version": version, "reason": reason},
            "ok" if found else "failed: none",
        )
        if not found:
            raise KeyError(f"the registry has no {project} {version}")

    # Every registry.

    def registries(self) -> list[Registry]:
        """The built-in registry, when it is on, and the ones added, by name."""
        found = [Registry(BUILT_IN, self._index_url, None, True, None, None)] if self.enabled else []
        return found + [self._shown(row) for row in self._store.registry.registries()]

    @staticmethod
    def _shown(row: RegistryRow) -> Registry:
        return Registry(row.name, row.index_url, row.token_ref, False, row.added_by, row.added_at.isoformat())

    def add(
        self,
        actor: Actor,
        name: str,
        index_url: str,
        token_ref: str | None = None,
        *,
        reason: str | None = None,
    ) -> Registry:
        """Trust another Python package index. Its token must be a secret reference such as `${secret:env:NAME}`,
        never the token itself. Raises `ValueError` for a bad name, URL or token, and when the name is taken."""
        target = f"registry:{name}"
        self._require(actor, "registries.manage", target)
        require_reason(actor, "registries.manage", reason)
        # Audited as given only when it is a reference: a token pasted in by mistake never reaches the audit.
        shown = token_ref if token_ref is None or is_secret_ref(token_ref) else "***"
        request = {"index_url": index_url, "token_ref": shown, "reason": reason}
        try:
            if name == BUILT_IN or not _REGISTRY_NAME.match(name) or len(name) > 64:
                raise ValueError(
                    f"{name!r} can't name a registry: use lower case words joined by '-', not feather"
                )
            parsed = urlparse(index_url)
            if (
                parsed.scheme not in ("http", "https")
                or not parsed.netloc
                or parsed.username
                or parsed.password
            ):
                raise ValueError("the index URL is http(s)://host/path/, with no credentials in it")
            if parsed.scheme == "http" and parsed.hostname not in _LOCAL_HOSTS:
                raise ValueError("the index URL must be https://: plain http is only for localhost")
            if token_ref is not None and not _TOKEN_REF.match(token_ref):
                raise ValueError(
                    "the token must be a secret reference, ${secret:env:NAME} or ${secret:file:PATH}, "
                    "never its value"
                )
            if self._store.registry.get_registry(name) is not None:
                raise ValueError(f"a registry named {name!r} exists already")
            row = self._store.registry.add_registry(name, index_url, token_ref, actor.name)
        except ValueError as error:
            self._audit(actor, "add_registry", target, request, f"failed: {error}")
            raise
        self._audit(actor, "add_registry", target, request, "ok")
        return self._shown(row)

    def remove(self, actor: Actor, name: str, *, reason: str | None = None) -> None:
        """Stop trusting a registry. The built-in one can only be turned off, with FEATHER_REGISTRY=0."""
        target = f"registry:{name}"
        self._require(actor, "registries.manage", target)
        require_reason(actor, "registries.manage", reason)
        if name == BUILT_IN:
            self._audit(actor, "remove_registry", target, {"reason": reason}, "failed: built in")
            raise ValueError("the built-in registry can't be removed: set FEATHER_REGISTRY=0 to turn it off")
        if not self._store.registry.remove_registry(name):
            raise KeyError(f"no registry named {name!r}")
        self._audit(actor, "remove_registry", target, {"reason": reason}, "ok")

    def _index(self, name: str) -> SimpleIndex:
        row = self._store.registry.get_registry(name)
        if row is None:
            raise KeyError(f"no registry named {name!r}")
        try:
            token = None if row.token_ref is None else resolve_secret(row.token_ref)
        except ProjectError as error:
            raise RegistryProblem(f"registry {name}: {error}") from None
        return SimpleIndex(row.index_url, token)

    def test(self, actor: Actor, name: str) -> dict[str, Any]:
        """Read the registry's index with its token, and say whether that worked and how many projects it lists."""
        target = f"registry:{name}"
        self._require(actor, "registries.manage", target)
        if name == BUILT_IN:
            self._built_in()
            return {"registry": name, "ok": True, "projects": len(self.projects()), "message": "built in"}
        try:
            with self._index(name) as index:
                count = len(index.projects())
        except (IndexReadError, RegistryProblem) as error:
            result = {"registry": name, "ok": False, "projects": None, "message": str(error)}
        else:
            result = {"registry": name, "ok": True, "projects": count, "message": "the index answered"}
        self._audit(
            actor, "test_registry", target, {}, "ok" if result["ok"] else f"failed: {result['message']}"
        )
        return result

    def search(self, name: str, query: str = "") -> list[Found]:
        """The registry's packages whose name holds `query`, with their versions. Raises `KeyError` for an unknown
        registry and `RegistryProblem` for one that can't be read."""
        wanted = canonicalize_name(query) if query else ""
        if name == BUILT_IN:
            found: list[Found] = []
            for project in self.projects():
                if wanted not in project:
                    continue
                files = self._store.registry.files(project)
                found.append(
                    Found(
                        project,
                        sorted((f.version for f in files), key=Version, reverse=True),
                        [f.version for f in files if f.yanked_at is not None],
                    )
                )
            return found[:_MAX_FOUND]
        try:
            with self._index(name) as index:
                matches = [p for p in index.projects() if wanted in canonicalize_name(p)][:_MAX_FOUND]
                return [
                    Found(
                        canonicalize_name(p),
                        index.versions(p),
                        sorted({f.version for f in index.files(p) if f.yanked and f.version}),
                    )
                    for p in matches
                ]
        except IndexReadError as error:
            raise RegistryProblem(str(error)) from None

    def fetch(self, registry: str, name: str, version: str) -> Fetched:
        """A package's wheel by name and version, its sha256 checked against what the registry recorded, and its
        manifest read and checked. The first step of an install, whose caller checks the role. Raises `KeyError`
        when the registry or the version is unknown and `RegistryProblem` when the wheel can't be had or checked."""
        project = canonicalize_name(name)
        check = "fetch"
        try:
            if registry == BUILT_IN:
                self._built_in()
                files = [f for f in self._store.registry.files(project) if f.version == version]
                if not files:
                    raise KeyError(f"the registry has no {project} {version}")
                file, data = self.content(files[0].filename)
                filename, expected, yanked = file.filename, file.sha256, file.yanked_at is not None
            else:
                with self._index(registry) as index:
                    listed = index.find(project, version)
                    data = index.fetch(listed, MAX_WHEEL)
                filename, expected, yanked = listed.filename, listed.sha256 or "", listed.yanked is not False
            if len(data) > MAX_WHEEL:
                check = "size"
                raise ManifestError(
                    f"{filename} has {len(data)} bytes, more than the {MAX_WHEEL} a package may have"
                )
            actual = hashlib.sha256(data).hexdigest()
            if not expected or actual != expected.lower():
                raise RegistryProblem(
                    f"{registry}: {filename} has sha256 {actual}, not the {expected or 'none'} recorded",
                    "sha256",
                )
            check = "manifest"
            info = read_wheel(filename, data)
        except IndexReadError as error:
            raise RegistryProblem(f"{registry}: {error}", error.check or check) from None
        except ManifestError as error:
            raise RegistryProblem(f"{registry}: {error}", check) from None
        if info.version != version or canonicalize_name(info.name) != project:
            raise RegistryProblem(f"{registry}: {filename} holds {info.name} {info.version}", "name_version")
        return Fetched(registry, filename, info.sha256, data, dict(info.manifest), yanked)

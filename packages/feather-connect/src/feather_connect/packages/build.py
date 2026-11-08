"""`feather package build`: a project in a customer's repository, made into a flow package's wheel (design §4).

The wheel holds the M1 project layout under the package's import name, the manifest beside it, the locked requirements
in its metadata, and an entry point in `feather_connect.packages`. The build is reproducible: the files go in in a fixed
order, with a fixed time and mode, and the manifest's time is the commit's (or `SOURCE_DATE_EPOCH`), so building the
same tree twice gives the same sha256.
"""

import base64
import hashlib
import os
import shutil
import subprocess
import sys
import tomllib
import zipfile
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import yaml
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from feather_connect.packages import manifest as m
from feather_connect.packages.manifest import ManifestError
from feather_connect.project import ProjectError, load_project, validate_project

PROJECT_DIRS = (
    "connectors",
    "env",
    "fixtures",
    "flows",
    "templates",
    "tests",
)  # what a wheel carries of a project
LOCK_FILE = (
    "feather-lock.txt"  # in the wheel's .dist-info: the requirements exported from uv.lock, with hashes
)
_ZIP_TIME = (1980, 1, 1, 0, 0, 0)  # the earliest a zip can say
_SKIPPED = ("__pycache__",)
_SKIPPED_SUFFIXES = (".pyc", ".pyo")
_TAG = "py3-none-any"


@dataclass(frozen=True)
class Built:
    """A wheel that `build_package` wrote."""

    path: Path
    sha256: str
    size: int
    manifest: Mapping[str, Any]


def wheel_name(name: str, version: str) -> str:
    return f"{m.module_of(name)}-{version}-{_TAG}.whl"


def build_package(root: Path, out_dir: Path | None = None, *, env: str = "dev") -> Built:
    """Build the project in `root` into `out_dir` (`root/dist` by default). Checks the project as `feather validate
    --env <env>` does first. Raises `ManifestError` saying what to fix."""
    root = root.resolve()
    config = _feather_yaml(root)
    name = m.check_name(config.get("name"))
    if "version" not in config:
        raise ManifestError("feather.yaml: version: is required to build a package, such as version: 1.0.0")
    version = m.check_version(config["version"])
    fc_versions = m.check_fc_versions(config.get("fc_versions") or m.default_fc_versions())
    dependencies = _dependencies(root, name, version)
    lock = _lock(root, dependencies)
    described = _validated(root, env)
    built_at, commit = _provenance(root)
    manifest = m.validate(
        {
            "schema": m.SCHEMA_VERSION,
            "name": name,
            "version": version,
            "module": m.module_of(name),
            "fc_versions": fc_versions,
            **described,
            "dependencies": dependencies,
            "lock_hash": hashlib.sha256(lock).hexdigest(),
            "built_at": built_at,
            "commit": commit,
        }
    )
    files = _project_files(root, config)
    target = (out_dir or root / "dist").resolve()
    target.mkdir(parents=True, exist_ok=True)
    path = target / wheel_name(name, version)
    _write_wheel(path, manifest, files, lock, dependencies)
    data = path.read_bytes()
    return Built(path, hashlib.sha256(data).hexdigest(), len(data), manifest)


# What goes in.


def _feather_yaml(root: Path) -> dict[str, Any]:
    path = root / "feather.yaml"
    if not path.is_file():
        raise ManifestError(f"no feather.yaml in {root}: run it from the project directory")
    try:
        found = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        raise ManifestError(f"feather.yaml: can't read it ({error})") from None
    if not isinstance(found, dict):
        raise ManifestError("feather.yaml: must be a mapping")
    return {str(k): v for k, v in cast(dict[Any, Any], found).items()}


def _dependencies(root: Path, name: str, version: str) -> list[str]:
    """The package's own requirements from pyproject.toml, without Feather Connect, whose range is `fc_versions`."""
    path = root / "pyproject.toml"
    if not path.is_file():
        return []
    try:
        project = cast(dict[str, Any], tomllib.loads(path.read_text(encoding="utf-8")).get("project", {}))
    except tomllib.TOMLDecodeError as error:
        raise ManifestError(f"pyproject.toml: can't read it ({error})") from None
    if "name" in project and canonicalize_name(str(project["name"])) != canonicalize_name(name):
        raise ManifestError(f"pyproject.toml names {project['name']!r}, but feather.yaml names {name!r}")
    if "version" in project and str(project["version"]) != version:
        raise ManifestError(
            f"pyproject.toml says version {project['version']}, but feather.yaml says {version}"
        )
    found: list[str] = []
    for raw in cast(list[Any], project.get("dependencies", [])):
        try:
            requirement = Requirement(str(raw))
        except InvalidRequirement as error:
            raise ManifestError(f"pyproject.toml: dependency {raw!r} is not valid ({error})") from None
        if canonicalize_name(requirement.name) != "feather-connect":
            found.append(str(requirement))
    return sorted(found)


def _lock(root: Path, dependencies: Sequence[str]) -> bytes:
    """The locked requirements, with hashes, exported from uv.lock. None to lock is fine; requirements without a lock
    are not, as an install must resolve exactly what was tested."""
    if not (root / "uv.lock").is_file():
        if dependencies:
            raise ManifestError("the package has dependencies but no uv.lock: run uv lock, then build again")
        return b""
    uv = shutil.which("uv")
    if uv is None:
        raise ManifestError("uv.lock needs uv to export it, and uv is not on the PATH")
    done = subprocess.run(
        [
            uv,
            "export",
            "--frozen",
            "--format",
            "requirements-txt",
            "--no-emit-project",
            "--no-dev",
            "--no-header",
        ],
        cwd=root,
        capture_output=True,
        timeout=120,
        check=False,
    )
    if done.returncode != 0:
        reason = done.stderr.decode("utf-8", "replace").strip().splitlines()
        raise ManifestError(f"uv export failed: {reason[-1] if reason else done.returncode}")
    return done.stdout.replace(b"\r\n", b"\n")


@contextmanager
def _modules_restored() -> Generator[None]:
    """Forget the project's modules once it is checked, so that a later build in this process imports them anew."""
    before = set(sys.modules)
    try:
        yield
    finally:
        for name in set(sys.modules) - before:
            del sys.modules[name]


def _validated(root: Path, env: str) -> dict[str, Any]:
    with _modules_restored():
        try:
            project = load_project(root, env)
        except ProjectError as error:
            raise ManifestError(f"the project doesn't load: {error}") from None
        errors = [p for p in validate_project(project) if p.severity == "error"]
        if errors:
            listed = "; ".join(f"{p.path}: {p.message}" for p in errors[:3])
            raise ManifestError(f"feather validate --env {env} finds {len(errors)} error(s): {listed}")
        return m.describe(project)


def _git(root: Path, *args: str) -> str | None:
    try:
        done = subprocess.run(
            ["git", *args], cwd=root, capture_output=True, text=True, timeout=10, check=True
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout.strip() or None


def _provenance(root: Path) -> tuple[str | None, str | None]:
    """When and from what it was built, in a way that doesn't change from one build of the same tree to the next."""
    epoch = os.environ.get("SOURCE_DATE_EPOCH") or _git(root, "log", "-1", "--format=%ct")
    built_at = None
    if epoch and epoch.isdigit():
        built_at = datetime.fromtimestamp(int(epoch), UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return built_at, _git(root, "rev-parse", "HEAD")


def _project_files(root: Path, config: Mapping[str, Any]) -> list[tuple[str, Path]]:
    """The project's files, by their path in the project: feather.yaml, the usual folders and the folders its
    `modules:` live in. Hidden files, caches and anything else (secrets/, .data/, dist/) stay out."""
    tops: set[str] = set(PROJECT_DIRS)
    for module in cast(list[Any], config.get("modules") or []):
        tops.add(str(module).split(".")[0])
    found = [("feather.yaml", root / "feather.yaml")]
    for top in sorted(tops):
        folder = root / top
        if not folder.is_dir():
            continue
        for path in folder.rglob("*"):
            rel = path.relative_to(root)
            if not path.is_file() or any(p.startswith(".") or p in _SKIPPED for p in rel.parts):
                continue
            if path.suffix in _SKIPPED_SUFFIXES:
                continue
            found.append((rel.as_posix(), path))
    return sorted(found)


# The wheel.


def _record_hash(data: bytes) -> str:
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()


def _write_wheel(
    path: Path,
    manifest: Mapping[str, Any],
    files: Sequence[tuple[str, Path]],
    lock: bytes,
    dependencies: Sequence[str],
) -> None:
    name, version, module = manifest["name"], manifest["version"], manifest["module"]
    info = f"{module}-{version}.dist-info"
    requires = [f"feather-connect{manifest['fc_versions']}", *dependencies]
    metadata = "".join(
        [
            "Metadata-Version: 2.1\n",
            f"Name: {name}\n",
            f"Version: {version}\n",
            f"Summary: The Feather Connect flow package {name}\n",
            "Requires-Python: >=3.12\n",
            *(f"Requires-Dist: {r}\n" for r in requires),
        ]
    )
    wheel = f"Wheel-Version: 1.0\nGenerator: feather-connect\nRoot-Is-Purelib: true\nTag: {_TAG}\n"
    entry_points = f"[{m.ENTRY_POINTS}]\n{name} = {module}\n"
    init = f'"""The flow package {name} {version}: its project, as feather package build made it."""\n'
    entries: list[tuple[str, bytes]] = [
        (f"{module}/__init__.py", init.encode()),
        (f"{module}/{m.MANIFEST}", m.dumps(manifest)),
        *((f"{module}/{rel}", source.read_bytes()) for rel, source in files),
    ]
    entries.sort(key=lambda e: e[0])
    entries += [
        (f"{info}/METADATA", metadata.encode()),
        (f"{info}/WHEEL", wheel.encode()),
        (f"{info}/entry_points.txt", entry_points.encode()),
        (f"{info}/{LOCK_FILE}", lock),
    ]
    record = "".join(f"{p},{_record_hash(d)},{len(d)}\n" for p, d in entries) + f"{info}/RECORD,,\n"
    entries.append((f"{info}/RECORD", record.encode()))
    partial = path.with_suffix(".whl.part")
    with zipfile.ZipFile(partial, "w") as archive:
        for arcname, data in entries:
            entry = zipfile.ZipInfo(arcname, date_time=_ZIP_TIME)
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.create_system = 3  # as on Unix, wherever it is built, so the bytes don't depend on the OS
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, data, compresslevel=9)
    partial.replace(path)

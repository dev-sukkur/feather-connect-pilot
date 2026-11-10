"""Dependency environments for flow packages (flow packages §5 step 3, task 8): one virtual environment per lock hash,
built with `uv` from the package's locked, hashed requirements, kept on the worker's disk and reused by every package
version with the same lock.

An environment lives at `<root>/<tenant>/<lock_hash>` (ADR-0013: what a process keeps is keyed by tenant). It is
built in a directory of its own and renamed into place when it is whole, with a marker file written last, so a build
that is killed leaves nothing that looks ready. `uv pip install --require-hashes --only-binary :all:` takes exactly the
wheels the lock names, and never builds a source distribution, which would run its code. It tries `--offline` first,
from uv's cache, and goes to the index only if that fails, so a worker that built a lock once builds it again with no
network. With no index named it uses none (`--no-index`). uv runs with a scrubbed environment (`uv_environment`): no
secret of the worker reaches it.

Feather Connect itself is not in a package's lock (`build.PLATFORM_DISTRIBUTIONS`): the environment sees the worker's
own site-packages through a `.pth` file, after its own, so a package's pinned version of a library wins over the
worker's in its own process, and two packages can pin two versions of one library side by side.

Several supervisors may share one cache, and what they share is guarded by locks on files (`filelock`): a build of a
lock hash holds `.locks/<hash>.lock`, so one process builds and the others wait and reuse it; a build's directory holds
`.locks/<build>.lock`, so only an abandoned one is cleaned up; and an environment in use, by a host or a load check,
holds a mark under `.users/<hash>/` (`use`), which no eviction passes.

Bounded: a uv command that runs past `timeout` is killed with its whole process tree, and the environments of a
tenant, with uv's cache (`.uv-cache`, `UV_CACHE_DIR`), are kept under `max_bytes`: the least recently used environment
not in use goes first, then uv prunes its cache. An environment is evicted by removing its marker first, so nothing
takes it for ready, then renaming it to `.trash-*`, and its files are deleted only if that rename worked. One that
can't fit is refused.
"""

import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Self, cast

from feather_connect.packages.filelock import FileLock, is_held
from feather_connect.packages.tree import ProcessTree

__all__ = [
    "BUILD_TIMEOUT",
    "MAX_BYTES",
    "EnvError",
    "EnvUse",
    "Environments",
    "python_of",
    "uv_environment",
    "worker_site_dirs",
]

log = logging.getLogger(__name__)

BUILD_TIMEOUT = 300.0  # seconds for one uv command
MAX_BYTES = 5 * 1024**3  # the environments of a tenant, with uv's cache, on one worker
_MARKER = "feather-env.json"
_PTH = "_feather_platform.pth"
_ERROR_TAIL = 1500  # characters of uv's output put in an error
_UV_CACHE = ".uv-cache"
_LOCKS = ".locks"
_USERS = ".users"
_INDEXES = ("--index-url", "-i", "--default-index", "--extra-index-url", "--index")
# What uv gets of the worker's environment: what it needs to run and to reach an index, never a secret of the worker.
_UV_KEPT = (
    "PATH",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "PATHEXT",
    "LANG",
    "LC_ALL",
    "TZ",
    "HOME",
    "USERPROFILE",
    "LOCALAPPDATA",
    "APPDATA",
    "TEMP",
    "TMP",
    "TMPDIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "ALL_PROXY",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
)


class EnvError(Exception):
    """An environment could not be built, or is not allowed: the message says why."""


def python_of(env: Path) -> Path:
    """The Python of a virtual environment."""
    return env / "Scripts" / "python.exe" if sys.platform == "win32" else env / "bin" / "python"


def worker_site_dirs() -> list[str]:
    """This process's site-packages, where Feather Connect and its connectors are installed."""
    return list(dict.fromkeys(p for p in sys.path if Path(p).name == "site-packages" and Path(p).is_dir()))


def uv_environment(cache_dir: Path) -> dict[str, str]:
    """The environment uv runs with: the variables of `_UV_KEPT` that are set, and uv's own settings."""
    kept = {k: v for k, v in os.environ.items() if k.upper() in _UV_KEPT}
    return kept | {
        "UV_CACHE_DIR": str(cache_dir),
        "UV_NO_PROGRESS": "1",
        "UV_PYTHON_DOWNLOADS": "never",
        "UV_NO_CONFIG": "1",
    }


@dataclass(frozen=True)
class _Built:
    path: Path
    used_at: float
    size: int


class EnvUse:
    """An environment held in use: no eviction, here or in another process, removes it until `release`."""

    def __init__(self, python: Path, mark: FileLock) -> None:
        self.python = python
        self._mark = mark

    def release(self) -> None:
        self._mark.release()
        try:
            self._mark.path.unlink()
        except OSError:
            pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


class Environments:
    """The dependency environments of one tenant on this worker, under `root`.

    `uv` is the uv binary (by default the one on the PATH, or `FEATHER_UV`), `index` the arguments that say where
    requirements come from (such as `["--index-url", url]` or `["--find-links", dir]`; with no index named, none is
    used), and `site_dirs` what the environments see of the worker's own packages.
    """

    def __init__(
        self,
        root: Path,
        *,
        tenant: str = "default",
        uv: str | None = None,
        index: Sequence[str] = (),
        timeout: float = BUILD_TIMEOUT,
        max_bytes: int = MAX_BYTES,
        base_python: str = sys.executable,
        site_dirs: Sequence[str] | None = None,
    ) -> None:
        self._root = (root / tenant).resolve()
        self._uv = uv or os.environ.get("FEATHER_UV") or shutil.which("uv")
        options = list(index)
        if "--no-index" not in options and not any(a in _INDEXES for a in options):
            options.append("--no-index")
        self._index = options
        self._timeout = timeout
        self._max_bytes = max_bytes
        self._base_python = base_python
        self._site_dirs = list(worker_site_dirs() if site_dirs is None else site_dirs)
        self._guard = threading.Lock()
        self._building: dict[str, threading.Lock] = {}

    @property
    def root(self) -> Path:
        return self._root

    @property
    def uv_cache(self) -> Path:
        return self._root / _UV_CACHE

    def path(self, lock_hash: str) -> Path:
        return self._root / _checked_hash(lock_hash)

    def python(self, lock_hash: str) -> Path | None:
        """The Python of the environment for `lock_hash`, if it is built here."""
        env = self.path(lock_hash)
        return python_of(env) if (env / _MARKER).is_file() else None

    def in_use(self, lock_hash: str) -> bool:
        """Whether a process holds the environment in use (`use`). Marks left by processes that ended are cleared."""
        users = self._root / _USERS / _checked_hash(lock_hash)
        held = False
        for mark in list(users.glob("*.lock")) if users.is_dir() else []:
            if is_held(mark):
                held = True
                continue
            try:
                mark.unlink()
            except OSError:
                pass
        return held

    def ensure(self, lock: bytes, lock_hash: str, *, keep: Iterable[str] = ()) -> Path:
        """The Python of the environment for the lock, built first if it isn't. `keep` names lock hashes that making
        room never removes, besides those in use. Raises `EnvError`."""
        with self.use(lock, lock_hash, keep=keep) as found:
            return found.python

    def use(self, lock: bytes, lock_hash: str, *, keep: Iterable[str] = ()) -> EnvUse:
        """The environment for the lock, built first if it isn't, and held in use until the result is released. One
        build of a lock hash runs at a time, in this process and in any other sharing the cache. Raises `EnvError`."""
        if hashlib.sha256(lock).hexdigest() != lock_hash:
            raise EnvError("the lock is not the one its lock hash names")
        with self._guard:
            local = self._building.setdefault(lock_hash, threading.Lock())
        with local:
            shared = FileLock(self._root / _LOCKS / f"{_checked_hash(lock_hash)}.lock")
            if not shared.acquire(self._timeout * 3):
                raise EnvError(f"another process has been building lock {lock_hash[:12]} for too long")
            try:
                mark = FileLock(
                    self._root / _USERS / lock_hash / f"{os.getpid()}-{uuid.uuid4().hex[:8]}.lock"
                )
                mark.try_acquire()
                try:
                    found = self.python(lock_hash)
                    if found is not None:
                        os.utime(self.path(lock_hash) / _MARKER)  # used: the last to be removed for room
                    else:
                        found = self._build(lock, lock_hash, {lock_hash, *keep})
                except BaseException:
                    EnvUse(Path(), mark).release()
                    raise
                return EnvUse(found, mark)
            finally:
                shared.release()

    def remove(self, lock_hash: str) -> None:
        self._evict(self.path(lock_hash))

    def _build(self, lock: bytes, lock_hash: str, keep: set[str]) -> Path:
        if self._uv is None:
            raise EnvError("uv is not on the PATH: a worker needs it to build a package's environment")
        self._root.mkdir(parents=True, exist_ok=True)
        self._make_room(keep)
        target = self.path(lock_hash)
        if target.exists() and not self._evict(target):  # one left unmarked while a process had it open
            raise EnvError(
                f"the old environment of lock {lock_hash[:12]} can't be moved away: it is still open"
            )
        name = f".build-{lock_hash[:12]}-{uuid.uuid4().hex[:8]}"
        work = self._root / name
        building = FileLock(self._root / _LOCKS / f"{name}.lock")
        building.try_acquire()
        started = time.monotonic()
        try:
            self._run(
                [
                    self._uv,
                    "venv",
                    str(work),
                    "--python",
                    self._base_python,
                    "--no-project",
                    "--no-config",
                    "--relocatable",
                    "--quiet",
                ]
            )
            if lock.strip():
                requirements = work / "requirements.txt"
                requirements.write_bytes(lock)
                install = [
                    self._uv,
                    "pip",
                    "install",
                    "--python",
                    str(python_of(work)),
                    "--require-hashes",
                    "--only-binary",
                    ":all:",
                    "--no-config",
                    "--quiet",
                    *self._index,
                    "-r",
                    str(requirements),
                ]
                try:
                    self._run([*install, "--offline"])
                except EnvError:
                    self._run(install)  # not all in uv's cache: from the index
            self._platform(work)
            (work / _MARKER).write_text(
                json.dumps({"lock_hash": lock_hash, "seconds": round(time.monotonic() - started, 3)}),
                encoding="utf-8",
            )
            try:
                work.rename(target)
            except OSError:
                if not (target / _MARKER).is_file():
                    raise
        finally:
            shutil.rmtree(work, ignore_errors=True)
            building.release()
            try:
                building.path.unlink()
            except OSError:
                pass
        if self._size() > self._max_bytes:
            self._make_room(keep)
            if self._size() > self._max_bytes:
                self._evict(target)
                raise EnvError(
                    f"the environment doesn't fit: the environments here may take {self._max_bytes} bytes"
                )
        log.info("built the environment of lock %s in %.1f s", lock_hash[:12], time.monotonic() - started)
        return python_of(target)

    def _platform(self, env: Path) -> None:
        """Let the environment see the worker's own packages, after its own."""
        found = list(env.glob("lib/python*/site-packages")) + list(env.glob("Lib/site-packages"))
        if not found:
            raise EnvError("the new environment has no site-packages")
        lines = [f"import site; site.addsitedir({d!r})" for d in self._site_dirs]
        (found[0] / _PTH).write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _run(self, command: list[str]) -> None:
        tree = ProcessTree(
            command,
            cwd=self._root,
            env=uv_environment(self.uv_cache),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        try:
            try:
                output, _ = tree.process.communicate(timeout=self._timeout)
            except subprocess.TimeoutExpired:
                raise EnvError(f"uv ran past {self._timeout:g} s and was stopped") from None
        finally:
            tree.kill()
        if tree.process.returncode != 0:
            text = cast(bytes, output or b"").decode("utf-8", "replace").strip()[-_ERROR_TAIL:]
            raise EnvError(f"uv failed (exit {tree.process.returncode}): {text}")

    def _built(self) -> list[_Built]:
        found: list[_Built] = []
        if not self._root.is_dir():
            return found
        for env in self._root.iterdir():
            marker = env / _MARKER
            if env.is_dir() and marker.is_file():
                found.append(_Built(env, marker.stat().st_mtime, _tree_size(env)))
        return found

    def _size(self) -> int:
        return sum(b.size for b in self._built()) + _tree_size(self.uv_cache)

    def _evict(self, env: Path) -> bool:
        """Remove an environment: its marker first, so nothing takes it for ready, then a rename out of the way, and
        only if that worked, its files. Returns whether it is gone."""
        try:
            (env / _MARKER).unlink(missing_ok=True)
        except OSError:
            return False
        trash = self._root / f".trash-{uuid.uuid4().hex[:12]}"
        try:
            env.rename(trash)
        except FileNotFoundError:
            return True
        except (
            OSError
        ):  # open in a process, on Windows: it stays, unmarked, and is moved when next built or evicted
            log.warning("the environment %s is still open: it is left without its marker", env.name[:12])
            return False
        shutil.rmtree(trash, ignore_errors=True)
        return True

    def _make_room(self, keep: set[str]) -> None:
        """Remove what builds and evictions left, then the least recently used environments that aren't in `keep` or
        in use until they fit under the cap, then what uv's cache doesn't need."""
        for stale in self._root.glob(".build-*"):
            if not is_held(self._root / _LOCKS / f"{stale.name}.lock"):
                shutil.rmtree(stale, ignore_errors=True)
        for trash in self._root.glob(".trash-*"):
            shutil.rmtree(trash, ignore_errors=True)
        built = sorted(self._built(), key=lambda b: b.used_at)
        total = sum(b.size for b in built) + _tree_size(self.uv_cache)
        for env in built:
            if total <= self._max_bytes:
                return
            if env.path.name in keep or self.in_use(env.path.name):
                continue
            shared = FileLock(self._root / _LOCKS / f"{env.path.name}.lock")
            if not shared.try_acquire():  # being built, or taken into use, now
                continue
            try:
                if self.in_use(env.path.name) or not self._evict(env.path):
                    continue
            finally:
                shared.release()
            total -= env.size
            log.info("removed the environment of lock %s to make room", env.path.name[:12])
        if total > self._max_bytes and self._uv is not None and self.uv_cache.is_dir():
            try:
                self._run([self._uv, "cache", "prune", "--cache-dir", str(self.uv_cache), "--quiet"])
            except EnvError:
                log.warning("uv could not prune its cache", exc_info=True)


def _checked_hash(lock_hash: str) -> str:
    if len(lock_hash) != 64 or any(c not in "0123456789abcdef" for c in lock_hash):
        raise EnvError(f"{lock_hash!r} is not a lock hash")
    return lock_hash


def _tree_size(path: Path) -> int:
    total = 0
    for dirpath, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                pass
    return total

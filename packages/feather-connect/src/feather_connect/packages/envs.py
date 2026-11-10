"""Dependency environments for flow packages (flow packages §5 step 3, task 8): one virtual environment per lock hash,
built with `uv` from the package's locked, hashed requirements, kept on the worker's disk and reused by every package
version with the same lock.

An environment lives at `<root>/<tenant>/<lock_hash>` (ADR-0013: what a process keeps is keyed by tenant). It is
built in a directory of its own and renamed into place when it is whole, with a marker file written last, so a build
that is killed leaves nothing that looks ready. `uv pip install --require-hashes` takes exactly the files the lock
names. It tries `--offline` first, from uv's cache, and goes to the index only if that fails, so a worker that built a
lock once builds it again with no network.

Feather Connect itself is not in a package's lock (`build.PLATFORM_DISTRIBUTIONS`): the environment sees the worker's
own site-packages through a `.pth` file, after its own, so a package's pinned version of a library wins over the
worker's in its own process, and two packages can pin two versions of one library side by side.

Bounded: a build that runs past `timeout` is killed with its whole process tree, and the environments of a tenant are
kept under `max_bytes`, the least recently used going first; one that can't fit is refused.
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
from typing import cast

from feather_connect.packages.tree import ProcessTree

__all__ = ["BUILD_TIMEOUT", "MAX_BYTES", "EnvError", "Environments", "python_of", "worker_site_dirs"]

log = logging.getLogger(__name__)

BUILD_TIMEOUT = 300.0  # seconds for one uv command
MAX_BYTES = 5 * 1024**3  # the environments of a tenant, on one worker
_MARKER = "feather-env.json"
_PTH = "_feather_platform.pth"
_ERROR_TAIL = 1500  # characters of uv's output put in an error


class EnvError(Exception):
    """An environment could not be built, or is not allowed: the message says why."""


def python_of(env: Path) -> Path:
    """The Python of a virtual environment."""
    return env / "Scripts" / "python.exe" if sys.platform == "win32" else env / "bin" / "python"


def worker_site_dirs() -> list[str]:
    """This process's site-packages, where Feather Connect and its connectors are installed."""
    return list(dict.fromkeys(p for p in sys.path if Path(p).name == "site-packages" and Path(p).is_dir()))


@dataclass(frozen=True)
class _Built:
    path: Path
    used_at: float
    size: int


class Environments:
    """The dependency environments of one tenant on this worker, under `root`.

    `uv` is the uv binary (by default the one on the PATH, or `FEATHER_UV`), `index` the arguments that say where
    requirements come from (such as `["--index-url", url]` or `["--no-index", "--find-links", dir]`), and
    `site_dirs` what the environments see of the worker's own packages.
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
        self._index = list(index)
        self._timeout = timeout
        self._max_bytes = max_bytes
        self._base_python = base_python
        self._site_dirs = list(worker_site_dirs() if site_dirs is None else site_dirs)
        self._lock = threading.Lock()

    @property
    def root(self) -> Path:
        return self._root

    def path(self, lock_hash: str) -> Path:
        return self._root / _checked_hash(lock_hash)

    def python(self, lock_hash: str) -> Path | None:
        """The Python of the environment for `lock_hash`, if it is built here."""
        env = self.path(lock_hash)
        return python_of(env) if (env / _MARKER).is_file() else None

    def ensure(self, lock: bytes, lock_hash: str, *, keep: Iterable[str] = ()) -> Path:
        """The Python of the environment for the lock, built first if it isn't. `keep` names lock hashes in use,
        which making room never removes. Raises `EnvError`."""
        if hashlib.sha256(lock).hexdigest() != lock_hash:
            raise EnvError("the lock is not the one its lock hash names")
        with self._lock:
            found = self.python(lock_hash)
            if found is not None:
                os.utime(self.path(lock_hash) / _MARKER)  # used: the last to be removed for room
                return found
            return self._build(lock, lock_hash, {lock_hash, *keep})

    def remove(self, lock_hash: str) -> None:
        with self._lock:
            shutil.rmtree(self.path(lock_hash), ignore_errors=True)

    def _build(self, lock: bytes, lock_hash: str, keep: set[str]) -> Path:
        if self._uv is None:
            raise EnvError("uv is not on the PATH: a worker needs it to build a package's environment")
        self._root.mkdir(parents=True, exist_ok=True)
        self._make_room(keep)
        work = self._root / f".build-{lock_hash[:12]}-{uuid.uuid4().hex[:8]}"
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
            target = self.path(lock_hash)
            try:
                work.rename(target)
            except OSError:
                if not (target / _MARKER).is_file():
                    raise
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if self._size() > self._max_bytes:
            self._make_room(keep)
            if self._size() > self._max_bytes:
                shutil.rmtree(self.path(lock_hash), ignore_errors=True)
                raise EnvError(
                    f"the environment doesn't fit: the environments here may take {self._max_bytes} bytes"
                )
        log.info("built the environment of lock %s in %.1f s", lock_hash[:12], time.monotonic() - started)
        return python_of(self.path(lock_hash))

    def _platform(self, env: Path) -> None:
        """Let the environment see the worker's own packages, after its own."""
        found = list(env.glob("lib/python*/site-packages")) + list(env.glob("Lib/site-packages"))
        if not found:
            raise EnvError("the new environment has no site-packages")
        lines = [f"import site; site.addsitedir({d!r})" for d in self._site_dirs]
        (found[0] / _PTH).write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _run(self, command: list[str]) -> None:
        env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"} | {"UV_NO_PROGRESS": "1"}
        tree = ProcessTree(
            command,
            cwd=self._root,
            env=env,
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
        return sum(b.size for b in self._built())

    def _make_room(self, keep: set[str]) -> None:
        """Remove the least recently used environments not in `keep` until they fit under the cap, and anything a
        build left half done."""
        for stale in self._root.glob(".build-*"):
            if time.time() - stale.stat().st_mtime > self._timeout * 3:
                shutil.rmtree(stale, ignore_errors=True)
        built = sorted(self._built(), key=lambda b: b.used_at)
        total = sum(b.size for b in built)
        for env in built:
            if total <= self._max_bytes:
                return
            if env.path.name in keep:
                continue
            shutil.rmtree(env.path, ignore_errors=True)
            total -= env.size
            log.info("removed the environment of lock %s to make room", env.path.name[:12])


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

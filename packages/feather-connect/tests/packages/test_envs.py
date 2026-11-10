"""Dependency environments (flow packages task 8): one per lock hash, built with uv from hashed requirements, reused,
kept under a cap, and stopped at a timeout. Built with no index: the libraries come from a local directory."""

import hashlib
import shutil
import subprocess
from pathlib import Path

import pytest

from feather_connect.packages.envs import EnvError, Environments
from feather_connect.testing.packaging import library_wheel, lock_for

pytestmark = pytest.mark.skipif(shutil.which("uv") is None, reason="uv builds the environments")


def _lib(tmp_path: Path, version: str) -> Path:
    return library_wheel(
        tmp_path / "wheels", "featherlib", version, {"featherlib/__init__.py": f"VERSION = {version!r}\n"}
    )


def _envs(tmp_path: Path, **options: object) -> Environments:
    return Environments(
        tmp_path / "envs",
        index=["--no-index", "--find-links", str(tmp_path / "wheels")],
        **options,  # type: ignore[arg-type]
    )


def _ask(python: Path, code: str) -> str:
    done = subprocess.run(
        [str(python), "-I", "-c", code], capture_output=True, text=True, timeout=60, check=True
    )
    return done.stdout.strip()


def test_two_locks_give_two_environments_with_two_versions_of_one_library(tmp_path: Path):
    one, two = lock_for([_lib(tmp_path, "1.0")]), lock_for([_lib(tmp_path, "2.0")])
    envs = _envs(tmp_path)
    first = envs.ensure(one, hashlib.sha256(one).hexdigest())
    second = envs.ensure(two, hashlib.sha256(two).hexdigest())
    assert first != second
    assert _ask(first, "import featherlib; print(featherlib.VERSION)") == "1.0"
    assert _ask(second, "import featherlib; print(featherlib.VERSION)") == "2.0"
    # and each sees Feather Connect, from the worker's own packages
    assert (
        _ask(first, "import feather_connect.packages.envs as e; print(e.__name__)")
        == "feather_connect.packages.envs"
    )
    assert (tmp_path / "envs" / "default") in first.parents  # kept per tenant


def test_an_environment_is_reused_and_a_lock_that_isnt_its_hash_is_refused(tmp_path: Path):
    lock = lock_for([_lib(tmp_path, "1.0")])
    digest = hashlib.sha256(lock).hexdigest()
    envs = _envs(tmp_path)
    built = envs.ensure(lock, digest)
    assert envs.python(digest) == built
    (tmp_path / "wheels").rename(tmp_path / "gone")  # nothing to build from: it must not build again
    assert envs.ensure(lock, digest) == built
    with pytest.raises(EnvError, match="lock hash"):
        envs.ensure(lock + b"\n", digest)


def test_a_lock_that_cant_be_met_says_why_and_leaves_nothing(tmp_path: Path):
    lock = lock_for([_lib(tmp_path, "1.0")]).replace(b"1.0", b"9.9")
    digest = hashlib.sha256(lock).hexdigest()
    envs = _envs(tmp_path)
    with pytest.raises(EnvError, match="uv failed"):
        envs.ensure(lock, digest)
    assert envs.python(digest) is None and not list(envs.root.glob(".build-*"))


def test_environments_stay_under_the_cap_the_least_recently_used_going_first(tmp_path: Path):
    locks = [lock_for([_lib(tmp_path, v)]) for v in ("1.0", "2.0", "3.0")]
    hashes = [hashlib.sha256(lock).hexdigest() for lock in locks]
    envs = _envs(tmp_path)
    envs.ensure(locks[0], hashes[0])
    size = envs._size()  # pyright: ignore[reportPrivateUsage]
    capped = _envs(tmp_path, max_bytes=int(size * 2.5))
    capped.ensure(locks[1], hashes[1])
    capped.ensure(locks[0], hashes[0])  # used again: the newest
    capped.ensure(locks[2], hashes[2])
    assert capped.python(hashes[1]) is None  # the least recently used went
    assert capped.python(hashes[0]) is not None and capped.python(hashes[2]) is not None
    tiny = _envs(tmp_path, max_bytes=1)
    with pytest.raises(EnvError, match="doesn't fit"):
        tiny.ensure(locks[1], hashes[1], keep=hashes)


def test_a_build_past_the_timeout_is_stopped(tmp_path: Path):
    lock = lock_for([_lib(tmp_path, "1.0")])
    envs = _envs(tmp_path, timeout=0.01)
    with pytest.raises(EnvError, match="past"):
        envs.ensure(lock, hashlib.sha256(lock).hexdigest())


def test_an_environment_in_use_is_never_evicted_and_eviction_removes_the_marker_first(tmp_path: Path):
    locks = [lock_for([_lib(tmp_path, v)]) for v in ("1.0", "2.0")]
    hashes = [hashlib.sha256(lock).hexdigest() for lock in locks]
    first = _envs(tmp_path)
    held = first.use(locks[0], hashes[0])  # a host runs in it
    other = _envs(tmp_path, max_bytes=1)  # another supervisor on the same cache, with no room at all
    assert other.in_use(hashes[0])
    with pytest.raises(EnvError, match="doesn't fit"):
        other.ensure(locks[1], hashes[1])
    assert other.python(hashes[0]) == held.python  # in use: it stayed
    held.release()
    assert not other.in_use(hashes[0])
    other._make_room(set())  # pyright: ignore[reportPrivateUsage]
    assert other.python(hashes[0]) is None and not other.path(hashes[0]).exists()
    assert not list(other.root.glob(".trash-*"))


def test_two_supervisors_sharing_a_cache_build_a_lock_once(tmp_path: Path):
    import threading

    lock = lock_for([_lib(tmp_path, "1.0")])
    digest = hashlib.sha256(lock).hexdigest()
    found: list[Path] = []
    builds: list[str] = []
    workers = [_envs(tmp_path), _envs(tmp_path)]
    for envs in workers:
        real = envs._build  # pyright: ignore[reportPrivateUsage]

        def counted(*args: object, real: object = real) -> Path:
            builds.append("built")
            return real(*args)  # type: ignore[operator]

        envs._build = counted  # type: ignore[method-assign]

    threads = [threading.Thread(target=lambda e=e: found.append(e.ensure(lock, digest))) for e in workers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120)
    assert builds == ["built"] and len(set(found)) == 1  # one built it, the other waited and took it


def test_uv_gets_no_secret_of_the_worker_and_no_index_unless_one_is_named(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    from feather_connect.packages.envs import uv_environment

    monkeypatch.setenv("FEATHER_MASTER_KEY", "a" * 44)
    monkeypatch.setenv("FEATHER_DATABASE_URL", "postgresql://u:p@db/feather")
    monkeypatch.setenv("UV_INDEX_URL", "https://evil.example/simple")
    env = uv_environment(tmp_path / "uv")
    assert not {"FEATHER_MASTER_KEY", "FEATHER_DATABASE_URL", "UV_INDEX_URL"} & env.keys()
    assert env["UV_CACHE_DIR"] == str(tmp_path / "uv") and "PATH" in env
    assert "--no-index" in Environments(tmp_path, index=["--find-links", "x"])._index  # pyright: ignore[reportPrivateUsage]
    assert "--no-index" not in Environments(tmp_path, index=["--index-url", "u"])._index  # pyright: ignore[reportPrivateUsage]


def test_a_process_tree_in_an_environment_dies_whole_grandchildren_too(tmp_path: Path):
    import sys
    import time

    from feather_connect.packages.tree import ProcessTree

    lock = lock_for([_lib(tmp_path, "1.0")])
    python = _envs(tmp_path).ensure(lock, hashlib.sha256(lock).hexdigest())
    marker = tmp_path / "grandchild.pid"
    (tmp_path / "grandchild.py").write_text(
        f"import os, time\nopen({str(marker)!r}, 'w').write(str(os.getpid()))\ntime.sleep(120)\n",
        encoding="utf-8",
    )
    (tmp_path / "child.py").write_text(
        "import subprocess, sys, time\n"
        f"subprocess.Popen([sys.executable, {str(tmp_path / 'grandchild.py')!r}])\n"
        "time.sleep(120)\n",
        encoding="utf-8",
    )
    tree = ProcessTree([str(python), str(tmp_path / "child.py")])  # the venv's python: on Windows, a launcher
    deadline = time.monotonic() + 60
    while not (marker.is_file() and marker.read_text()) and time.monotonic() < deadline:
        time.sleep(0.1)
    grandchild = int(marker.read_text())
    tree.kill()
    deadline = time.monotonic() + 20
    while _alive(grandchild) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not _alive(grandchild), f"the grandchild {grandchild} outlived its tree on {sys.platform}"


def _alive(pid: int) -> bool:
    import os
    import sys

    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return False
        code = ctypes.c_ulong()
        kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        kernel32.CloseHandle(handle)
        return code.value == 259
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:  # a zombie the test process doesn't reap still answers kill 0
        with open(f"/proc/{pid}/stat") as stat:
            return stat.read().split()[2] != "Z"
    except OSError:
        return False

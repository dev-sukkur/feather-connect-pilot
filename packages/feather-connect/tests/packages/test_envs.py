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

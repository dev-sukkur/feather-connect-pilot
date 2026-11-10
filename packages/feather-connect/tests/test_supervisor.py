"""The worker supervisor on stand-in hosts (flow packages §6, §7.1, §11): what it does when a host comes up, dies, keeps
dying or is wanted again while it drains, what environment it gives a host, and how it waits for an environment it
can't build. The real hosts run in tests/e2e/test_package_hosts.py."""

import hashlib
import io
import os
import sys
import time
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from feather_connect.packages.build import LOCK_FILE
from feather_connect.packages.check import LoadCheck
from feather_connect.packages.envs import EnvError, Environments, EnvUse
from feather_connect.packages.filelock import FileLock
from feather_connect.packages.tree import ProcessTree
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import PackageVersion
from feather_connect.supervisor import CRASH_LOOP, Supervisor, host_environment

T0 = datetime(2026, 11, 10, 9, 0, tzinfo=UTC)
READY = 'import sys, time\nsys.stdout.write(\'{"op": "ready"}\\n\'); sys.stdout.flush()\nsys.stdin.read()\n'
CRASH = "import sys\nsys.stderr.write('boom: the package fell over\\n')\nsys.exit(1)\n"
NOT_LOADING = (
    'import sys\nsys.stdout.write(\'{"op": "failed", "error": "ImportError: no module named x"}\\n\')\n'
    "sys.stdout.flush()\nsys.exit(1)\n"
)


def _wheel(name: str, version: str) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr(f"{name}-{version}.dist-info/{LOCK_FILE}", b"")
    return out.getvalue()


def _installed(store: MemoryStore, name: str = "acme", version: str = "1.0.0") -> None:
    content = _wheel(name, version)
    row = PackageVersion(
        name,
        version,
        hashlib.sha256(content).hexdigest(),
        "feather",
        f"{name}-{version}-py3-none-any.whl",
        len(content),
        {"name": name, "version": version, "flows": [{"name": "feed"}], "connections": []},
        ">=0",
        hashlib.sha256(b"").hexdigest(),
        "preparing",
        {},
        "root",
        T0,
    )
    store.packages.install(row, content, [f"{name}/feed"])
    store.packages.prepared(name, version, "ready", {"ok": True})


class _Envs:
    """Environments that are there at once: the host's Python is this one."""

    def __init__(self, tmp: Path, fail: bool = False) -> None:
        self.tmp = tmp
        self.fail = fail
        self.used = 0
        self.built_here: list[str] = []
        self.held: set[str] = set()

    def use(self, lock: bytes, lock_hash: str, *, keep: Any = ()) -> EnvUse:
        self.used += 1
        if self.fail:
            raise EnvError("uv failed: the index is away")
        mark = FileLock(self.tmp / f"use-{self.used}.lock")
        mark.try_acquire()
        return EnvUse(Path(sys.executable), mark)

    def built(self) -> list[str]:
        return sorted(self.built_here)

    def in_use(self, lock_hash: str) -> bool:
        return lock_hash in self.held

    def remove(self, lock_hash: str) -> bool:
        self.built_here = [h for h in self.built_here if h != lock_hash]
        return True


class _World:
    def __init__(self, tmp: Path, script: str, envs: _Envs | None = None) -> None:
        self.store = MemoryStore()
        self.now = [1000.0]
        self.script = script
        self.started: list[dict[str, str]] = []
        self.supervisor = Supervisor(
            self.store,
            env="dev",
            worker_id="w",
            database_url="postgresql://owner@db/feather",
            host_database_url="postgresql://feather_host@db/feather",
            envs=cast(Environments, envs or _Envs(tmp)),
            cache=tmp / "cache",
            spawn=self._spawn,
            monotonic=lambda: self.now[0],
            log_dir=tmp / "logs",
        )

    def _spawn(self, command: list[str], environ: dict[str, str], host_id: str) -> ProcessTree:
        self.started.append(environ)
        log = self.supervisor._log_path(host_id)  # pyright: ignore[reportPrivateUsage]
        assert log is not None
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("ab") as out:
            return ProcessTree(
                [sys.executable, "-c", self.script],
                stdin=-1,
                stdout=-1,
                stderr=out,
                bufsize=0,
                env={**os.environ, **{k: v for k, v in environ.items() if k != "FEATHER_DATABASE_URL"}},
            )

    def until(self, check: Callable[[], Any], timeout: float = 30) -> Any:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.supervisor.step()
            found = check()
            if found:
                return found
            time.sleep(0.05)
        raise AssertionError("timed out")

    def host(self) -> Any:
        return self.supervisor._hosts.get(("acme", "1.0.0"))  # pyright: ignore[reportPrivateUsage]

    def close(self) -> None:
        self.supervisor.stop_all(wait_s=10)


def test_triggers_move_to_a_version_only_once_its_host_says_ready(tmp_path: Path):
    world = _World(tmp_path, READY)
    _installed(world.store)
    world.store.packages.activate("acme/feed", "acme", "1.0.0", "ops", T0)
    try:
        assert world.store.packages.active() == []
        world.until(lambda: world.store.packages.active())
        [host] = world.store.packages.hosts("w")
        assert (host.package, host.version, host.state) == ("acme", "1.0.0", "ready")
    finally:
        world.close()
    assert world.store.packages.hosts("w")[0].state == "stopped"


def test_a_host_that_cant_load_leaves_the_triggers_on_the_version_before(tmp_path: Path):
    world = _World(tmp_path, NOT_LOADING)
    _installed(world.store, version="1.0.0")
    _installed(world.store, version="1.1.0")
    try:
        world.store.packages.activate("acme/feed", "acme", "1.0.0", "ops", T0)
        world.store.packages.promote("acme", "1.0.0", T0)  # 1.0.0 fires
        world.store.packages.activate("acme/feed", "acme", "1.1.0", "ops", T0)
        failed = world.until(
            lambda: [a for a in world.store.packages.activations("acme", "1.1.0") if a.state == "failed"]
        )
        assert "no module named x" in (failed[0].error or "")
        assert [(a.flow, a.version) for a in world.store.packages.active()] == [("acme/feed", "1.0.0")]
    finally:
        world.close()


def test_a_host_that_keeps_dying_is_recorded_in_a_crash_loop_and_still_restarted(tmp_path: Path):
    world = _World(tmp_path, CRASH)
    _installed(world.store)
    world.store.packages.activate("acme/feed", "acme", "1.0.0", "ops", T0)
    world.store.packages.promote("acme", "1.0.0", T0)
    try:
        for ended in range(1, CRASH_LOOP + 1):
            host = world.until(world.host)
            host.tree.process.wait(30)
            world.supervisor.step()  # sees it end
            [row] = world.store.packages.hosts("w")
            assert row.restarts == ended and "boom: the package fell over" in (row.error or "")
            world.now[0] += 61  # past the longest back-off, within the crash window
        assert world.store.packages.hosts("w")[0].state == "crash_loop"
        world.until(world.host)  # and it is started again
    finally:
        world.close()


def test_a_version_wanted_again_while_its_host_drains_gets_a_new_host_at_once(tmp_path: Path):
    world = _World(tmp_path, READY)
    _installed(world.store)
    world.store.packages.activate("acme/feed", "acme", "1.0.0", "ops", T0)
    try:
        first = world.until(lambda: world.host() if world.store.packages.active() else None)
        world.store.packages.deactivate("acme/feed", "ops", T0)
        world.supervisor.step()
        assert first.stopping_since is not None  # draining
        world.store.packages.activate("acme/feed", "acme", "1.0.0", "ops", T0)  # a rollback to it
        first.tree.process.wait(30)
        second = world.until(lambda: (h := world.host()) is not None and h is not first and h)
        assert second.stopping_since is None and len(world.started) == 2
    finally:
        world.close()


def test_a_host_gets_no_secret_of_the_worker(monkeypatch: pytest.MonkeyPatch):
    for name in (
        "FEATHER_MASTER_KEY",
        "FEATHER_MASTER_KEY_FILE",
        "FEATHER_MASTER_KEY_PREVIOUS",
        "FEATHER_SECRET_KEY",
    ):
        monkeypatch.setenv(name, "secret-of-the-worker")
    monkeypatch.setenv("FEATHER_DATABASE_URL", "postgresql://owner:pw@db/feather")
    monkeypatch.setenv("ACME_SETTING", "x")
    env = host_environment("postgresql://feather_host:pw@db/feather", ["ACME_SETTING", "FEATHER_MASTER_KEY"])
    assert "secret-of-the-worker" not in env.values()
    assert env["FEATHER_DATABASE_URL"] == "postgresql://feather_host:pw@db/feather"
    assert env["ACME_SETTING"] == "x" and "PATH" in env


def test_an_environment_that_cant_be_built_is_tried_again_after_a_back_off(tmp_path: Path):
    envs = _Envs(tmp_path, fail=True)
    world = _World(tmp_path, READY, envs)
    _installed(world.store)
    world.store.packages.activate("acme/feed", "acme", "1.0.0", "ops", T0)
    try:
        world.until(lambda: world.store.packages.hosts("w"))
        [row] = world.store.packages.hosts("w")
        assert row.state == "failed" and "the index is away" in (row.error or "")
        tried = envs.used
        for _ in range(5):
            world.supervisor.step()
            time.sleep(0.02)
        assert envs.used == tried  # backing off
        envs.fail = False
        world.now[0] += 61
        world.until(lambda: world.store.packages.active())  # the network is back: it builds and starts
    finally:
        world.close()


def test_a_version_whose_environment_cant_be_built_stays_preparing_and_is_tried_again(tmp_path: Path):
    envs = _Envs(tmp_path, fail=True)
    world = _World(tmp_path, READY, envs)
    _installed(world.store)
    content = _wheel("acme", "2.0.0")
    world.store.packages.install(
        PackageVersion(
            "acme",
            "2.0.0",
            hashlib.sha256(content).hexdigest(),
            "feather",
            "acme-2.0.0-py3-none-any.whl",
            len(content),
            {"name": "acme", "version": "2.0.0", "flows": []},
            ">=0",
            hashlib.sha256(b"").hexdigest(),
            "preparing",
            {},
            "root",
            T0,
        ),
        content,
        [],
    )
    assert world.supervisor.prepare_pending() == []
    row = world.store.packages.version("acme", "2.0.0")
    assert row is not None and row.state == "preparing"  # not failed: the network may be back soon
    assert world.store.packages.envs("w")[0].error == "uv failed: the index is away"
    world.supervisor.prepare_pending()
    assert envs.used == 1  # backing off
    envs.fail = False
    world.now[0] += 10
    world.supervisor._checker = lambda *a, **k: LoadCheck(True)  # pyright: ignore[reportPrivateUsage]
    assert world.supervisor.prepare_pending() == ["acme 2.0.0"]


def test_environments_no_installed_version_needs_are_removed_unless_in_use(tmp_path: Path):
    envs = _Envs(tmp_path)
    acme, stale, held = hashlib.sha256(b"").hexdigest(), "f" * 64, "e" * 64
    envs.built_here, envs.held = [acme, stale, held], {held}
    world = _World(tmp_path, READY, envs)
    _installed(world.store)
    assert world.supervisor.remove_unneeded_envs() == [stale]  # acme 1.0.0 needs its lock; one is in use
    assert envs.built() == sorted([acme, held])
    world.store.packages.uninstall("acme", "1.0.0", "root", T0)
    assert world.supervisor.remove_unneeded_envs() == [acme]  # its last version was uninstalled
    assert envs.built() == [held]


def test_a_queued_run_of_an_uninstalled_version_is_failed_with_the_reason(tmp_path: Path):
    world = _World(tmp_path, READY)
    _installed(world.store)
    world.store.packages.uninstall("acme", "1.0.0", "root", T0)
    late = world.store.runs.create_run(
        "acme/feed", "cron", "scheduler", {"package": "acme", "version": "1.0.0"}
    )
    assert world.supervisor.wanted() == set()
    run = world.store.runs.get_run(late)
    assert run.status == "failed" and "uninstalled" in (run.reason or "")

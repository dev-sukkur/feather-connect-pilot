"""Package hosts on real processes and Postgres (flow packages tasks 6 and 8): the supervisor prepares an installed
version, starts its host when a flow of it is activated and moves the flow's triggers to it once it is up, the host
runs the version's runs on that version's code with credentials the supervisor resolves, and drains when it has no
work; two packages that pin two versions of one library run side by side."""

import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from hosts import FLOW, TOKEN, TOKEN_DIGEST, HostWorld, set_url, wait_for
from real_stack import scratch_database, server_url

from feather_connect.packages.link import LinkError
from feather_connect.runtime.catalogue import Catalogue
from feather_connect.storage.postgres.leader import PostgresLeaderLock, advisory_key

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.timeout(600),
    pytest.mark.skipif(shutil.which("uv") is None, reason="uv builds the environments"),
]


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[HostWorld]:
    with scratch_database(server_url()) as url:
        monkeypatch.setenv("FEATHER_HOSTED_URL", url)
        set_url(url)
        found = HostWorld(url, tmp_path)
        try:
            yield found
        finally:
            found.close()


POLL = "poll"


def _first_tick(world: HostWorld, version: str, timeout: float) -> str:
    """The first tick of the poller that the version's host ran, by the delivery it wrote."""
    return wait_for(
        lambda: next(iter(world.ticks("acme", version)), None), timeout, f"a tick of the poller on {version}"
    )


def test_a_poller_activated_ticks_on_its_host_into_one_stream_run_and_deactivated_stops_after_5_s(
    world: HostWorld, record_property: Callable[[str, object], None]
):
    world.install(world.build("acme", "1.0.0", lib="1.0"))
    world.start()
    world.start_scheduler()  # it ticks the other triggers: a poller's are its host's (M2 plan 1 §3)
    wait_for(lambda: world.state("acme", "1.0.0") == "ready", 120, "the supervisor to prepare acme 1.0.0")
    row = world.store.packages.version("acme", "1.0.0")
    assert row is not None
    world.envs.remove(row.lock_hash)  # an environment not built yet on this worker
    assert world.host_of("acme", "1.0.0") is None  # nothing is active: no host

    activated = time.monotonic()
    world.activate("acme", "1.0.0", POLL)
    assert world.active("acme", POLL) is None  # it ticks once its host is up
    _first_tick(world, "1.0.0", 120)
    record_property("activate_uncached_s", round(time.monotonic() - activated, 1))
    host = world.host_of("acme", "1.0.0")
    assert host is not None
    wait_for(lambda: len(world.ticks("acme")) >= 3, 60, "three ticks")
    runs = world.store.runs.list_runs(flow=f"acme/{POLL}", limit=50)
    assert [r.trigger for r in runs] == ["stream"] * len(runs) and len(runs) <= 2  # a run a day, not a tick
    assert {(code, lib) for _, _, code, lib in world.rows()} == {("1.0.0", "1.0")}

    # Deactivated: its host ticks nothing once 5 s have passed.
    stopped = time.monotonic()
    world.deactivate("acme", POLL)
    time.sleep(max(0.0, stopped + 5 - time.monotonic()))
    after = world.ticks("acme")
    time.sleep(6)  # three ticks of its trigger
    assert world.ticks("acme") == after, "the host ticked 5 s after deactivating"
    wait_for(lambda: world.host_of("acme", "1.0.0") is None, 120, "the host to drain and stop")
    worker = next(w for w in world.store.workers.list_workers() if w.worker_id == host)
    assert worker.state == "draining"  # it drained, as a worker does on SIGTERM

    # Activated again, on the environment it built: a tick within 30 s.
    seen = set(world.ticks("acme"))
    activated = time.monotonic()
    world.activate("acme", "1.0.0", POLL)
    wait_for(
        lambda: set(world.ticks("acme")) - seen,
        30,
        "a new tick within 30 s of activating, its environment built",
    )
    record_property("activate_cached_s", round(time.monotonic() - activated, 1))


def test_a_queued_run_of_an_inactive_version_still_runs_on_its_own_code(world: HostWorld):
    world.install(world.build("acme", "1.0.0"))
    world.start()
    wait_for(lambda: world.state("acme", "1.0.0") == "ready", 120, "acme 1.0.0 to be ready")
    run = world.queue("acme", "1.0.0")  # queued before a deactivation: it is work of that version still
    assert world.wait_for_runs([run])[run] == "succeeded"
    wait_for(lambda: world.host_of("acme", "1.0.0") is None, 120, "the host to stop once the run is over")


def test_a_host_gets_its_connections_credentials_from_the_supervisor_and_no_other_secret(
    world: HostWorld, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("FEATHER_MASTER_KEY", world.master_key)  # the worker has it; its hosts must not
    world.install(world.build("acme", "1.0.0"))
    world.start()
    wait_for(lambda: world.state("acme", "1.0.0") == "ready", 120, "acme 1.0.0 to be ready")
    run = world.queue("acme", "1.0.0")
    assert world.wait_for_runs([run])[run] == "succeeded"
    [(_, _, _, token, names)] = world.attempts()
    assert token == TOKEN_DIGEST  # the sink had its credential, resolved by the supervisor's vault
    assert "FEATHER_MASTER_KEY" not in (names or "").split(",") and "FEATHER_DATABASE_URL" in (names or "")

    key = ("acme", "1.0.0")
    ask = world.supervisor._secret  # pyright: ignore[reportPrivateUsage]
    assert ask(key, {"connection": "out", "field": "token", "ref": "${secret:db:out/token}"}) == {
        "value": TOKEN
    }
    for message in (
        {"connection": "src", "field": "token", "ref": "${secret:db:out/token}"},  # another connection's
        {"connection": "out", "field": "password", "ref": "${secret:db:out/password}"},  # a field it hasn't
        {"connection": "other", "field": "token", "ref": "${secret:db:other/token}"},  # not its connection
        {
            "connection": "out",
            "field": "token",
            "ref": "${secret:env:FEATHER_MASTER_KEY}",
        },  # not its reference
    ):
        with pytest.raises(LinkError):
            ask(key, message)


def test_a_second_host_with_the_same_id_refuses_to_start_while_the_first_holds_it(world: HostWorld):
    held = PostgresLeaderLock(world.host_url, advisory_key("default", "host:sup/acme@1.0.0"))
    assert held.try_acquire()  # as the first host does, before it takes work
    try:
        done = subprocess.run(
            [
                sys.executable,
                "-m",
                "feather_connect.host",
                "acme",
                "1.0.0",
                "dev",
                "--worker-id",
                "sup/acme@1.0.0",
            ]
            + ["--cache", str(world.tmp / "cache"), "--no-link"],
            env={**os.environ, "FEATHER_DATABASE_URL": world.host_url},
            capture_output=True,
            timeout=120,
            check=False,
        )
    finally:
        held.release()
    assert done.returncode == 3, done.stderr.decode(errors="replace")[-2000:]
    assert b"another host runs as sup/acme@1.0.0" in done.stderr


def test_a_version_that_doesnt_load_fails_preparing_and_gets_no_host(world: HostWorld, tmp_path: Path):
    built = world.build("broken", "1.0.0")
    import io
    import zipfile

    out = io.BytesIO()
    with zipfile.ZipFile(built.path) as source, zipfile.ZipFile(out, "w") as target:
        for name in source.namelist():
            data = source.read(name)
            target.writestr(
                name, b"raise RuntimeError('no rows today')\n" + data if name.endswith("hosted.py") else data
            )
    built.path.write_bytes(out.getvalue())
    world.install(built)
    world.start()
    wait_for(lambda: world.state("broken", "1.0.0") == "failed", 120, "the load check to fail")
    row = world.store.packages.version("broken", "1.0.0")
    assert row is not None and row.load_check["check"] == "load_check"
    assert "no rows today" in row.load_check["error"] and row.load_check["worker"] == "sup"
    world.queue("broken", "1.0.0")
    time.sleep(3)
    assert world.host_of("broken", "1.0.0") is None  # a version that isn't ready never gets a host


def test_activating_a_version_whose_host_cant_load_keeps_the_old_one_firing(world: HostWorld):
    for version in ("1.0.0", "1.1.0"):
        world.install(world.build("acme", version))
    world.start()
    for version in ("1.0.0", "1.1.0"):
        wait_for(lambda v=version: world.state("acme", v) == "ready", 120, f"acme {version} to be ready")
    world.activate("acme", "1.0.0", POLL)
    _first_tick(world, "1.0.0", 60)
    world.fail_load("1.1.0")  # loads in the load check, but not in a host
    world.activate("acme", "1.1.0", POLL)
    wait_for(
        lambda: world.activation("acme", "1.1.0", POLL) == "failed", 120, "the activation of 1.1.0 to fail"
    )
    [failed] = [a for a in world.store.packages.activations("acme", "1.1.0") if a.flow == f"acme/{POLL}"]
    assert "was told not to load" in (failed.error or "")
    assert world.active("acme", POLL) == "1.0.0"
    seen = set(world.ticks("acme"))
    wait_for(lambda: set(world.ticks("acme", "1.0.0")) - seen, 30, "the old version to tick again")
    assert world.ticks("acme", "1.1.0") == []


def test_two_packages_pinning_two_versions_of_one_library_run_side_by_side(world: HostWorld):
    world.install(world.build("lib-one", "1.0.0", lib="1.0"))
    world.install(world.build("lib-two", "1.0.0", lib="2.0"))
    world.start()
    for name in ("lib-one", "lib-two"):
        wait_for(lambda name=name: world.state(name, "1.0.0") == "ready", 180, f"{name} to be ready")
        world.activate(name, "1.0.0")
    runs = [world.queue("lib-one", "1.0.0"), world.queue("lib-two", "1.0.0")]
    wait_for(lambda: len(world.supervisor.hosts()) == 2, 60, "both hosts")  # at once, each in its environment
    assert set(world.wait_for_runs(runs).values()) == {"succeeded"}
    libs = {(route.split("/")[0], lib) for route, _, _, lib in world.conn.execute(
        "select route, key, code, lib from hosted.rows"
    ).fetchall()}  # fmt: skip
    assert libs == {("lib-one", "1.0"), ("lib-two", "2.0")}
    envs = {e.lock_hash for e in world.store.packages.envs("sup") if e.state == "ready"}
    assert len(envs) == 2  # two locks, two environments


def test_a_version_switch_and_a_rollback_finish_each_run_on_the_code_it_started_with(world: HostWorld):
    for version in ("1.0.0", "1.1.0"):
        world.install(world.build("acme", version))
    world.start()
    for version in ("1.0.0", "1.1.0"):
        wait_for(lambda v=version: world.state("acme", v) == "ready", 120, f"acme {version} to be ready")
    world.activate("acme", "1.0.0")
    wait_for(lambda: world.active("acme") == "1.0.0", 60, "1.0.0's host to be up")
    world.arm("apply:1.0.0")  # the 1.0.0 run stops mid-apply, so it is in progress through the switch
    old = world.queue("acme", "1.0.0")
    wait_for(lambda: world.reached("apply:1.0.0"), 60, "the 1.0.0 run to be applying")

    world.activate("acme", "1.1.0")  # the switch: 1.0.0 fires until 1.1.0's host is up
    wait_for(lambda: world.active("acme") == "1.1.0", 60, "1.1.0's host to be up")
    new = world.queue("acme", "1.1.0")
    wait_for(lambda: world.status(new) == "running", 60, "the 1.1.0 host to take its run")
    assert world.host_of("acme", "1.0.0") is not None  # still running: its run isn't over
    assert world.status(old) == "running"
    # The 1.1.0 delivery waits for the 1.0.0 one on their route, which stays in order across versions.
    world.release("apply:1.0.0")
    assert world.wait_for_runs([old, new]) == {old: "succeeded", new: "succeeded"}
    wait_for(lambda: world.host_of("acme", "1.0.0") is None, 120, "the 1.0.0 host to drain")
    assert {code: len(keys) for code, keys in world.keys_by_code().items()} == {"1.0.0": 1, "1.1.0": 1}

    world.activate("acme", "1.0.0")  # the rollback is a switch back
    wait_for(lambda: world.active("acme") == "1.0.0", 60, "1.0.0's host to be up again")
    back = world.queue("acme", "1.0.0")
    assert world.wait_for_runs([back])[back] == "succeeded"
    wait_for(lambda: world.host_of("acme", "1.1.0") is None, 120, "the 1.1.0 host to drain")
    assert {code: len(keys) for code, keys in world.keys_by_code().items()} == {"1.0.0": 2, "1.1.0": 1}
    assert all(
        len([r for r in world.rows() if r[0] == key]) == 5
        for keys in world.keys_by_code().values()
        for key in keys
    )
    catalogue = Catalogue(world.store)
    assert [(f.name, f.version) for f in catalogue.flows()] == [(f"acme/{FLOW}", "1.0.0")]

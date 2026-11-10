"""Package hosts on real processes and Postgres (flow packages tasks 6 and 8): the supervisor prepares an installed
version, starts its host when a flow of it is activated, the host runs the version's queued runs on that version's
code, and drains when it has no work; two packages that pin two versions of one library run side by side."""

import shutil
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from hosts import FLOW, HostWorld, set_url, wait_for
from real_stack import scratch_database, server_url

from feather_connect.runtime.catalogue import Catalogue

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


def test_a_flow_activated_gets_a_host_in_30_s_and_runs_and_deactivated_stops_firing_in_5_s(world: HostWorld):
    world.install(world.build("acme", "1.0.0"))
    world.start()
    wait_for(lambda: world.state("acme", "1.0.0") == "ready", 120, "the supervisor to prepare acme 1.0.0")
    assert world.host_of("acme", "1.0.0") is None  # nothing is active: no host

    activated = time.monotonic()
    world.activate("acme", "1.0.0")
    run = world.queue("acme", "1.0.0")  # the scheduler's first tick
    host = wait_for(lambda: world.host_of("acme", "1.0.0"), 30, "a host")
    wait_for(
        lambda: world.status(run) != "queued", 30 - (time.monotonic() - activated), "the host to take the run"
    )
    assert time.monotonic() - activated <= 30, "activate is within 30 s with a ready environment"
    assert world.wait_for_runs([run])[run] == "succeeded", world.log_of(host)
    assert {(code, lib) for _, _, code, lib in world.rows()} == {("1.0.0", None)}
    assert len(world.rows()) == 5  # five records, written once

    catalogue = Catalogue(world.store)
    catalogue.listen()
    try:
        assert [f.name for f in catalogue.flows()] == [f"acme/{FLOW}"]
        stopped = time.monotonic()
        world.deactivate("acme")
        wait_for(lambda: catalogue.flows() == [], 5, "its triggers to stop")
        assert time.monotonic() - stopped <= 5
    finally:
        catalogue.close()
    wait_for(lambda: world.host_of("acme", "1.0.0") is None, 120, "the host to drain and stop")
    worker = next(w for w in world.store.workers.list_workers() if w.worker_id == host)
    assert worker.state == "draining"  # it drained, as a worker does on SIGTERM


def test_a_queued_run_of_an_inactive_version_still_runs_on_its_own_code(world: HostWorld):
    world.install(world.build("acme", "1.0.0"))
    world.start()
    wait_for(lambda: world.state("acme", "1.0.0") == "ready", 120, "acme 1.0.0 to be ready")
    run = world.queue("acme", "1.0.0")  # queued before a deactivation: it is work of that version still
    assert world.wait_for_runs([run])[run] == "succeeded"
    wait_for(lambda: world.host_of("acme", "1.0.0") is None, 120, "the host to stop once the run is over")


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
    world.arm("apply:1.0.0")  # the 1.0.0 run stops mid-apply, so it is in progress through the switch
    old = world.queue("acme", "1.0.0")
    wait_for(lambda: world.reached("apply:1.0.0"), 60, "the 1.0.0 run to be applying")

    world.activate("acme", "1.1.0")  # the switch: new ticks are 1.1.0's
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
    back = world.queue("acme", "1.0.0")
    assert world.wait_for_runs([back])[back] == "succeeded"
    wait_for(lambda: world.host_of("acme", "1.1.0") is None, 120, "the 1.1.0 host to drain")
    assert {code: len(keys) for code, keys in world.keys_by_code().items()} == {"1.0.0": 2, "1.1.0": 1}
    assert all(
        len([r for r in world.rows() if r[0] == key]) == 5
        for keys in world.keys_by_code().values()
        for key in keys
    )

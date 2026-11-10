"""Flow packages task 7's exit criterion: a package host killed with `kill -9` during a version switch loses and
duplicates nothing. The old version's run is stopped mid-apply, the switch is made, and its host is killed there; the
supervisor starts it again under the same id, DBOS gives it back its task, and the run finishes on its own code while
the new version's runs go on beside it."""

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from hosts import HostWorld, set_url, wait_for
from real_stack import scratch_database, server_url

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.chaos,
    pytest.mark.timeout(600),
    pytest.mark.skipif(shutil.which("uv") is None, reason="uv builds the environments"),
]


@pytest.fixture
def hosts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[HostWorld]:
    with scratch_database(server_url()) as url:
        monkeypatch.setenv("FEATHER_HOSTED_URL", url)
        set_url(url)
        found = HostWorld(url, tmp_path)
        try:
            yield found
        finally:
            found.close()


def test_kill_9_of_a_host_during_a_switch_loses_and_duplicates_nothing(hosts: HostWorld):
    for version in ("1.0.0", "1.1.0"):
        hosts.install(hosts.build("acme", version))
    hosts.start()
    for version in ("1.0.0", "1.1.0"):
        wait_for(lambda v=version: hosts.state("acme", v) == "ready", 120, f"acme {version} to be ready")
    hosts.activate("acme", "1.0.0")
    hosts.arm("apply:1.0.0")
    old = hosts.queue("acme", "1.0.0")
    [(_, pid)] = wait_for(lambda: hosts.reached("apply:1.0.0"), 60, "the 1.0.0 run to be applying")
    host = hosts.host_of("acme", "1.0.0")
    assert host is not None

    hosts.activate("acme", "1.1.0")  # the switch
    during = [hosts.queue("acme", "1.1.0") for _ in range(2)]
    hosts.kill_host("acme", "1.0.0")  # mid-apply, mid-switch
    assert hosts.wait_for_runs(during) == {run: "succeeded" for run in during}

    # The supervisor starts the old host again, as its run isn't over, and DBOS hands it its task back.
    assert hosts.wait_for_runs([old], timeout=180)[old] == "succeeded", hosts.log_of(host)
    attempts = hosts.conn.execute("select code, pid from hosted.attempts where code = '1.0.0'").fetchall()
    assert len(attempts) >= 2 and attempts[0][1] == pid != attempts[-1][1]  # tried again, by a new process
    wait_for(lambda: hosts.host_of("acme", "1.0.0") is None, 120, "the old host to drain")

    keys = hosts.keys_by_code()
    assert {code: len(found) for code, found in keys.items()} == {
        "1.0.0": 1,
        "1.1.0": 2,
    }  # one delivery a run
    for found in keys.values():
        for key in found:
            assert sorted(r[1] for r in hosts.rows() if r[0] == key) == [
                "1",
                "2",
                "3",
                "4",
                "5",
            ]  # each record once
    runs = hosts.store.runs.list_runs(flow="acme/feed", limit=50)
    assert sorted(r.run_id for r in runs) == sorted([old, *during])  # nothing lost, nothing made up
    for run in runs:
        plans = hosts.store.runs.list_tasks(run.run_id, kinds=("plan",))
        assert len(plans) == 1, run.run_id  # each run planned once

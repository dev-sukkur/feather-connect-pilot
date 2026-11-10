"""Flow packages task 7's exit criterion: a package host killed with `kill -9` during a version switch loses and
duplicates nothing. The old version's run is stopped mid-apply, the switch is made, and its host is killed there; the
supervisor starts it again under the same id, DBOS gives it back its task, and the run finishes on its own code while
the new version's runs go on beside it."""

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from hosts import HostWorld, pid_alive, set_url, wait_for
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
    wait_for(lambda: hosts.active("acme") == "1.0.0", 60, "1.0.0's host to be up")
    hosts.arm("apply:1.0.0")
    old = hosts.queue("acme", "1.0.0")
    [(_, pid)] = wait_for(lambda: hosts.reached("apply:1.0.0"), 60, "the 1.0.0 run to be applying")
    host = hosts.host_of("acme", "1.0.0")
    assert host is not None

    hosts.activate("acme", "1.1.0")  # the switch, once 1.1.0's host is up
    wait_for(lambda: hosts.active("acme") == "1.1.0", 60, "1.1.0's host to be up")
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


def test_kill_9_of_the_supervisor_leaves_no_host_behind_and_each_task_runs_once(hosts: HostWorld):
    """A supervisor killed with `kill -9` takes its hosts with it: on Windows their Job Object dies with it, on Linux
    the kernel sends them SIGTERM, and on both they see their pipe close. Its replacement starts the host again under
    the same id, which the advisory lock on the id keeps from running beside an orphan, and nothing runs twice."""
    from collections import Counter

    hosts.install(hosts.build("acme", "1.0.0"))
    cache = hosts.tmp / "worker-cache"
    first = hosts.supervisor_process(cache)
    wait_for(lambda: hosts.state("acme", "1.0.0") == "ready", 180, "the supervisor to prepare acme 1.0.0")
    hosts.activate("acme", "1.0.0")
    wait_for(lambda: hosts.active("acme") == "1.0.0", 120, "its host to be up")
    before = hosts.queue("acme", "1.0.0")
    assert hosts.wait_for_runs([before])[before] == "succeeded"
    [(_, _, orphan, _, _)] = hosts.attempts()

    first.kill()  # kill -9: no drain, no cleanup
    first.wait(30)
    wait_for(lambda: not pid_alive(orphan), 60, "the host of the killed supervisor to stop")

    hosts.supervisor_process(cache)  # its replacement, on the same worker
    after = [hosts.queue("acme", "1.0.0") for _ in range(2)]
    assert hosts.wait_for_runs(after, timeout=180) == {run: "succeeded" for run in after}
    attempts = hosts.attempts()
    assert set(Counter(key for key, *_ in attempts).values()) == {1}  # each apply attempted once
    assert len(attempts) == 3 and orphan not in {pid for _, _, pid, _, _ in attempts[1:]}
    assert len({pid for _, _, pid, _, _ in attempts[1:]}) == 1  # one host of the id at a time
    for run in [before, *after]:
        assert len(hosts.store.runs.list_tasks(run, kinds=("plan",))) == 1  # each planned once

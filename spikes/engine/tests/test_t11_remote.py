import time

from conftest import count, events, record, wait_until
from spike import journal
from spike.port import TaskCall


def test_t11a_native_site_routing(client, workers, run, engine_name):
    workers.start("cloud1")
    workers.start("agent-hq", labels={"site": "hq"})
    for i in range(10):
        client.submit(TaskCall(task="work", args={"ms": 50}, idempotency_key=f"{run}-hq-{i}",
                               labels={"site": "hq"}), run)
        client.submit(TaskCall(task="work", args={"ms": 50}, idempotency_key=f"{run}-cloud-{i}"), run)
    wait_until(lambda: count(run, "end") >= 20, timeout=90, what="20 tasks")

    placement: dict[str, set] = {"hq": set(), "cloud": set()}
    for e in events(run, "end"):
        placement["hq" if "-hq-" in e["task_id"] else "cloud"].add(e["worker"])
    record(engine_name, "T11a", hq_ran_on=sorted(placement["hq"]),
           cloud_ran_on=sorted(placement["cloud"]))
    assert placement == {"hq": {"agent-hq"}, "cloud": {"cloud1"}}


def test_t11b_gateway_relay(client, workers, run, engine_name):
    """The cloud task posts work for an agent and durably waits for the gateway's signal."""
    workers.start("cloud1")
    ref = client.submit(TaskCall(task="remote_relay", args={"job": "read-sap", "timeout_s": 60},
                                 idempotency_key=f"{run}-relay"), run)
    row = wait_until(lambda: journal.query(
        "select id from spike.agent_task where run = %s and leased_at is null", (run,)),
        timeout=60, what="agent task posted")[0]
    # Act as gateway + agent: lease, do the work, report back.
    journal.execute("update spike.agent_task set leased_at = clock_timestamp() where id = %s", (row["id"],))
    time.sleep(0.5)
    signalled_at = journal.scalar("select clock_timestamp()")
    client.signal(ref, "agent_result", {"rows": 42})
    wait_until(lambda: count(run, "end") >= 1, timeout=60, what="relay task completion")

    done = events(run, "relay_done")[0]
    record(engine_name, "T11b", result=done["info"]["result"],
           signal_to_resume_s=round((done["at"] - signalled_at).total_seconds(), 3))
    assert done["info"]["result"] == {"rows": 42}

"""The push path's crash safety: the server, which runs the sealer, killed between writing a batch's chunks and
`seal_batch`, or between `seal_batch` and submitting the applies, and a worker killed mid-apply of a webhook version.

Whatever is killed is started again, and every accepted event must reach the sink once: none lost, no effective write
twice (see `SlowSink`), and each event in one sealed version only, which is what one version per first event comes to.
"""

import json
import urllib.request
from typing import Any

import pytest

from feather_connect.ids import stable_id
from feather_connect.storage.spool import table_to_records

pytestmark = [pytest.mark.postgres, pytest.mark.chaos]

EVENTS = 30  # two chunks of `CHUNK_ROWS`, so that a kill mid-apply finds a chunk to come


def post(world: Any, event: dict[str, Any]) -> int:
    request = urllib.request.Request(
        f"{world.server_address}/ingress/default/chaos-project/hooks",  # the package's declared webhook
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {world.hook_token}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.status


def keys_by_version(world: Any) -> dict[str, list[str]]:
    """The record keys of each sealed version of the push dataset."""
    found: dict[str, list[str]] = {}
    for version in world.store.versions.list_versions(world.name("hooks"), limit=1000):
        if version.status != "sealed":
            continue
        found[version.version_id] = [
            record.key
            for chunk in world.spool.list_chunks(version.version_id)
            for record in table_to_records(world.spool.get_chunk(version.version_id, chunk.chunk_no))
        ]
    return found


def assert_each_event_sealed_once_and_written_once(world: Any) -> None:
    keys = {str(n) for n in range(1, EVENTS + 1)}
    world.wait(lambda: world.sink_keys("push_sink") == keys, 150, "every event to reach the sink")
    sealed = [key for found in keys_by_version(world).values() for key in found]
    assert sorted(sealed, key=int) == sorted(keys, key=int), "an event in no version, or in two"
    assert world.max_writes_per_key() == 1, world.state()
    world.wait(lambda: set(world.deliveries("push_sink")) == {"applied"}, 60, "every delivery to be applied")
    first_seq = min(e for e in world.conn.execute("select event_seq from feather.inbox").fetchall())[0]
    assert stable_id(world.name("hooks"), "_", str(first_seq)) in keys_by_version(
        world
    )  # named after its first event


@pytest.mark.timeout(400)
@pytest.mark.parametrize("point", ["seal:written", "seal:sealed"])
def test_a_server_killed_mid_seal_loses_and_duplicates_nothing(world: Any, point: str):
    world.start_worker()
    world.arm(point)
    world.start_server()
    assert [post(world, {"id": n}) for n in range(1, EVENTS + 1)] == [202] * EVENTS

    [who] = world.wait_for_point(point)
    assert world.worker_of(who) == "server"
    world.kill("server")
    if point == "seal:written":
        assert keys_by_version(world) == {}  # written, and not sealed
    else:
        assert keys_by_version(world) and world.deliveries("push_sink") == [
            "pending"
        ]  # sealed, not submitted

    world.start_server()  # its sealer finds the events, or its sweeper the pending deliveries
    assert_each_event_sealed_once_and_written_once(world)


@pytest.mark.timeout(400)
def test_a_worker_killed_mid_apply_of_a_webhook_version_loses_and_duplicates_nothing(world: Any):
    point = "apply:push_sink:0"
    worker = world.start_worker()
    world.arm(point)
    world.start_server()
    assert [post(world, {"id": n}) for n in range(1, EVENTS + 1)] == [202] * EVENTS

    [who] = world.wait_for_point(point)
    assert world.worker_of(who) == worker
    world.kill(worker)
    assert world.attempts("push_sink", 0) == [(who, False)]  # it died with the chunk half written

    world.start_worker(worker)
    assert_each_event_sealed_once_and_written_once(world)
    assert world.attempts("push_sink", 0)[-1] == (world.who(worker), True)  # and the chunk was tried again

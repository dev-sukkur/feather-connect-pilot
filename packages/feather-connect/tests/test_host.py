"""A package host's own parts (flow packages §7.1): taking its version's queued runs, the locks on concurrency keys
across versions, and its pipe to the supervisor. The real hosts run in tests/e2e/test_package_hosts.py."""

import os
import threading
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, cast

import pytest

from feather_connect.host import RESUBMIT_S, keyed_tasks, take_queued
from feather_connect.packages.link import HostSide, LinkError, SupervisorSide
from feather_connect.runtime.runtime import Runtime
from feather_connect.storage.memory import MemoryStore


class _Runtime:
    """What `take_queued` uses of a runtime, with a submit that fails while `failing` says so."""

    def __init__(self, store: MemoryStore, flows: list[str]) -> None:
        self.store = store
        self._flows = flows
        self.paused: set[str] = set()
        self.failing = 0
        self.submitted: list[str] = []

    def flows(self) -> list[Any]:
        return [SimpleNamespace(name=f) for f in self._flows]

    def is_flow_paused(self, flow: str) -> bool:
        return flow in self.paused

    def submit_plan(self, flow: str, run_id: str) -> None:
        if self.failing:
            self.failing -= 1
            raise ConnectionError("the database went away mid-submit")
        self.submitted.append(run_id)


def _queue(store: MemoryStore, version: str, flow: str = "acme/feed") -> str:
    return store.runs.create_run(flow, "every:60s", "scheduler", {"package": "acme", "version": version})


def test_a_queued_run_whose_submit_failed_is_taken_on_the_next_pass_and_again_while_it_stays_queued():
    store = MemoryStore()
    runtime = _Runtime(store, ["acme/feed"])
    first, second = _queue(store, "1.0.0"), _queue(store, "1.0.0")
    _queue(store, "1.1.0")  # another version's: never this host's
    submitted: dict[str, float] = {}
    take = cast(Runtime, runtime)
    runtime.failing = 1
    with pytest.raises(ConnectionError):
        take_queued(take, "acme", "1.0.0", submitted, now=0.0)  # the database failed during the take
    assert submitted == {} and runtime.submitted == []  # nothing is taken for submitted that wasn't
    assert take_queued(take, "acme", "1.0.0", submitted, now=1.0) == [first, second]  # oldest first
    assert take_queued(take, "acme", "1.0.0", submitted, now=2.0) == []  # just submitted: left alone
    # Still queued long after: the submit may have been lost, so it goes again, which changes nothing if it wasn't.
    assert take_queued(take, "acme", "1.0.0", submitted, now=1.0 + RESUBMIT_S) == [first, second]
    store.runs.set_run_status(first, "running")
    take_queued(take, "acme", "1.0.0", submitted, now=2.0 + RESUBMIT_S)
    assert first not in submitted  # taken: forgotten


def test_a_queued_run_of_a_paused_flow_is_skipped():
    store = MemoryStore()
    runtime = _Runtime(store, ["acme/feed"])
    runtime.paused.add("acme/feed")
    run = _queue(store, "1.0.0")
    assert take_queued(cast(Runtime, runtime), "acme", "1.0.0", {}) == []
    assert store.runs.get_run(run).status == "skipped"


def test_a_plan_and_an_extract_hold_their_concurrency_keys_across_versions():
    held: list[str] = []
    order: list[str] = []
    locks: dict[str, threading.Lock] = {}

    @contextmanager
    def hold(name: str) -> Generator[None]:
        lock = locks.setdefault(name, threading.Lock())
        with lock:
            held.append(name)
            yield

    def task(ctx: Any, args: Mapping[str, Any]) -> Mapping[str, Any] | None:
        order.append(args["run_id"])
        return None

    tasks = keyed_tasks({"feather.plan": task, "feather.extract": task, "feather.apply": task}, hold)
    tasks["feather.plan"](None, {"run_id": "r1", "flow": "acme/feed"})  # type: ignore[arg-type]
    tasks["feather.extract"](None, {"run_id": "r2", "dataset": "acme/rows", "partition": "2026-11-10"})  # type: ignore[arg-type]
    tasks["feather.apply"](None, {"run_id": "r3"})  # type: ignore[arg-type]
    assert held == ["plan:acme/feed", "extract:acme/rows:2026-11-10"] and order == ["r1", "r2", "r3"]


def test_the_link_answers_requests_and_says_when_the_supervisor_is_gone():
    to_host_r, to_host_w = os.pipe()
    to_sup_r, to_sup_w = os.pipe()
    lost = threading.Event()
    notes: list[Mapping[str, Any]] = []

    def secret(message: Mapping[str, Any]) -> Mapping[str, Any]:
        if message["connection"] != "out":
            raise LinkError("only its own connections")
        return {"value": "tok"}

    supervisor = SupervisorSide(
        os.fdopen(to_host_w, "wb", buffering=0),
        os.fdopen(to_sup_r, "rb", buffering=0),
        {"secret": secret},
        notes.append,
        "h",
    )
    host = HostSide(
        os.fdopen(to_host_r, "rb", buffering=0),
        os.fdopen(to_sup_w, "wb", buffering=0),
        lost.set,
    )
    assert host.request("secret", connection="out", field="token", ref="r")["value"] == "tok"
    with pytest.raises(LinkError, match="only its own"):
        host.request("secret", connection="src", field="token", ref="r")
    host.send("ready")
    for _ in range(100):  # the supervisor reads it on a thread of its own
        if notes:
            break
        threading.Event().wait(0.05)
    supervisor.close()  # the supervisor is gone, killed or not
    assert lost.wait(5)
    with pytest.raises(LinkError, match="gone"):
        host.request("secret", connection="out", field="token", ref="r")
    assert [n["op"] for n in notes] == ["ready"]

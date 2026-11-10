"""The dashboard tests' site: the stack running tests/dashboard/project, seeded with three days of items (`open_site`),
and what the tests look at (`Site`)."""

import json
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
from stack import Stack, open_stack

from feather_server.auth import hash_password, issue_token

PROJECT = Path(__file__).resolve().parents[1] / "dashboard" / "project"
DAYS = ["2026-11-01", "2026-11-02", "2026-11-03"]
USER, PASSWORD = "operator", "correct horse battery staple"
SETTLE = 120  # a run and its deliveries, on a stack polling every 0.2 s


def wait_until(check: Callable[[], object], what: str, timeout: float = SETTLE) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.3)
    raise AssertionError(f"timed out waiting for {what}")


@dataclass
class Site:
    """The seeded stack, its files, and what the tests look at."""

    stack: Stack
    fixture: Path
    capture: Path

    @property
    def url(self) -> str:
        return self.stack.url

    def deliveries(self, route: str, status: str | None = None) -> list[Any]:
        return self.stack.store.ledger.list_deliveries(route=route, status=status)

    def captured(self) -> list[dict[str, Any]]:
        if not self.capture.exists():
            return []
        return [json.loads(line) for line in self.capture.read_text(encoding="utf-8").splitlines() if line]

    def records_path(self, kind: str, route: str | None = None) -> str:
        """The Records of the first day's extract task, or of its apply task to `route`, in the first run."""
        version = self.stack.store.versions.get(self.deliveries("lake")[0].version_id)
        task = next(
            t
            for t in self.stack.store.runs.list_tasks(version.run_id)
            if t.kind == kind and t.partition == version.partition and (route is None or t.route == route)
        )
        return f"/runs/{version.run_id}/tasks/{task.task_id}/records"

    def write_rows(self, per_day: dict[str, int]) -> None:
        write_rows(self.fixture, per_day)


def write_rows(fixture: Path, per_day: dict[str, int]) -> None:
    """Make the source hold `count` items on each day."""
    rows = [{"id": f"{day}-{n}", "day": day, "n": n} for day, count in per_day.items() for n in range(count)]
    fixture.write_text(json.dumps(rows), encoding="utf-8")


def open_site(root: Path) -> Iterator[Site]:
    root.mkdir(parents=True, exist_ok=True)
    fixture, capture = root / "items.json", root / "lake.jsonl"
    env = {"FEATHER_DASHBOARD_FIXTURE": str(fixture), "FEATHER_DASHBOARD_CAPTURE": str(capture)}
    write_rows(fixture, dict.fromkeys(DAYS, 2))
    with open_stack(PROJECT, root, env=env) as stack:
        site = Site(stack, fixture, capture)
        stack.store.users.create_user(USER, hash_password(PASSWORD), "admin")
        token = issue_token(stack.store, "dashboard-tests", timedelta(hours=1), role="operator")
        answer = httpx.post(
            f"{stack.url}/api/v1/flows/demo:run",
            json={"partitions": [f"day={day}" for day in DAYS]},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        assert answer.status_code == 202, answer.text
        wait_until(lambda: len(site.deliveries("lake", "applied")) == len(DAYS), "the lake's deliveries")
        wait_until(lambda: len(site.deliveries("flaky", "failed")) == len(DAYS), "the flaky route to fail")
        seed_package(stack.store, root / "wheels")
        yield site


PACKAGE = "acme-invoices"


def seed_package(store: Any, out: Path) -> None:
    """Two versions of the sample package for the Packages pages: 1.0.0 in service, 1.1.0 ready, a worker's
    environment, a host of 1.1.0 in a crash loop, and a version that failed its load check. No package code runs:
    the load checks are recorded as a worker would."""
    from feather_connect.clock import SystemClock
    from feather_server.auth import Actor
    from feather_server.services.packages import Packages
    from feather_server.services.registries import Registries
    from feather_server.testing import sample_wheel

    clock = SystemClock()
    admin = Actor("human", USER, "admin", USER)
    registries = Registries(store, clock)
    packages = Packages(store, clock, registries, env="dev")
    for version in ("1.0.0", "1.1.0", "1.2.0"):
        wheel = sample_wheel(out / version, version)
        registries.publish(admin, wheel.name, wheel.read_bytes())
        packages.install(admin, PACKAGE, version, reason="the dashboard tests")
    loaded = {"ok": True, "flows": ["invoices"], "seconds": 1.2, "worker": "w1"}
    store.packages.prepared(PACKAGE, "1.0.0", "ready", loaded)
    store.packages.prepared(PACKAGE, "1.1.0", "ready", loaded)
    store.packages.prepared(
        PACKAGE,
        "1.2.0",
        "failed",
        {"ok": False, "check": "load_check", "error": "ImportError: no module named x"},
    )
    now = clock.now()
    store.packages.activate(f"{PACKAGE}/invoices", PACKAGE, "1.0.0", USER, now, "go live")
    store.packages.promote(PACKAGE, "1.0.0", now)
    lock = store.packages.version(PACKAGE, "1.0.0").lock_hash
    store.packages.record_env("w1", lock, "ready", now)
    store.packages.record_host("w1", PACKAGE, "1.0.0", "ready", 0, now)
    store.packages.record_host("w1", PACKAGE, "1.1.0", "crash_loop", 5, now, "boom: the package fell over")

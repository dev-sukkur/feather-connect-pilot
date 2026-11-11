"""M1's exit criteria on the real stack: a webhook reaches a sink within 5 s at p95 (FR-B10, NFR-01), a burst of ten times
the rate loses nothing and recovers (H14), and schedules run by themselves through a real engine, across a restart of the
server.

Latency runs from the server's stamp on receipt to the sink's write, both by this machine's clock. The load generator's
achieved rate is asserted too, or a slow generator would pass for a fast system. What was measured is printed, with `-s`.
"""

import ctypes
import itertools
import json
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Self

import pytest
from real_stack import wait_until

pytestmark = [pytest.mark.postgres, pytest.mark.perf]

PROJECT = Path(__file__).with_name("perf_project")
SCHEDULES = Path(__file__).with_name(
    "perf_schedules"
)  # a package of its own, so no poller ticks beside a latency check
HOOKS = "perf-project/perf_hooks"
LOAD = Path(__file__).parents[1] / "support" / "webhook_load.py"
SECRET = "perf-signature-secret"
DDL = """
create schema perf;
create table perf.sink_rows (
  key text not null, sent_at float8, received_at float8, applied_at float8 not null
);
create index on perf.sink_rows (key);
"""

P95_TARGET_S = 5.0
RECOVERY_TARGET_S = 300.0
BASE_RATE, BURST_RATE = 50.0, 500.0
ACHIEVED = 0.97  # how much of the asked-for rate must really have been sent, for a number to mean anything
DRAIN_S = 120.0  # how long the rest may take to arrive once sending has ended, counted as lost after that
BUCKET_S = 10
STAYS_RECOVERED_S = 30  # how long p95 has to stay under target to count as recovered
TICK = timedelta(seconds=5)
POLLER, BY_CRON = "perf-schedules/perf_poller", "perf-schedules/perf_by_cron"
TRIGGER = "every:5s"


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]


class Cpu:
    """Samples how busy the whole machine is, each second, for a failure to be read against. Load of its own included:
    the test's processes count, and so do the other projects' containers."""

    def __init__(self) -> None:
        self.samples: list[tuple[float, float]] = []  # time, percent busy
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    @staticmethod
    def _times() -> tuple[float, float]:
        """Return the idle and the total CPU time of the machine, in some unit, since it started."""
        if sys.platform == "win32":
            idle, kernel, user = ctypes.c_ulonglong(), ctypes.c_ulonglong(), ctypes.c_ulonglong()
            ctypes.windll.kernel32.GetSystemTimes(  # type: ignore[attr-defined]
                ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)
            )
            return float(idle.value), float(kernel.value + user.value)  # kernel time includes idle
        with open("/proc/stat", encoding="ascii") as stat:
            fields = [float(part) for part in stat.readline().split()[1:]]
        return fields[3] + fields[4], sum(fields)

    def _run(self) -> None:
        idle, total = self._times()
        while not self._stop.wait(1.0):
            now_idle, now_total = self._times()
            spent = now_total - total
            if spent > 0:
                self.samples.append((time.time(), 100 * (1 - (now_idle - idle) / spent)))
            idle, total = now_idle, now_total

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(5)

    def summary(self, start: float = 0.0, end: float = float("inf")) -> dict[str, float]:
        busy = sorted(value for at, value in self.samples if start <= at <= end)
        if not busy:
            return {}
        return {
            "mean": round(sum(busy) / len(busy), 1),
            "p95": round(p95(busy), 1),
            "max": round(busy[-1], 1),
        }


@dataclass
class Load:
    """A load generator running as a process, and what it reported once stopped."""

    proc: subprocess.Popen[bytes]
    out: Path
    report: dict[str, Any] = field(default_factory=dict[str, Any])

    def end_last_phase(self) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(b"stop\n")
        self.proc.stdin.flush()

    def finish(self, timeout: float) -> dict[str, Any]:
        self.proc.wait(timeout=timeout)
        assert self.proc.returncode == 0
        self.report = json.loads(self.out.read_text())
        return self.report


def start_load(url: str, out: Path, *phases: tuple[float, float]) -> Load:
    specs = [f"{rate}:{seconds}" for rate, seconds in phases]
    proc = subprocess.Popen([sys.executable, str(LOAD), url, SECRET, str(out), *specs], stdin=subprocess.PIPE)
    return Load(proc, out)


def rows_of(stack: Any, since: float = 0.0) -> list[tuple[str, float, float, float]]:
    """Return each key's first write: key, sent, received (the server's, from the inbox), applied."""
    return stack.conn.execute(
        """select s.key, min(s.sent_at), min(extract(epoch from i.received_at))::float8, min(s.applied_at)
           from perf.sink_rows s join feather.inbox i on i.dataset = %s and i.event_id = s.key
           group by s.key having min(extract(epoch from i.received_at)) >= %s""",
        (HOOKS, since),
    ).fetchall()


def writes_of(stack: Any) -> int:
    """Every write the sink took, a repeat of a key included."""
    found = stack.conn.execute("select count(*) from perf.sink_rows").fetchone()
    return found[0]


def duplicates_of(stack: Any) -> int:
    """The writes of a key past its first."""
    found = stack.conn.execute("select count(*) - count(distinct key) from perf.sink_rows").fetchone()
    return found[0]


def wait_for_arrival(stack: Any, accepted: list[int], timeout: float) -> float:
    """Wait until all that was accepted has reached the sink, or `timeout`, and return the seconds it took."""
    started = time.monotonic()
    want = {str(key) for key in accepted}
    deadline = started + timeout
    while time.monotonic() < deadline:
        if want <= {row[0] for row in stack.conn.execute("select distinct key from perf.sink_rows")}:
            break
        time.sleep(1)
    return time.monotonic() - started


def count_lost(report: dict[str, Any], rows: list[tuple[str, float, float, float]]) -> int:
    return len({str(key) for key in report["accepted"]} - {row[0] for row in rows})


def check_rates(report: dict[str, Any]) -> None:
    """Fail if the generator or the server's answers kept it from sending what was asked for."""
    for phase in report["phases"]:
        assert phase["achieved"] >= phase["rate"] * ACHIEVED, f"not sent at the rate asked for: {phase}"


def buckets(rows: list[tuple[str, float, float, float]], start: float) -> dict[int, list[float]]:
    """Return latencies by which `BUCKET_S` after `start` the event was received in."""
    found: dict[int, list[float]] = {}
    for _, _, received, applied in rows:
        found.setdefault(int((received - start) // BUCKET_S), []).append(applied - received)
    return found


def recovered_after(rows: list[tuple[str, float, float, float]], burst_end: float) -> float | None:
    """Seconds from the burst's end to the start of the first bucket from which p95 stays under target for good, given
    `STAYS_RECOVERED_S` of good ones at least. None if that hasn't happened."""
    by_bucket = {n: p95(values) for n, values in buckets(rows, burst_end).items() if n >= 0}
    if not by_bucket:
        return None
    last = max(by_bucket) - 1  # the newest bucket may still be arriving
    first_good = last + 1
    for n in range(last, -1, -1):
        if by_bucket.get(n, float("inf")) > P95_TARGET_S:  # none yet: they wait behind what is on its way
            break
        first_good = n
    if (last + 1 - first_good) * BUCKET_S < STAYS_RECOVERED_S:
        return None
    return float(first_good * BUCKET_S)


def print_numbers(name: str, numbers: dict[str, Any]) -> None:
    print(f"\n{name}: {json.dumps(numbers)}")


@pytest.fixture
def push_stack(stack: Callable[..., Any]) -> Any:
    return stack(PROJECT, env={"HOOK_SECRET": SECRET}, ddl=DDL)


@pytest.mark.timeout(420)
def test_webhook_to_sink_p95_within_5s(push_stack: Any, tmp_path: Path, record_property: Any) -> None:
    seconds = 60
    with Cpu() as cpu:
        load = start_load(push_stack.url, tmp_path / "load.json", (BASE_RATE, seconds))
        report = load.finish(timeout=seconds + 60)
        drained = wait_for_arrival(push_stack, report["accepted"], DRAIN_S)

    rows = rows_of(push_stack)
    latencies = [applied - received for _, _, received, applied in rows]
    lost = count_lost(report, rows)
    start = min(row[2] for row in rows)
    end = max(row[2] for row in rows)
    first = [applied - received for _, _, received, applied in rows if received < start + 10]
    last = [applied - received for _, _, received, applied in rows if received >= end - 10]
    numbers = {
        "accepted": len(report["accepted"]),
        "lost": lost,
        "writes": writes_of(push_stack),
        "duplicates": duplicates_of(push_stack),
        "statuses": report["statuses"],
        "p50": round(sorted(latencies)[len(latencies) // 2], 3),
        "p95": round(p95(latencies), 3),
        "max": round(max(latencies), 3),
        "p95_first_10s": round(p95(first), 3),
        "p95_last_10s": round(p95(last), 3),
        "p95_from_send": round(p95([applied - sent for _, sent, _, applied in rows]), 3),
        "drained_s": round(drained, 1),
        "phases": report["phases"],
        "cpu": cpu.summary(),
    }
    print_numbers("webhook latency", numbers)
    record_property("numbers", json.dumps(numbers))
    assert set(report["statuses"]) == {"202"}, report["refused"]
    check_rates(report)
    assert lost == 0, numbers
    assert numbers["writes"] == numbers["accepted"] and numbers["duplicates"] == 0, numbers
    assert p95(latencies) <= P95_TARGET_S, numbers


@pytest.mark.timeout(900)
def test_burst_at_10x_loses_nothing_and_recovers(
    push_stack: Any, tmp_path: Path, record_property: Any
) -> None:
    before, burst = 30, 30
    most_after = RECOVERY_TARGET_S + STAYS_RECOVERED_S + 2 * BUCKET_S
    sends_from = time.time()
    with Cpu() as cpu:
        load = start_load(
            push_stack.url,
            tmp_path / "load.json",
            (BASE_RATE, before),
            (BURST_RATE, burst),
            (BASE_RATE, most_after),
        )
        burst_end = sends_from + before + burst  # about right: the report has it exactly, for the numbers
        recovered: float | None = None
        deadline = time.monotonic() + before + burst + most_after
        while recovered is None and time.monotonic() < deadline and load.proc.poll() is None:
            time.sleep(5)
            if time.time() > burst_end:
                recovered = recovered_after(rows_of(push_stack), burst_end)
        if load.proc.poll() is None:
            load.end_last_phase()
        report = load.finish(timeout=120)
        drained = wait_for_arrival(push_stack, report["accepted"], DRAIN_S)

    rows = rows_of(push_stack)
    lost = count_lost(report, rows)
    burst_end = report["phases"][1]["wall_ended"]
    by_bucket = {n * BUCKET_S: round(p95(v), 3) for n, v in sorted(buckets(rows, burst_end).items())}
    recovered = recovered_after(rows, burst_end)
    numbers = {
        "accepted": len(report["accepted"]),
        "lost": lost,
        "writes": writes_of(push_stack),
        "duplicates": duplicates_of(push_stack),
        "statuses": report["statuses"],
        "refused": report["refused"][:5],
        "seconds_until_p95_recovers": recovered,
        "p95_by_10s_from_burst_end": by_bucket,
        "drained_s": round(drained, 1),
        "phases": report["phases"],
        "cpu_burst": cpu.summary(burst_end - burst, burst_end),
        "cpu": cpu.summary(),
    }
    print_numbers("burst", numbers)
    record_property("numbers", json.dumps(numbers))
    assert set(report["statuses"]) == {"202"}, numbers
    check_rates(report)
    assert lost == 0, numbers
    assert numbers["writes"] == numbers["accepted"] and numbers["duplicates"] == 0, numbers
    assert recovered is not None and recovered <= RECOVERY_TARGET_S, numbers


# Schedules. Neither test sends the stack anything: `start` makes the stack, and only the clock moves it on.


@dataclass
class Schedules:
    stack: Any

    def tick_runs(self) -> list[Any]:
        """The poller's ticks. A packaged flow's tick is a run of its own, queued for its version's host, not a task
        of a day's stream run as in M1 (an M2 item: flow packages §12)."""
        return [r for r in self.stack.store.runs.list_runs(flow=POLLER, limit=200) if r.trigger == TRIGGER]

    def tick_times(self) -> list[datetime]:
        """The due time of every tick that went ahead, oldest first, with repeats if a tick fired twice."""
        return sorted(
            datetime.fromisoformat(r.params["due_at"]) for r in self.tick_runs() if r.status != "skipped"
        )

    def skipped_ticks(self) -> list[str | None]:
        return [r.reason for r in self.tick_runs() if r.status == "skipped"]

    def cron_runs(self) -> list[Any]:
        return self.stack.store.runs.list_runs(flow=BY_CRON, limit=50)

    def versions(self) -> list[Any]:
        return self.stack.store.versions.list_versions("perf-schedules/perf_ticks")


@pytest.fixture
def schedules(stack: Callable[..., Any]) -> Schedules:
    return Schedules(stack(SCHEDULES, ddl=DDL))


def assert_no_tick_fired_twice(found: Schedules) -> None:
    ticks = found.tick_times()
    assert len(ticks) == len(set(ticks)), ticks
    assert all((b - a) % TICK == timedelta(0) for a, b in itertools.pairwise(ticks)), ticks


def assert_each_minute_fired_once(found: Schedules) -> None:
    minutes = [run.created_at.replace(second=0, microsecond=0) for run in found.cron_runs()]
    assert len(minutes) == len(set(minutes)), minutes
    assert all(run.trigger == "cron:* * * * *:UTC" for run in found.cron_runs())


@pytest.mark.timeout(300)
def test_scheduled_flow_runs_by_itself(schedules: Schedules) -> None:
    wanted_ticks, wanted_cron = 6, 2
    wait_until(
        lambda: len(schedules.tick_times()) >= wanted_ticks and len(schedules.cron_runs()) >= wanted_cron,
        180,
    )
    wait_until(lambda: writes_of(schedules.stack) >= wanted_ticks, 30)

    runs = schedules.tick_runs()
    versions = schedules.versions()
    ticks = schedules.tick_times()
    numbers = {
        "tick_runs": len(runs),
        "ticks": len(ticks),
        "versions": len(versions),
        "skipped": schedules.skipped_ticks(),
        "cron_runs": len(schedules.cron_runs()),
        "writes": writes_of(schedules.stack),
    }
    print_numbers("schedules", numbers)
    assert all(run.status in ("succeeded", "skipped", "running", "queued") for run in runs), numbers
    assert len({v.version_id for v in versions}) == len(versions) >= wanted_ticks - 1, numbers
    assert {v.run_id for v in versions} <= {run.run_id for run in runs}, numbers
    assert_no_tick_fired_twice(schedules)
    assert_each_minute_fired_once(schedules)
    assert {run.actor for run in schedules.cron_runs()} == {"scheduler"}


@pytest.mark.timeout(420)
def test_restarting_the_server_keeps_scheduling(schedules: Schedules) -> None:
    wait_until(lambda: len(schedules.tick_times()) >= 3 and schedules.cron_runs(), 150)
    ticks_before = len(schedules.tick_times())
    cron_before = len(schedules.cron_runs())

    schedules.stack.kill_server()
    time.sleep(7)  # long enough that a tick falls due with no server
    schedules.stack.start_server()

    wait_until(
        lambda: (
            len(schedules.tick_times()) >= ticks_before + 3 and len(schedules.cron_runs()) >= cron_before + 1
        ),
        150,
    )
    numbers = {
        "ticks_before": ticks_before,
        "ticks_after": len(schedules.tick_times()),
        "cron_before": cron_before,
        "cron_after": len(schedules.cron_runs()),
        "skipped": schedules.skipped_ticks(),
    }
    print_numbers("restart", numbers)
    assert_no_tick_fired_twice(schedules)
    assert_each_minute_fired_once(schedules)

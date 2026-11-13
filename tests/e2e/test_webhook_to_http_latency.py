"""Webhook to HTTP p95 within 5 s on the real connectors (M1 exit criterion): signed events at 50 a second for 60 s into
feather-http's `Webhook`, out through its `Call` to an echo on this machine, with nothing lost.

Latency here runs from when the load generator sent an event to when the echo received the call, both by this machine's
clock, so it includes the ingress request, unlike tests/e2e/test_webhook_latency.py. The worker polls at the default
0.5 s. For a number on a durable database (S19), point it at one with fsync on:

    docker run -d --name feather-latency-postgres -p 127.0.0.1:25434:5432 -e POSTGRES_USER=feather \
        -e POSTGRES_PASSWORD=feather -e POSTGRES_DB=feather_test postgres:17
    FEATHER_TEST_DATABASE_URL=postgresql://feather:feather@127.0.0.1:25434/feather_test \
        uv run pytest tests/e2e/test_webhook_to_http_latency.py -s
    docker rm -fv feather-latency-postgres  # -v: its data volume too

`FEATHER_LATENCY_OUT` names a file to write the numbers to, as JSON.
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from perf_results import record
from real_stack import free_port, kill_abruptly, server_url, wait_until

pytestmark = [pytest.mark.postgres, pytest.mark.perf]

PROJECT = Path(__file__).with_name("http_project")
SUPPORT = Path(__file__).parents[1] / "support"
SECRET = "http-latency-secret"
RATE, SECONDS = 50.0, 60
P95_TARGET_S = 5.0
ACHIEVED = 0.97  # how much of the asked-for rate must really have been sent, for the number to mean anything
DRAIN_S = 120.0
POLL_S = 0.5  # the worker's default


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]


def arrivals(out: Path) -> dict[str, tuple[float, float]]:
    """Each id's first call at the echo: when it was sent, and when it arrived."""
    found: dict[str, tuple[float, float]] = {}
    if out.exists():
        for line in out.read_text(encoding="utf-8").splitlines():
            if line:
                row = json.loads(line)
                found.setdefault(row["id"], (row["sent_at"], row["arrived_at"]))
    return found


@pytest.mark.timeout(600)
def test_webhook_to_http_p95_within_5s(
    stack: Callable[..., Any], tmp_path: Path, record_property: Any
) -> None:
    calls = tmp_path / "calls.jsonl"
    port = free_port()
    echo = subprocess.Popen([sys.executable, str(SUPPORT / "http_echo.py"), str(port), str(calls)])
    try:

        def listening() -> bool:
            with socket.socket() as probe:
                return probe.connect_ex(("127.0.0.1", port)) == 0

        wait_until(listening, 30)
        project = tmp_path / "project"
        shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns("__pycache__"))
        overlay = {"connections": {"echo": {"base_url": f"http://127.0.0.1:{port}"}}}
        (project / "env" / "dev.yaml").write_text(json.dumps(overlay), encoding="utf-8")  # JSON is YAML
        running = stack(project, env={"HOOK_SECRET": SECRET}, poll_s=POLL_S)

        report_file = tmp_path / "load.json"
        load = subprocess.Popen(
            [sys.executable, str(SUPPORT / "webhook_load.py"), running.url, SECRET, str(report_file)]
            + [f"{RATE}:{SECONDS}"],
            stdin=subprocess.PIPE,  # a line on its stdin ends the load early, and pytest's stdin is at its end
        )
        load.wait(timeout=SECONDS + 60)
        assert load.returncode == 0
        report = json.loads(report_file.read_text())
        accepted = {str(n) for n in report["accepted"]}
        deadline = time.monotonic() + DRAIN_S
        while time.monotonic() < deadline and not accepted <= set(arrivals(calls)):
            time.sleep(1)
        found = arrivals(calls)
        calls_made = sum(1 for line in calls.read_text(encoding="utf-8").splitlines() if line)
        latencies = [arrived - sent for key, (sent, arrived) in found.items() if key in accepted]
        numbers = {
            "database": server_url().rsplit("@", 1)[-1],
            "poll_s": POLL_S,
            "accepted": len(accepted),
            "lost": len(accepted - set(found)),
            "calls": calls_made,
            "repeats": calls_made - len(found),
            "statuses": report["statuses"],
            "p50": round(sorted(latencies)[len(latencies) // 2], 3) if latencies else None,
            "p95": round(p95(latencies), 3) if latencies else None,
            "max": round(max(latencies), 3) if latencies else None,
            "phases": report["phases"],
        }
        print(f"\nwebhook to http latency: {json.dumps(numbers)}")
        record_property("numbers", json.dumps(numbers))
        if numbers["p95"] is not None:
            record("NFR-01", "webhook_to_http_p95", numbers["p95"], "s", P95_TARGET_S, higher_is_better=False)
        if out := os.environ.get("FEATHER_LATENCY_OUT"):
            Path(out).write_text(json.dumps(numbers, indent=2), encoding="utf-8")
        assert set(report["statuses"]) == {"202"}, report["refused"]
        for phase in report["phases"]:
            assert phase["achieved"] >= phase["rate"] * ACHIEVED, f"not sent at the rate asked for: {phase}"
        assert numbers["lost"] == 0, numbers
        assert p95(latencies) <= P95_TARGET_S, numbers
    finally:
        kill_abruptly(echo)

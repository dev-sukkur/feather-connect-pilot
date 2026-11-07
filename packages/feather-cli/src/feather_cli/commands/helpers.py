"""The helper processes a project declares under `dev.processes`, such as simulators of the systems its flows talk to.

`feather dev` starts them in the project directory before the stack, keeps them running while the flows reload, and
stops them when it stops. Their output goes to this console, each line prefixed by the process's name.
"""

import os
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import IO

from feather_connect.project import DevProcess

HOST = "127.0.0.1"
READY_S = 30.0  # how long a helper may take to listen on its port
STOP_S = 10.0


class HelperError(RuntimeError):
    """A helper could not be started: its port is busy, it ended, or it did not listen in time."""


def busy_ports(processes: Sequence[DevProcess], in_use: Callable[[int], bool]) -> list[DevProcess]:
    """The processes whose port something on this machine already uses."""
    return [p for p in processes if p.port is not None and in_use(p.port)]


def _listening(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.5)
        return probe.connect_ex((HOST, port)) == 0


def _command(process: DevProcess) -> list[str]:
    """The command to run, a leading `python` being the interpreter that runs feather."""
    first, *rest = process.command
    return [sys.executable if first in ("python", "python3") else first, *rest]


def _relay(name: str, stream: IO[str], echo: Callable[[str], None]) -> None:
    for line in stream:
        echo(f"[{name}] {line.rstrip()}")


class Helpers:
    """The running helper processes of one `feather dev`."""

    def __init__(self, root: Path, echo: Callable[[str], None]) -> None:
        self._root = root
        self._echo = echo
        self._running: list[tuple[DevProcess, subprocess.Popen[str], threading.Thread]] = []

    def start(self, processes: Sequence[DevProcess]) -> None:
        """Start each process and wait for it to listen on its port. On failure the ones started are stopped."""
        try:
            for process in processes:
                self._start(process)
        except BaseException:
            self.stop()
            raise

    def _start(self, process: DevProcess) -> None:
        # In a group of its own, so that the console's Ctrl+C reaches feather only, which then stops it.
        windows = os.name == "nt"
        try:
            child = subprocess.Popen(
                _command(process),
                cwd=self._root,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if windows else 0,
                start_new_session=not windows,
            )
        except OSError as error:
            raise HelperError(f"could not start {process.name}: {error.strerror or error}") from None
        assert child.stdout is not None
        relay = threading.Thread(
            target=_relay,
            args=(process.name, child.stdout, self._echo),
            name=f"dev-{process.name}",
            daemon=True,
        )
        relay.start()
        self._running.append((process, child, relay))
        if process.port is None:
            return
        deadline = time.monotonic() + READY_S
        while not _listening(process.port):
            if child.poll() is not None:
                raise HelperError(
                    f"{process.name} ended with exit code {child.returncode} before it listened"
                )
            if time.monotonic() > deadline:
                raise HelperError(
                    f"{process.name} did not listen on port {process.port} within {READY_S:.0f} s"
                )
            time.sleep(0.1)
        self._echo(f"[{process.name}] listening on port {process.port}")

    def stop(self) -> None:
        """Stop every process, the last started first, killing one that does not end within `STOP_S`."""
        for process, child, relay in reversed(self._running):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(STOP_S)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(STOP_S)
            relay.join(STOP_S)  # the last of its output, before the line that says it stopped
            self._echo(f"[{process.name}] stopped")
        self._running = []

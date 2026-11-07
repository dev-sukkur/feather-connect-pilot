"""The helper processes a project declares under `dev.processes`, such as simulators of the systems its flows talk to.

`feather dev` starts them in the project directory before the stack, keeps them running while the flows reload, and
stops them when it stops. Their output goes to this console, each line prefixed by the process's name. On Windows
they are in a job object that ends them when feather ends, however it ends, so that a killed feather leaves no helper
holding its port.
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


class HelperStopped(HelperError):
    """`feather dev` was asked to stop while a helper started."""


def busy_ports(processes: Sequence[DevProcess], in_use: Callable[[int], bool]) -> list[DevProcess]:
    """The processes whose port something on this machine already uses."""
    return [p for p in processes if p.port is not None and in_use(p.port)]


def clashing_ports(processes: Sequence[DevProcess], server_port: int) -> list[DevProcess]:
    """The processes that want the port the server takes."""
    return [p for p in processes if p.port == server_port]


def _kill_on_close_job() -> int | None:
    """A Windows job object that ends its processes when its last handle closes, that is when feather ends; None
    elsewhere, or if Windows refuses one."""
    if sys.platform != "win32":
        return None
    import ctypes
    from ctypes import wintypes

    class Basic(ctypes.Structure):
        _fields_ = [
            ("per_process_time", ctypes.c_int64),
            ("per_job_time", ctypes.c_int64),
            ("limit_flags", wintypes.DWORD),
            ("min_working_set", ctypes.c_size_t),
            ("max_working_set", ctypes.c_size_t),
            ("active_processes", wintypes.DWORD),
            ("affinity", ctypes.c_size_t),
            ("priority_class", wintypes.DWORD),
            ("scheduling_class", wintypes.DWORD),
        ]

    class Extended(ctypes.Structure):
        _fields_ = [("basic", Basic), ("io", ctypes.c_uint64 * 6), ("sizes", ctypes.c_size_t * 4)]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    job = kernel32.CreateJobObjectW(None, None)
    info = Extended()
    info.basic.limit_flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    # 9 is JobObjectExtendedLimitInformation.
    if not job or not kernel32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
        return None
    return job


def _join_job(job: int, pid: int) -> None:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    process = kernel32.OpenProcess(0x0101, False, pid)  # PROCESS_SET_QUOTA | PROCESS_TERMINATE
    if process:
        kernel32.AssignProcessToJobObject(job, process)
        kernel32.CloseHandle(process)


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
        self._job: int | None = None

    def start(self, processes: Sequence[DevProcess], stop: threading.Event | None = None) -> None:
        """Start each process and wait for it to listen on its port, or until `stop` is set, which raises
        `HelperStopped`. On failure the ones started are stopped."""
        stop = stop or threading.Event()
        try:
            for process in processes:
                self._start(process, stop)
        except BaseException:
            self.stop()
            raise

    def _start(self, process: DevProcess, stop: threading.Event) -> None:
        if stop.is_set():
            raise HelperStopped(f"stopped before {process.name} started")
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
        if self._job is None:
            self._job = _kill_on_close_job()
        if self._job is not None:
            _join_job(self._job, child.pid)
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
            if stop.is_set():
                raise HelperStopped(f"stopped while {process.name} started")
            if child.poll() is not None:
                raise HelperError(
                    f"{process.name} ended with exit code {child.returncode} before it listened"
                )
            if time.monotonic() > deadline:
                raise HelperError(
                    f"{process.name} did not listen on port {process.port} within {READY_S:.0f} s"
                )
            stop.wait(0.1)
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

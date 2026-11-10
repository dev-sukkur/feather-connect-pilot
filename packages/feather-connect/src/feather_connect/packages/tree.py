"""A child process and its whole tree, killed together however it ends (flow packages: the load check, environment
builds and package hosts). A process group on POSIX; a Job Object set to kill on close on Windows.

On Windows the child starts suspended and is resumed only once it is in the Job Object, so nothing it starts, such as
the base interpreter a virtual environment's `python.exe` launches, can get out of the job before it is made. The job
is killed when its last handle closes, so a tree outlives neither `kill` nor the death of the process that made it."""

import os
import subprocess
import sys
from typing import Any, cast

__all__ = ["ProcessTree"]


class ProcessTree:
    """A child process and every process it starts: a process group on POSIX, a Job Object on Windows that kills
    them all when it is closed. On Windows the child gets a process group of its own too, so that `terminate` can
    send it CTRL_BREAK_EVENT."""

    def __init__(self, command: list[str], **options: Any) -> None:
        if sys.platform == "win32":
            self.process = subprocess.Popen(
                command, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | _CREATE_SUSPENDED, **options
            )
            self._job = _job_for(self.process)
            _resume(self.process)
        else:
            self.process = subprocess.Popen(command, start_new_session=True, **options)

    def terminate(self) -> None:
        """Ask the child to stop, as SIGTERM does on POSIX and CTRL_BREAK_EVENT on Windows. It may take its time."""
        if self.process.poll() is not None:
            return
        import signal

        try:
            if sys.platform == "win32":
                self.process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                self.process.send_signal(signal.SIGTERM)
        except OSError:
            pass

    def kill(self) -> None:
        """Kill the whole tree and reap the child. Safe to call more than once."""
        if sys.platform == "win32":
            if self._job is not None:
                _close_job(self._job)
                self._job = None
            if self.process.poll() is None:
                self.process.kill()
        else:
            import signal

            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - a process SIGKILL doesn't end
            pass


if sys.platform == "win32":  # defined only where they run, so a type checker for another platform skips them
    _CREATE_SUSPENDED = 0x00000004

    def _resume(process: "subprocess.Popen[Any]") -> None:
        """Resume every thread of a process started suspended (`NtResumeProcess`)."""
        import ctypes
        from ctypes import wintypes

        ntdll = ctypes.WinDLL("ntdll")
        ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]
        handle = int(cast(Any, process)._handle)  # pyright: ignore[reportPrivateUsage]
        status = ntdll.NtResumeProcess(handle)
        if status != 0:
            process.kill()
            raise OSError(status, "the child process could not be resumed")

    def _job_for(process: "subprocess.Popen[Any]") -> int | None:
        """A Job Object holding `process`, set to kill every process in it when its handle is closed."""
        import ctypes
        from ctypes import wintypes

        class Basic(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class Extended(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", Basic),
                ("IoInfo", ctypes.c_uint64 * 6),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            process.kill()
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
        limits = Extended()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        extended_limit_information = 9
        set_ok = kernel32.SetInformationJobObject(
            job, extended_limit_information, ctypes.byref(limits), ctypes.sizeof(limits)
        )
        handle = int(cast(Any, process)._handle)  # pyright: ignore[reportPrivateUsage]
        if not set_ok or not kernel32.AssignProcessToJobObject(job, handle):
            error = ctypes.get_last_error()
            _close_job(job)
            process.kill()
            raise OSError(error, "the load check's process could not be put in a Job Object")
        return int(job)

    def _close_job(job: int) -> None:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.TerminateJobObject(job, 1)
        kernel32.CloseHandle(job)

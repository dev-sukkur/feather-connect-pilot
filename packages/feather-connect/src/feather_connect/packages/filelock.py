"""Locks on files, between processes on one machine (flow packages task 8): what lets two supervisors share one cache of
environments. An exclusive lock on the file's first byte, `fcntl.flock` on POSIX and `msvcrt.locking` on Windows,
which the system lets go of when the process holding it ends, however it ends."""

import os
import sys
import time
from pathlib import Path

__all__ = ["FileLock", "is_held"]


class FileLock:
    """An exclusive lock on `path`, which is made if it isn't there. Not reentrant; one holder per instance."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def try_acquire(self) -> bool:
        """Take the lock if nobody holds it, and return whether this did."""
        if self._fd is not None:
            return True
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        if _lock(fd):
            self._fd = fd
            return True
        os.close(fd)
        return False

    def acquire(self, timeout: float) -> bool:
        """Wait up to `timeout` seconds for the lock, and return whether this took it."""
        deadline = time.monotonic() + timeout
        while not self.try_acquire():
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.05)
        return True

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            _unlock(fd)
            os.close(fd)

    def __enter__(self) -> "FileLock":
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


def is_held(path: Path) -> bool:
    """Whether some process holds the lock on `path` now. A file nobody holds says nothing."""
    if not path.is_file():
        return False
    probe = FileLock(path)
    try:
        if probe.try_acquire():
            return False
    except OSError:
        return True
    finally:
        probe.release()
    return True


if sys.platform == "win32":
    import msvcrt

    def _lock(fd: int) -> bool:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def _unlock(fd: int) -> None:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

else:
    import fcntl

    def _lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        return True

    def _unlock(fd: int) -> None:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass

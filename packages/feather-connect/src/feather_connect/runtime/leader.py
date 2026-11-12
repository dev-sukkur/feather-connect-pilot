"""Leader election for the loops that only one process may run (design 6.12).

A loop runner asks its lock each pass. `PostgresLeaderLock` (in `storage.postgres.leader`, where psycopg may be
imported) is an advisory lock of a session that stays open for as long as the lock is held, so a process that dies, or
loses its connection, gives up leadership without saying so, and another takes over on its next pass.
"""

import zlib
from typing import Protocol

# Advisory locks share one key space with the partition lock of `storage.postgres.support`, which sits far below.
_BASE = 7_262_411_000_000_000


def leader_key(name: str, tenant: str = "default") -> int:
    """Return a stable lock key for the loop called `name` in `tenant`. The default tenant's keys are those of M1, so
    that a server of before and one of after never both lead; another tenant's loops lead on their own (ADR-0013 rule
    7)."""
    return _BASE + zlib.crc32((name if tenant == "default" else f"{tenant}/{name}").encode())


class LeaderLock(Protocol):
    """A lock that at most one process holds."""

    def try_acquire(self) -> bool:
        """Return True if this process holds the lock now, taking it if it is free. Asking again while holding it
        returns True, and returns False once the hold has been lost."""
        ...

    def release(self) -> None:
        """Give the lock up. It does nothing when the lock is not held."""
        ...


class AlwaysLeader(LeaderLock):
    """The lock of a single process, which leads whenever it is asked."""

    def try_acquire(self) -> bool:
        return True

    def release(self) -> None:
        pass

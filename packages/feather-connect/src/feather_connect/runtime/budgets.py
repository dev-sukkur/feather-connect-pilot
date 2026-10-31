"""The handle a connector meters its calls with: it takes units from a named budget and waits when there are none."""

import time
from collections.abc import Callable
from datetime import timedelta

from feather_connect.core.errors import BudgetDeferred
from feather_connect.storage.port import BudgetStore, Priority

# A wait shorter than this is slept in the process. A longer one is left to the runtime, to sleep durably.
_SLEEP_BELOW = timedelta(seconds=60)


class BudgetHandle:
    """One connection's view of a budget, at the priority of the work it does. This is `Context.budget`.

    `sleep` takes seconds, and is what a short wait is spent in.
    """

    def __init__(
        self,
        store: BudgetStore,
        name: str,
        priority: Priority = "normal",
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._store = store
        self.name = name
        self.priority: Priority = priority
        self._sleep = sleep

    def acquire(self, units: int = 1) -> None:
        """Take `units` from the budget. Returns once they are taken.

        A wait under 60 seconds is slept off here. Any longer one raises `BudgetDeferred`, with the wait and the name.
        """
        while True:
            grant = self._store.acquire(self.name, units, self.priority)
            if grant.granted:
                return
            if grant.wait >= _SLEEP_BELOW:
                raise BudgetDeferred(grant.wait, self.name)
            self._sleep(grant.wait.total_seconds())

"""Errors raised by sources, sinks and the runtime. The runtime decides what to do from the type."""

from datetime import timedelta


class MissingKeyError(Exception):
    """A record has no value for one of its key fields."""


class PartitionError(Exception):
    """A record's partition field is missing or can't be read, so the record has no partition."""


class TransientError(Exception):
    """A failure that may go away if tried again, such as a timeout or a rate limit.

    `retry_after` is how many seconds the other side asked us to wait, when it said.
    """

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PermanentError(Exception):
    """A failure that must not be retried."""


class BudgetDeferred(Exception):
    """A call has to wait for budget: try again after `wait`. `budget` names the budget that ran out."""

    def __init__(self, wait: timedelta, budget: str) -> None:
        # `args` stays the constructor arguments, so that pickle and copy can rebuild the error.
        super().__init__(wait, budget)
        self.wait = wait
        self.budget = budget

    def __str__(self) -> str:
        return f"budget {self.budget!r} is used up; try again in {self.wait}"


class RecordRejected(Exception):
    """One record can't be applied, for example because it is invalid.

    The record is quarantined with this message and the rest of the version carries on.
    """

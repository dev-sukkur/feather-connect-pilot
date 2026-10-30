import pickle
from datetime import timedelta

from feather_connect.core.errors import BudgetDeferred, TransientError


def test_transient_error_carries_the_retry_after_hint():
    assert TransientError("busy").retry_after is None
    e = TransientError("rate limited", retry_after=2.5)
    assert str(e) == "rate limited" and e.retry_after == 2.5


def test_budget_deferred_says_which_budget_and_how_long_to_wait():
    e = BudgetDeferred(timedelta(minutes=5), "zakya")
    assert e.wait == timedelta(minutes=5) and e.budget == "zakya"
    assert "zakya" in str(e) and "0:05:00" in str(e)


def test_errors_with_extra_arguments_survive_pickling():
    # pickle rebuilds an exception from its `args`, so errors that cross a process or storage boundary
    # must keep their extra fields working through it.
    deferred = pickle.loads(pickle.dumps(BudgetDeferred(timedelta(minutes=5), "zakya")))
    assert deferred.wait == timedelta(minutes=5) and deferred.budget == "zakya"
    transient = pickle.loads(pickle.dumps(TransientError("busy", retry_after=2.5)))
    assert str(transient) == "busy" and transient.retry_after == 2.5

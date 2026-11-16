"""A durable engine keeps a step's error by pickling it: each error a connector raises comes back whole."""

import pickle
from datetime import timedelta

from feather_connect.core.errors import BudgetDeferred, PermanentError, TransientError
from feather_connect.runtime.http import HttpStatusError


def test_errors_survive_pickling_with_what_the_runtime_reads_from_them():
    status = pickle.loads(pickle.dumps(HttpStatusError("GET /x: HTTP 403", 403, "forbidden")))
    assert (str(status), status.status_code, status.excerpt, status.response) == (
        "GET /x: HTTP 403",
        403,
        "forbidden",
        None,
    )
    assert isinstance(status, PermanentError)
    transient = pickle.loads(pickle.dumps(TransientError("busy", retry_after=2.5)))
    assert (str(transient), transient.retry_after) == ("busy", 2.5)
    deferred = pickle.loads(pickle.dumps(BudgetDeferred(timedelta(minutes=10), "zoho-api", 3)))
    assert (deferred.wait, deferred.budget, deferred.taken) == (timedelta(minutes=10), "zoho-api", 3)

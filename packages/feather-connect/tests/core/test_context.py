import logging
from datetime import UTC, datetime

import pytest

from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError


def make(config=None) -> Context:
    return Context(
        config or {}, logging.getLogger("test"), datetime(2026, 10, 18, tzinfo=UTC), "Asia/Kolkata"
    )


def test_context_holds_what_it_is_given():
    ctx = make({"a": 1})
    assert ctx.config == {"a": 1} and ctx.tz == "Asia/Kolkata" and ctx.now.year == 2026
    assert ctx.budget is None and ctx.http is None


def test_checkpoint_returns_the_latest_value_saved():
    ctx = make()
    assert ctx.checkpoint() is None
    assert ctx.checkpoint("page-2") == "page-2"
    ctx.checkpoint("page-3")
    assert ctx.checkpoint() == "page-3"


def test_secret_returns_the_configured_text():
    assert make({"token": "hunter2"}).secret("token") == "hunter2"


@pytest.mark.parametrize("config", [{}, {"token": None}, {"token": 42}])
def test_secret_that_is_not_configured_as_text_is_a_permanent_error(config):
    with pytest.raises(PermanentError, match="token"):
        make(config).secret("token")


def test_repr_keeps_the_config_out_of_logs():
    assert "hunter2" not in repr(make({"token": "hunter2"}))

"""The real stack as a fixture: a server under uvicorn and a worker process, on a database of their own.

The tests marked `perf` here take minutes, so they run only when asked for, by marker or by file. `tests/e2e` as a folder
leaves them out, which is what the `postgres` job and the usual local command run. See the `chaos` job for the nightly run.
"""

from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from stack import Stack, stack_factory


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    named_a_file = any(str(arg).partition("::")[0].endswith(".py") for arg in config.args)
    if config.option.markexpr or named_a_file:
        return
    leave_out = pytest.mark.skip(reason="takes minutes: name the file, or select with -m perf")
    here = Path(__file__).parent
    for item in items:
        if item.get_closest_marker("perf") and item.path.is_relative_to(here):
            item.add_marker(leave_out)


@pytest.fixture
def stack(tmp_path: Path) -> Iterator[Callable[..., Stack]]:
    """Make a `Stack` on a new database of the test server, running `project`. `ddl` is for what a project reports to."""
    start, stop_all = stack_factory(tmp_path)
    yield start
    stop_all()

"""What the runtime's tasks need from outside them."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from feather_connect.clock import Clock
from feather_connect.core.context import Context
from feather_connect.core.windows import AvailabilityWindow
from feather_connect.engine.port import RetryPolicy
from feather_connect.storage.port import Store
from feather_connect.storage.spool import Spool


@dataclass(frozen=True)
class RuntimeDeps:
    """The services a task runs with.

    `code_sha` identifies the running code, and is recorded on what it produces. `make_context` returns the
    context to call a connector with, given the name of its connection. `retry` is the policy that `apply` tasks
    are submitted with, and sets how many tries each delivery gets. `package` is the prefix of an installed package's
    names, by which a bare dataset name in a reference is the package's own (M3.1 F1). `windows` maps a connection to
    its availability window, outside which nothing reads or writes it (M3.1 F3).
    """

    store: Store
    spool: Spool
    clock: Clock
    code_sha: str
    make_context: Callable[[str], Context]
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    package: str | None = None
    windows: Mapping[str, AvailabilityWindow] = field(default_factory=dict[str, AvailabilityWindow])

    def __post_init__(self) -> None:
        self.spool.bind(
            self.store.versions
        )  # so that a backend can guard a sealed version whatever the caller does

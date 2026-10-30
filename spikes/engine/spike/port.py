"""The Engine port from design §6.8, trimmed to what the conformance tests need."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    first_delay_s: float = 1.0
    backoff: float = 2.0
    max_delay_s: float = 60.0

    def delay(self, failed_attempt: int) -> float:
        return min(self.first_delay_s * self.backoff ** (failed_attempt - 1), self.max_delay_s)


@dataclass(frozen=True)
class TaskCall:
    task: str
    args: Mapping[str, Any]
    idempotency_key: str
    concurrency_key: str | None = None
    labels: Mapping[str, str] = field(default_factory=dict)
    priority: int = 1  # 0 low, 1 normal, 2 high
    retry: RetryPolicy = RetryPolicy()


class PermanentError(Exception):
    """A failure that must not be retried."""


class Ctx(Protocol):
    """What task bodies may use. Adapters implement it on top of their engine."""

    run: str
    task_id: str
    attempt: int
    worker: str

    def log(self, event: str, **info: Any) -> None: ...

    def chunk(self, name: str, fn: Callable[[], Any]) -> Any:
        """Run a side-effecting unit once; on replay or retry, a completed chunk is skipped."""

    def sleep(self, seconds: float) -> None:
        """Durable sleep: survives a worker restart."""

    def wait_signal(self, key: str, timeout_s: float) -> Any:
        """Durable wait for a signal sent with EngineClient.signal."""


class EngineClient(Protocol):
    """The control-plane side: submits work, never executes it."""

    name: str

    def submit(self, call: TaskCall, run: str) -> str: ...

    def signal(self, task_ref: str, key: str, payload: Any) -> None: ...

    def close(self) -> None: ...

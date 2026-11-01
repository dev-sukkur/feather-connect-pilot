"""Structured logs: one JSON object per line, carrying the ids of the work that wrote it.

`bind` puts ids (tenant, flow, run, task, ...) in a `contextvars` scope, so every record logged inside it, from any
logger, carries them. Secrets are replaced by `***` in what a record says and in the exception it carries, before
the line is written or a task's log is saved. `TaskLogHandler` copies the records of a task into the run journal.
"""

import json
import logging
import sys
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, TextIO

from feather_connect.storage.port import Store

__all__ = ["TaskLogHandler", "bind", "bound_ids", "configure_logging", "redact", "register_secrets"]

ID_NAMES = ("tenant", "flow", "run", "task", "dataset", "partition", "version", "route")
TASK_LOG_LIMIT = 64 * 1024  # bytes kept per task
_MASK = "***"
_MIN_SECRET = 4  # a shorter value would blot out ordinary text

_ids: ContextVar[Mapping[str, str]] = ContextVar("feather_log_ids", default=MappingProxyType({}))
_secrets: set[str] = set()


def register_secrets(*values: str | None) -> None:
    """Make every later record mask these values. Empty and very short ones are ignored."""
    _secrets.update(v for v in values if v and len(v) >= _MIN_SECRET)


def redact(text: str) -> str:
    """Replace each known secret in `text` with `***`, the longest first."""
    for secret in sorted(_secrets, key=len, reverse=True):
        text = text.replace(secret, _MASK)
    return text


@contextmanager
def bind(**ids: str | None) -> Generator[None]:
    """Add `ids` to the records logged inside the block, on top of what an enclosing `bind` set. A None is left out."""
    unknown = ids.keys() - set(ID_NAMES)
    if unknown:
        raise TypeError(f"cannot bind {', '.join(sorted(unknown))}: the ids are {', '.join(ID_NAMES)}")
    token = _ids.set({**_ids.get(), **{k: v for k, v in ids.items() if v is not None}})
    try:
        yield
    finally:
        _ids.reset(token)


def bound_ids() -> dict[str, str]:
    """The ids bound right now."""
    return dict(_ids.get())


def _text(record: logging.LogRecord, formatter: logging.Formatter) -> str:
    """The message of a record, with its traceback, secrets masked."""
    text = record.getMessage()
    if record.exc_info:
        text += "\n" + formatter.formatException(record.exc_info)
    return redact(text)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": _text(record, self),
            **_ids.get(),
        }
        return json.dumps(line, ensure_ascii=False)


_handler: logging.Handler | None = None


def configure_logging(
    level: str = "INFO", *, secrets: Iterable[str] = (), stream: TextIO | None = None
) -> None:
    """Log JSON lines to `stream` (stderr by default) from the root logger. Calling it again replaces the handler it
    installed before, so the lines never double. `secrets` are masked from now on, together with any registered."""
    global _handler
    register_secrets(*secrets)
    root = logging.getLogger()
    if _handler is not None:
        root.removeHandler(_handler)
    _handler = logging.StreamHandler(stream or sys.stderr)
    _handler.setFormatter(JsonFormatter())
    root.addHandler(_handler)
    root.setLevel(level.upper())


class TaskLogHandler(logging.Handler):
    """Copy the records logged while a task is bound into that task's log in the journal.

    A task keeps at most 64 KB of log. The line that crosses it is replaced by a notice and the rest of its lines
    are dropped; the stdout log still has them.
    """

    def __init__(self, store: Store, level: int = logging.INFO) -> None:
        super().__init__(level)
        self._runs = store.runs
        self._used: dict[str, int] = {}  # task id -> bytes saved, or the limit once the notice is written

    def _spent(self, task_id: str) -> int:
        if task_id not in self._used:  # another attempt or process may have written some already
            self._used[task_id] = sum(len(line.encode()) for line in self._runs.task_logs(task_id))
        return self._used[task_id]

    def emit(self, record: logging.LogRecord) -> None:
        task_id = _ids.get().get("task")
        if task_id is None:
            return
        try:
            message = _text(record, self.formatter or logging.Formatter())
            used = self._spent(task_id)
            if used >= TASK_LOG_LIMIT:
                return
            if used + len(message.encode()) > TASK_LOG_LIMIT:
                self._runs.log(task_id, "WARNING", f"log truncated: a task keeps {TASK_LOG_LIMIT // 1024} KB")
                self._used[task_id] = TASK_LOG_LIMIT
                return
            self._runs.log(task_id, record.levelname, message)
            self._used[task_id] = used + len(message.encode())
        except Exception:  # noqa: BLE001 - a log handler must never raise into the code that logged
            self.handleError(record)

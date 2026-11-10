"""The pipe between a worker supervisor and each of its package hosts (flow packages §7.1, §11): JSON lines, one message
a line, on the host's stdin (supervisor to host) and its stdout (host to supervisor). The host's own output goes to
stderr: it moves fd 1 aside before any package code is imported (`HostSide.attach`).

The host says `ready` once it takes work, or `failed` with why it can't start, and asks for `secret`s: the value of a
credential of one of its version's connections, which the supervisor resolves, since it holds the environment's vault
and the host holds no master key. The supervisor answers each request by its `id`, with a `value` or an `error`.

The pipe is also how a host knows its supervisor is there: end of file on stdin means the supervisor is gone, killed or
not, and the host stops (`on_lost`), so no host outlives its supervisor to run tasks beside the one that replaces it.
"""

import json
import logging
import os
import sys
import threading
from collections.abc import Callable, Mapping
from typing import IO, Any

__all__ = ["MAX_LINE", "HostSide", "LinkError", "SupervisorSide"]

log = logging.getLogger(__name__)

MAX_LINE = 256 * 1024  # bytes of one message; a longer line ends the link
REQUEST_TIMEOUT_S = 30.0


class LinkError(Exception):
    """The other side refused a request, or is gone: the message says which, never a secret's value."""


def _encode(message: Mapping[str, Any]) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode() + b"\n"


def _lines(stream: IO[bytes]) -> "Any":
    while True:
        line = stream.readline(MAX_LINE + 1)
        if not line:
            return
        if len(line) > MAX_LINE or not line.endswith(b"\n"):
            log.warning("a message on the host link is over %d bytes: the link ends", MAX_LINE)
            return
        try:
            message = json.loads(line)
        except ValueError:
            log.warning("a line on the host link is not JSON: it is ignored")
            continue
        if isinstance(message, dict):
            yield message


class HostSide:
    """The host's end of the link. `on_lost` is called once when the supervisor's end closes."""

    def __init__(self, reader: IO[bytes], writer: IO[bytes], on_lost: Callable[[], None]) -> None:
        self._writer = writer
        self._on_lost = on_lost
        self._lock = threading.Lock()
        self._next = 0
        self._waiting: dict[int, tuple[threading.Event, list[Mapping[str, Any]]]] = {}
        self._lost = False
        self._thread = threading.Thread(
            target=self._read, args=(reader,), name="supervisor-link", daemon=True
        )
        self._thread.start()

    @classmethod
    def attach(cls, on_lost: Callable[[], None]) -> "HostSide":
        """The link on this process's stdin and stdout, with fd 1 pointed at stderr from now on, so that nothing a
        package prints can pass for a message."""
        sys.stdout.flush()
        out = os.dup(1)
        os.dup2(2, 1)
        reader = os.fdopen(os.dup(0), "rb", buffering=0)
        devnull = os.open(os.devnull, os.O_RDONLY)
        os.dup2(devnull, 0)
        os.close(devnull)
        return cls(reader, os.fdopen(out, "wb", buffering=0), on_lost)

    def _read(self, reader: IO[bytes]) -> None:
        try:
            for message in _lines(reader):
                found = self._waiting.get(int(message.get("id", -1)))
                if found is not None:
                    found[1].append(message)
                    found[0].set()
        except (OSError, ValueError):
            pass
        with self._lock:
            self._lost = True
            waiting = list(self._waiting.values())
        for event, _ in waiting:
            event.set()
        self._on_lost()

    def send(self, op: str, **fields: Any) -> None:
        """Say something that needs no answer. A link that is gone takes it silently."""
        try:
            with self._lock:
                self._writer.write(_encode({"op": op, **fields}))
        except (OSError, ValueError):
            pass

    def request(self, op: str, timeout: float = REQUEST_TIMEOUT_S, **fields: Any) -> Mapping[str, Any]:
        """Ask the supervisor, and return its answer. Raises `LinkError` for a refusal, no answer or no link."""
        event, answers = threading.Event(), list[Mapping[str, Any]]()
        with self._lock:
            if self._lost:
                raise LinkError("the supervisor is gone")
            self._next += 1
            number = self._next
            self._waiting[number] = (event, answers)
            try:
                self._writer.write(_encode({"op": op, "id": number, **fields}))
            except (OSError, ValueError):
                del self._waiting[number]
                raise LinkError("the supervisor is gone") from None
        try:
            if not event.wait(timeout) or not answers:
                raise LinkError(f"the supervisor didn't answer {op} in {timeout:g} s")
        finally:
            with self._lock:
                self._waiting.pop(number, None)
        answer = answers[0]
        if "error" in answer:
            raise LinkError(str(answer["error"]))
        return answer


Handler = Callable[[Mapping[str, Any]], Mapping[str, Any]]


class SupervisorSide:
    """The supervisor's end of one host's link: `handlers` answer the host's requests by op (a handler raises
    `LinkError` to refuse), and `notes` takes what the host says without asking. Closing it is what tells the host to
    stop at once."""

    def __init__(
        self,
        writer: IO[bytes],
        reader: IO[bytes],
        handlers: Mapping[str, Handler],
        notes: Callable[[Mapping[str, Any]], None],
        name: str,
    ) -> None:
        self._writer = writer
        self._handlers = dict(handlers)
        self._notes = notes
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._read, args=(reader,), name=f"link-{name}", daemon=True)
        self._thread.start()

    def _read(self, reader: IO[bytes]) -> None:
        try:
            for message in _lines(reader):
                op = str(message.get("op"))
                if "id" not in message:
                    try:
                        self._notes(message)
                    except Exception:
                        log.exception("a note from a host failed")
                    continue
                handler = self._handlers.get(op)
                try:
                    if handler is None:
                        raise LinkError(f"no request is called {op!r}")
                    answer: Mapping[str, Any] = handler(message)
                except LinkError as error:
                    answer = {"error": str(error)}
                except Exception as error:
                    log.exception("a host's %s request failed", op)
                    answer = {"error": f"{op} failed: {type(error).__name__}"}
                self._write({**answer, "id": message["id"]})
        except (OSError, ValueError):
            pass

    def _write(self, message: Mapping[str, Any]) -> None:
        try:
            with self._lock:
                self._writer.write(_encode(message))
                self._writer.flush()
        except (OSError, ValueError):
            pass

    def close(self) -> None:
        with self._lock:
            try:
                self._writer.close()
            except OSError:
                pass

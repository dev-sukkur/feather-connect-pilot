"""A TCP proxy that adds a delay to every chunk of bytes in each direction: a slow network between Feather and
Postgres, for the chaos tests (M3 plan 1 P2). It binds 127.0.0.1 on a free port and forwards to one address."""

import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit


class LatencyProxy:
    def __init__(self, target: tuple[str, int], delay_s: float) -> None:
        self.target = target
        self.delay_s = delay_s
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port: int = self._listener.getsockname()[1]
        self._stopped = threading.Event()
        self._sockets: list[socket.socket] = []
        self.chunks = 0  # how many chunks were delayed, so a test can tell the proxy was in the way
        threading.Thread(target=self._accept, daemon=True, name="latency-proxy").start()

    def _accept(self) -> None:
        while not self._stopped.is_set():
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            try:
                upstream = socket.create_connection(self.target, timeout=10)
            except OSError:
                client.close()
                continue
            upstream.settimeout(None)
            self._sockets += [client, upstream]
            for a, b in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    def _pipe(self, source: socket.socket, sink: socket.socket) -> None:
        try:
            while data := source.recv(65536):
                time.sleep(self.delay_s)
                self.chunks += 1
                sink.sendall(data)
        except OSError:
            pass
        finally:
            for s in (source, sink):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self) -> None:
        self._stopped.set()
        self._listener.close()
        for s in self._sockets:
            try:
                s.close()
            except OSError:
                pass


@contextmanager
def delayed(url: str, delay_s: float) -> Iterator[tuple[str, LatencyProxy]]:
    """The URL through a proxy that delays each chunk by `delay_s`, and the proxy."""
    parts = urlsplit(url)
    proxy = LatencyProxy((parts.hostname or "127.0.0.1", parts.port or 5432), delay_s)
    netloc = (
        f"{parts.username}:{parts.password}@127.0.0.1:{proxy.port}"
        if parts.username
        else f"127.0.0.1:{proxy.port}"
    )
    try:
        yield urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment)), proxy
    finally:
        proxy.close()

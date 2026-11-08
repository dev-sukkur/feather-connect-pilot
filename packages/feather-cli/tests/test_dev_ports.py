"""How `feather dev` tells whether a port is taken."""

import os
import socket

from feather_cli.commands.dev import _in_use


def test_a_port_with_only_closed_connections_left_is_free() -> None:
    """A server that closed its side leaves TIME_WAIT on its port for a minute; its next run can bind there all the same."""
    with socket.socket() as server:
        if os.name != "nt":
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # as uvicorn, and so the helpers, do
        server.bind(("127.0.0.1", 0))
        server.listen()
        port = server.getsockname()[1]
        client = socket.create_connection(("127.0.0.1", port))
        accepted, _ = server.accept()
        accepted.close()  # the server closes first, so its end of the connection waits
        client.recv(1)
        client.close()
    assert not _in_use(port)


def test_a_listening_port_is_in_use() -> None:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        assert _in_use(server.getsockname()[1])

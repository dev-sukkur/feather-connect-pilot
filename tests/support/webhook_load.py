"""A load generator for webhook ingress, to run as a process of its own so that its work doesn't slow the test's.

    python webhook_load.py URL SECRET OUT.json RATE:SECONDS [RATE:SECONDS ...]

It is open loop: each event has a time it is due, whatever became of the ones before, so a slow server shows as late
writes and not as a lower rate that nobody asked for. Its own speed is measured rather than assumed: for each phase, `OUT`
says how many requests were written within the phase, for the caller to compare with what it asked for. A line on stdin
ends the last phase early. It speaks HTTP over plain sockets, since a client library's overhead per request is a large part
of what 500 a second can afford.
"""

import asyncio
import json
import os
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from feather_connect.connectors.simulation import SIGNATURE_HEADER, TIMESTAMP_HEADER
from feather_connect.core.webhooks import sign

CONNECTIONS = 160  # at 10x the server answers in ~0.2 s on a CI runner; 64 would cap the rate near 320/s
# Where to send, and whether the signature is bound to the timestamp (`http.Webhook`'s, the default) or of the body
# alone (a package's declared HMAC webhook, which holds the timestamp only to its tolerance).
PATH = os.environ.get("FEATHER_LOAD_PATH", "/ingress/default/perf_hooks")
BODY_ONLY = os.environ.get("FEATHER_LOAD_SIGN") == "body"
ERROR_BODY = 300


@dataclass
class Phase:
    rate: float
    seconds: float
    started: float = 0.0
    ended: float = 0.0
    wall_offset: float = 0.0  # what turns `started` and `ended` into the time of day
    written: int = 0
    responses: list[float] = field(default_factory=list[float])


@dataclass
class Totals:
    statuses: Counter[int] = field(default_factory=Counter[int])
    accepted: list[int] = field(default_factory=list[int])
    refused: list[tuple[int, int, str]] = field(default_factory=list[tuple[int, int, str]])


def _request(host: str, secret: str, number: int) -> bytes:
    sent_at = time.time()
    body = json.dumps({"id": number, "event_id": f"perf-{number}", "sent_at": sent_at}).encode()
    stamp = str(int(sent_at))
    head = (
        f"POST {PATH} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n{SIGNATURE_HEADER}: {sign(secret, body, None if BODY_ONLY else stamp)}\r\n"
        f"{TIMESTAMP_HEADER}: {stamp}\r\n\r\n"
    )
    return head.encode() + body


async def _read_response(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    head = await reader.readuntil(b"\r\n\r\n")
    status = int(head.split(b" ", 2)[1])
    length = 0
    for line in head.split(b"\r\n")[1:]:
        if line.lower().startswith(b"content-length:"):
            length = int(line.split(b":", 1)[1])
    return status, await reader.readexactly(length)


async def _sender(
    target: tuple[str, int],
    secret: str,
    queue: asyncio.Queue[tuple[int, Phase] | None],
    totals: Totals,
) -> None:
    """Send what `queue` gives over one connection, one at a time. A failed connection counts as a refusal."""
    host, port = target
    reader, writer = await asyncio.open_connection(host, port)
    while (item := await queue.get()) is not None:
        number, phase = item
        data = _request(f"{host}:{port}", secret, number)
        started = time.perf_counter()
        if phase.started <= started <= phase.ended:
            phase.written += 1
        try:
            writer.write(data)
            await writer.drain()
            status, body = await _read_response(reader)
        except (OSError, EOFError, asyncio.IncompleteReadError) as error:
            status, body = -1, type(error).__name__.encode()
            writer.close()
            reader, writer = await asyncio.open_connection(host, port)
        phase.responses.append(time.perf_counter() - started)
        totals.statuses[status] += 1
        if status == 202:
            totals.accepted.append(number)
        else:
            totals.refused.append((number, status, body[:ERROR_BODY].decode(errors="replace")))
    writer.close()


async def _pace(
    phases: list[Phase], queue: asyncio.Queue[tuple[int, Phase] | None], stop: threading.Event
) -> None:
    """Queue each event when it is due. Only the last phase can be ended early."""
    number = 0
    at = time.perf_counter()
    for index, phase in enumerate(phases):
        phase.started, phase.ended = at, at + phase.seconds
        phase.wall_offset = time.time() - time.perf_counter()
        for n in range(round(phase.rate * phase.seconds)):
            if stop.is_set() and index == len(phases) - 1:
                phase.ended = time.perf_counter()
                return
            wait = at + n / phase.rate - time.perf_counter()
            if wait > 0:
                await asyncio.sleep(wait)
            number += 1
            queue.put_nowait((number, phase))
        at = phase.ended


async def _run(url: str, secret: str, phases: list[Phase], stop: threading.Event) -> Totals:
    parts = urlsplit(url)
    target = (str(parts.hostname), int(parts.port or 80))
    queue: asyncio.Queue[tuple[int, Phase] | None] = asyncio.Queue()
    totals = Totals()
    senders = [asyncio.create_task(_sender(target, secret, queue, totals)) for _ in range(CONNECTIONS)]
    await _pace(phases, queue, stop)
    for _ in senders:
        queue.put_nowait(None)
    await asyncio.gather(*senders)
    return totals


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def _report(phases: list[Phase], totals: Totals) -> dict[str, Any]:
    return {
        "phases": [
            {
                "rate": phase.rate,
                "seconds": phase.ended - phase.started,
                "wall_started": phase.started + phase.wall_offset,
                "wall_ended": phase.ended + phase.wall_offset,
                "written": phase.written,
                "achieved": phase.written / max(phase.ended - phase.started, 1e-9),
                "response_p50": _percentile(phase.responses, 0.50),
                "response_p95": _percentile(phase.responses, 0.95),
            }
            for phase in phases
        ],
        "statuses": {str(status): count for status, count in totals.statuses.items()},
        "accepted": totals.accepted,
        "refused": totals.refused[:50],
    }


def main(argv: list[str]) -> int:
    url, secret, out, *specs = argv
    phases = [Phase(float(rate), float(seconds)) for rate, seconds in (spec.split(":") for spec in specs)]
    stop = threading.Event()

    def watch_stdin() -> None:
        sys.stdin.readline()
        stop.set()

    threading.Thread(target=watch_stdin, daemon=True).start()
    totals = asyncio.run(_run(url, secret, phases, stop))
    with open(out, "w", encoding="utf-8") as file:
        json.dump(_report(phases, totals), file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

"""Faults the demo's simulators answer with on purpose, set by hand on each simulator's page, to see Feather retry,
wait, fail a delivery or quarantine a record.

A fault is queued with a kind, how many calls it takes (or until it is cleared), and which calls it is for: `any`,
`read` (a GET), `write` (a POST, PUT or PATCH) or `token` (a sign-in). The next call it fits takes it. `KINDS` says,
for each kind, what the call gets and what Feather should do with it, so the page can say so too. Each fault served is
logged, newest first, for the page to show.

    faults = Faults(KINDS)
    faults.add("503", times=2, scope="read")   # the next two reads answer 503
    faults.add("403", times=None)              # every call answers 403 until cleared
    faults.clear()
"""

import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any

SCOPES = ("any", "read", "write", "token")


@dataclass(frozen=True)
class Kind:
    """A kind of fault: its HTTP status (None for one with no answer), what it is, and what Feather should do."""

    status: int | None
    label: str
    expect: str
    retry_after: float | None = None  # seconds, for a 429 or a 503
    seconds: float | None = None  # how long a slow or hanging answer takes


KINDS: dict[str, Kind] = {
    "429": Kind(
        429,
        "Too many requests, Retry-After a few seconds",
        "The client waits the Retry-After and calls again: the run goes on, a little later.",
        retry_after=2,
    ),
    "429-long": Kind(
        429,
        "Too many requests, Retry-After 10 minutes",
        "Longer than a minute, so the task gives the wait back to the runtime (BudgetDeferred) and sleeps it off "
        "durably: the run shows waiting, then carries on.",
        retry_after=600,
    ),
    "429-day": Kind(
        429,
        "The daily cap is used up (Zoho code 45)",
        "The task waits for the cap to reset (Retry-After, or midnight in the organisation's time zone with none), "
        "durably; nothing fails.",
        retry_after=900,
    ),
    "401": Kind(
        401,
        "Unauthorized: the token expired or was revoked",
        "Once: the client gets a new token and calls again. Every time: the chunk fails (credentials), the run or "
        "delivery shows failed; fix the connection's secret and Retry.",
    ),
    "403": Kind(
        403,
        "Forbidden: the account lacks the right",
        "Not retried: the chunk fails at once and the delivery goes to Failures. Grant the right, then Retry.",
    ),
    "400": Kind(
        400,
        "Bad request: this record is wrong",
        "On a write, the record is rejected and kept in the quarantine with the answer; the rest go on. On a read, "
        "the run fails.",
    ),
    "404": Kind(
        404,
        "Not found: the API path isn't there",
        "The chunk fails (a wrong URL or a missing iFlow): fix the connection, then Retry.",
    ),
    "409": Kind(
        409,
        "Conflict",
        "An HTTP write counts a 409 as applied already when the destination says so for the same content; "
        "otherwise the record is rejected to the quarantine.",
    ),
    "422": Kind(
        422,
        "Unprocessable: the destination's own check refused the record",
        "The record is rejected and kept in the quarantine, with the destination's message; the rest go on.",
    ),
    "500": Kind(
        500,
        "Internal server error",
        "Tried again with backoff; if it keeps failing, the chunk fails and the runtime retries the delivery.",
    ),
    "502": Kind(
        502,
        "Bad gateway",
        "As a 500: tried again with backoff, then the delivery is retried.",
    ),
    "503": Kind(
        503,
        "Service unavailable",
        "As a 500: tried again with backoff (after its Retry-After, if any), then the delivery is retried.",
        retry_after=1,
    ),
    "504": Kind(
        504,
        "Gateway timeout",
        "As a 500. A write that created something is not sent twice by the client: the chunk is retried, and "
        "the idempotency key or a lookup finds what the first one made.",
    ),
    "timeout": Kind(
        None,
        "No answer: the call hangs past the client's timeout (30 s)",
        "A timeout, tried again as a 5xx is; a write is not resent by the client but retried with its chunk.",
        seconds=35,
    ),
    "slow": Kind(
        None,
        "A slow answer: right, after a few seconds",
        "Nothing fails: the run takes longer. Use it to watch a run in progress.",
        seconds=5,
    ),
    "reset": Kind(
        None,
        "The connection drops with no answer",
        "A connection error, tried again as a 5xx is.",
    ),
    "garbage": Kind(
        200,
        "200 with an HTML error page instead of JSON",
        'The answer can\'t be read: the read fails ("the answer is not JSON") and the run shows failed.',
    ),
    "lost": Kind(
        504,
        "Carried out, then the answer is lost (504)",
        "The write happened though the answer failed. The retry must not do it twice: the idempotency key, or the "
        "sink's lookup, finds it.",
    ),
}


def scope_of(
    method: str, path: str, token_paths: tuple[str, ...] = ("/oauth/v2/token", "/oauth/token")
) -> str:
    """The scope of a call: `token` for a sign-in, `read` for a GET or HEAD, else `write`."""
    if path in token_paths:
        return "token"
    return "read" if method in ("GET", "HEAD") else "write"


@dataclass
class Fault:
    fault_id: int
    kind: str
    scope: str
    left: int | None  # None: until cleared
    retry_after: float | None
    seconds: float | None
    served: int = 0


@dataclass
class Faults:
    """The faults queued on one simulator, and the newest ones served. Thread safe."""

    kinds: dict[str, Kind] = field(default_factory=lambda: dict(KINDS))
    _queued: list[Fault] = field(default_factory=list[Fault])
    _served: deque[dict[str, Any]] = field(default_factory=lambda: deque[dict[str, Any]](maxlen=30))
    _next: int = 1
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(
        self,
        kind: str,
        *,
        times: int | None = 1,
        scope: str = "any",
        retry_after: float | None = None,
        seconds: float | None = None,
    ) -> Fault:
        """Queue a fault for the next `times` calls of `scope` (None: until cleared). Raises `ValueError`."""
        found = self.kinds.get(kind)
        if found is None:
            raise ValueError(f"no fault {kind!r}: there are {', '.join(self.kinds)}")
        if scope not in SCOPES:
            raise ValueError(f"scope is one of {', '.join(SCOPES)}")
        if times is not None and not 1 <= times <= 1000:
            raise ValueError("times is 1 to 1000, or none for until cleared")
        with self._lock:
            fault = Fault(
                self._next,
                kind,
                scope,
                times,
                found.retry_after if retry_after is None else retry_after,
                found.seconds if seconds is None else seconds,
            )
            self._next += 1
            self._queued.append(fault)
            return fault

    def clear(self, fault_id: int | None = None) -> int:
        """Clear one fault, or all of them; return how many went."""
        with self._lock:
            before = len(self._queued)
            self._queued = [f for f in self._queued if fault_id is not None and f.fault_id != fault_id]
            return before - len(self._queued)

    def take(self, method: str, path: str, scope: str | None = None) -> Fault | None:
        """The fault the call gets, if any: the first queued one for its scope, counted as served."""
        scope = scope or scope_of(method, path)
        with self._lock:
            for fault in self._queued:
                if fault.scope not in ("any", scope):
                    continue
                fault.served += 1
                if fault.left is not None:
                    fault.left -= 1
                    if fault.left <= 0:
                        self._queued.remove(fault)
                self._served.appendleft(
                    {"at": time.strftime("%H:%M:%S"), "kind": fault.kind, "call": f"{method} {path}"}
                )
                return fault
        return None

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "kinds": {k: asdict(v) for k, v in self.kinds.items()},
                "scopes": list(SCOPES),
                "queued": [asdict(f) for f in self._queued],
                "served": list(self._served),
            }

    def control(self, method: str, action: str, body: dict[str, Any], sim: Any) -> dict[str, Any] | None:
        """The page's calls under `/sim/`: GET `faults`, POST `faults` (queue one), POST `faults/clear` and POST
        `fault-rate` (the share of calls that answer 429 or 503 by chance, `sim.fault_rate`). None for another."""
        if action == "faults" and method == "GET":
            return {**self.snapshot(), "fault_rate": sim.fault_rate}
        if method != "POST" or action not in ("faults", "faults/clear", "fault-rate"):
            return None
        if action == "faults":
            times = body.get("times", 1)
            fault = self.add(
                str(body.get("kind")),
                times=None if times in (None, "", "until cleared") else int(times),
                scope=str(body.get("scope") or "any"),
                retry_after=None if body.get("retry_after") in (None, "") else float(body["retry_after"]),
                seconds=None if body.get("seconds") in (None, "") else float(body["seconds"]),
            )
            return asdict(fault)
        if action == "faults/clear":
            given = body.get("fault_id")
            return {"cleared": self.clear(None if given in (None, "") else int(given))}
        rate = float(body.get("rate", 0))
        if not 0 <= rate <= 1:
            raise ValueError("the fault rate is 0 to 1")
        sim.fault_rate = rate
        return {"fault_rate": rate}


def body_of(fault: Fault, kinds: dict[str, Kind], flavour: str) -> tuple[int, dict[str, str], Any]:
    """The answer of a fault that has a status, as Zoho (`zoho`) or SAP CPI and the receiver (`plain`) word one."""
    kind = kinds[fault.kind]
    status = kind.status or 503
    headers: dict[str, str] = {}
    if fault.retry_after is not None and status in (429, 503):
        headers["Retry-After"] = str(int(fault.retry_after))
    if flavour == "zoho":
        code = {
            "429-day": 45,
            "429": 44,
            "429-long": 44,
            "401": 57,
            "403": 57,
            "400": 4,
            "404": 5,
            "409": 1001,
        }
        body: Any = {"code": code.get(fault.kind, status), "message": f"{kind.label} (on purpose)"}
        if fault.kind == "429-day":
            body["message"] = "You have reached the maximum number of API calls for the day (on purpose)"
    else:
        body = {"error": {"code": str(status), "message": f"{kind.label} (on purpose)"}}
    return status, headers, body


GARBAGE = b"<html><body><h1>502 Bad Gateway</h1><p>The proxy had a bad day (on purpose).</p></body></html>"

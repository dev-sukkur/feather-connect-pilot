"""The JSON simulation connector: repeatable source data and injected faults, so dev and qa need no real system.

A source serves rows from a JSON or JSONL fixture or from a generator spec. A fixed seed makes its rows, and
the faults it injects, the same on every run. The sink is the in-memory one and can also write what it gets to
a JSONL file.
"""

import json
import random
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from feather_connect.connectors import memory
from feather_connect.core import connector
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyResult,
    CheckResult,
    ConnectorSpec,
    ReadRequest,
    StreamSchema,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError

# How a generated field makes its value: from the source's random generator and the row's number, from 0.
FieldSpec = Callable[[random.Random, int], Any]

_FIRST_NAMES = ("Asha", "Bruno", "Chen", "Dara", "Elif", "Farid", "Gita", "Hugo", "Ines", "Jamal")
_LAST_NAMES = ("Abbott", "Banerjee", "Costa", "Diaz", "Evans", "Fischer", "Gupta", "Haas", "Ito", "Joshi")
# What `schema_change` adds to the rows. It also drops their last original field.
ADDED_FIELD = "sim_extra"
_RETRY_AFTER_429 = 1.0


@dataclass(frozen=True)
class Faults:
    """The chance, from 0 to 1, that each fault happens.

    `duplicate`, `late`, `hard_delete`, `key_rewrite` and `schema_change` are chances per row, except
    `schema_change`, which is per read. `http_429`, `http_5xx` and `timeout` are chances per read. The first
    read serves the rows as they are, apart from duplicates and the transient errors, so the faults that change
    the data take effect from the second read on.
    """

    duplicate: float = 0.0
    late: float = 0.0
    hard_delete: float = 0.0
    key_rewrite: float = 0.0
    schema_change: float = 0.0
    http_429: float = 0.0
    http_5xx: float = 0.0
    timeout: float = 0.0


def seq() -> FieldSpec:
    """A field that counts up from 1."""
    return lambda rng, index: index + 1


def choice(values: list[Any]) -> FieldSpec:
    """A field that takes one of `values`."""
    options = tuple(values)
    return lambda rng, index: rng.choice(options)


def date_in(start: str, end: str) -> FieldSpec:
    """A field that holds an ISO date from `start` to `end`, both included."""
    first = date.fromisoformat(start)
    days = (date.fromisoformat(end) - first).days
    return lambda rng, index: (first + timedelta(days=rng.randint(0, days))).isoformat()


def amount(low: float, high: float) -> FieldSpec:
    """A field that holds a number from `low` to `high`, with two decimals."""
    return lambda rng, index: round(rng.uniform(low, high), 2)


def name() -> FieldSpec:
    """A field that holds a person's name."""
    return lambda rng, index: f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"


def _load_fixture(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    rows = json.loads(text)
    if not isinstance(rows, list):
        raise PermanentError(f"the fixture {path} must hold a list of rows")
    return [dict(row) for row in rows]  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]


def _rewrite(value: Any) -> Any:
    return value + 1_000_000 if isinstance(value, int) else f"{value}-rewritten"


class Source(connector.Source):
    """A source of fixture or generated rows that can misbehave in the ways `faults` lists.

    The rows come from `fixture` (a JSON or JSONL file) or else from `generator` (field names mapped to specs
    such as `seq()`), `rows_per_partition` of them, whatever the partition a read asks for. A read draws its
    faults from a generator seeded with `seed` and the number of the read, so the same source gives the same
    reads every time. The first field of a row is its key, for `key_rewrite`.
    """

    spec = ConnectorSpec(
        name="simulation.source",
        strategies=frozenset({"by_stamp", "snapshot_diff", "replace", "push"}),
    )

    def __init__(
        self,
        connection: str = "simulation",
        fixture: str | Path | None = None,
        generator: Mapping[str, FieldSpec] | None = None,
        rows_per_partition: int = 100,
        seed: int = 42,
        faults: Faults = Faults(),  # noqa: B008  (frozen, so it is safe to share)
    ) -> None:
        if (fixture is None) == (generator is None):
            raise ValueError("give a simulation source either a fixture or a generator")
        self.connection = connection
        self.seed = seed
        self.faults = faults
        self.reads = 0
        self._rows = self._make_rows(fixture, generator, rows_per_partition)
        self._late: list[dict[str, Any]] = []  # held back by `late`, served by the next read
        self._schema_changed = False

    def _make_rows(
        self, fixture: str | Path | None, generator: Mapping[str, FieldSpec] | None, count: int
    ) -> list[dict[str, Any]]:
        if fixture is not None:
            return _load_fixture(Path(fixture))
        assert generator is not None
        rng = random.Random(self.seed)
        return [{field: make(rng, index) for field, make in generator.items()} for index in range(count)]

    def check(self, ctx: Context) -> CheckResult:
        return CheckResult(ok=True)

    def discover(self, ctx: Context) -> list[StreamSchema]:
        """Offer one stream, `rows`, with the fields of the rows and the type of each first value."""
        fields: dict[str, str] = {}
        for row in self._rows:
            for field, value in row.items():
                if value is not None:
                    fields.setdefault(field, type(value).__name__)
        return [StreamSchema("rows", fields)]

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[list[dict[str, Any]]]:
        """Yield the matching rows as a single batch, with this read's faults, or nothing if none match."""
        self.reads += 1
        rng = random.Random(f"{self.seed}:{self.reads}")
        self._raise_transient(rng)
        if self.reads > 1:
            self._change_data(rng)
        held = [row for row in self._late if req.contains(row)]
        self._late = [row for row in self._late if not req.contains(row)]
        rows = [dict(row) for row in held]
        held_ids = {id(row) for row in self._late} | {id(row) for row in held}
        served = [
            row
            for row in self._rows
            if id(row) not in held_ids and req.contains(row) and memory.changed_since(row, req)
        ]
        for row in served:
            if rng.random() < self.faults.late:
                self._late.append(row)
                continue
            rows.append(dict(row))
            if rng.random() < self.faults.duplicate:
                rows.append(dict(row))
        if rows:
            yield rows

    def _raise_transient(self, rng: random.Random) -> None:
        if rng.random() < self.faults.http_429:
            raise TransientError("simulated HTTP 429", retry_after=_RETRY_AFTER_429)
        if rng.random() < self.faults.http_5xx:
            raise TransientError("simulated HTTP 503")
        if rng.random() < self.faults.timeout:
            raise TransientError("simulated timeout")

    def _change_data(self, rng: random.Random) -> None:
        """Delete, rewrite and reshape the held rows for good, as the faults of this read decide."""
        kept: list[dict[str, Any]] = []
        for row in self._rows:
            if rng.random() < self.faults.hard_delete:
                continue
            if row and rng.random() < self.faults.key_rewrite:
                key_field = next(iter(row))
                row[key_field] = _rewrite(row[key_field])
            kept.append(row)
        self._rows = kept
        if not self._schema_changed and rng.random() < self.faults.schema_change:
            self._schema_changed = True
            for row in self._rows:
                if row:
                    del row[next(reversed(row))]
                row[ADDED_FIELD] = None


class Sink(memory.Sink):
    """The in-memory sink, which also writes each record it applies to `capture_to` as a line of JSON.

    A line holds the record's `op`, `key` and `payload`. A batch that is applied again is not written again.
    """

    spec = ConnectorSpec(name="simulation.sink", modes=memory.Sink.spec.modes)

    def __init__(self, connection: str = "simulation", capture_to: str | Path | None = None) -> None:
        super().__init__(connection)
        self.capture_to = None if capture_to is None else Path(capture_to)

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        is_repeat = batch.idempotency_key in self._results
        result = super().apply(ctx, batch)
        if self.capture_to is not None and not is_repeat:
            lines = [
                json.dumps({"op": r.op, "key": r.key, "payload": r.payload}, default=str)
                for r in batch.records
            ]
            self.capture_to.parent.mkdir(parents=True, exist_ok=True)
            with self.capture_to.open("a", encoding="utf-8", newline="\n") as out:
                out.writelines(line + "\n" for line in lines)
        return result

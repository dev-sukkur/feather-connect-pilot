"""Connector types: the spec, the requests and batches that pass to sources and sinks, and their protocols."""

from collections.abc import Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Protocol

import pyarrow as pa
from pydantic import BaseModel

from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.partitions import PartitionScheme
from feather_connect.core.records import Op

RouteMode = Literal["upsert", "append", "replace_partition", "side_effect"]
# A key as a sink gets it (see `decode_key`): the text for one key field, a tuple of the parts for several.
Key = str | tuple[Any, ...]
# What a source yields: an Arrow record batch, or rows as dicts.
Batch = pa.RecordBatch | list[dict[str, Any]]


@dataclass(frozen=True)
class ConnectorSpec:
    """What a connector declares about itself, so that a flow can be checked against it before it runs.

    `strategies` are the change strategies a source can serve and `modes` the route modes a sink can apply.
    `deletes` says how the connector reports deleted records, `"none"` when it doesn't. `config` is the
    Pydantic model of its connection settings and `cost` the budget units each kind of call uses.
    """

    name: str
    config: type[BaseModel] | None = None
    strategies: AbstractSet[str] = frozenset()
    modes: AbstractSet[RouteMode] = frozenset({"upsert"})
    deletes: str = "none"
    cost: Mapping[str, int] = field(default_factory=dict[str, int])


@dataclass(frozen=True)
class CheckResult:
    """The outcome of checking a connection: whether it works and, if not, why."""

    ok: bool
    message: str = ""


@dataclass(frozen=True)
class StreamSchema:
    """A stream a source offers, with its fields as names mapped to type names."""

    name: str
    fields: Mapping[str, str]


@dataclass(frozen=True)
class ReadRequest:
    """What a source is asked to read: one partition of a dataset, or all of it when `partition` is `"*"`.

    `since` and `stamp_field` ask for the records whose change stamp is `since` or later. `resume` is a
    checkpoint that an earlier read of the same partition saved before it was interrupted.
    """

    partition: str
    scheme: PartitionScheme
    since: Any = None
    stamp_field: str | None = None
    resume: str | None = None

    def contains(self, payload: Mapping[str, Any]) -> bool:
        """Return whether a record is in the requested partition. Raises `PartitionError` if it has none."""
        return self.partition == "*" or self.scheme.key_of(payload) == self.partition


@dataclass(frozen=True)
class ApplyRecord:
    """A record for a sink to apply. A delete has no payload.

    `moved_from` names the partition that the key was in before, on the upsert of a record that moved.
    """

    key: Key
    op: Op
    payload: Mapping[str, Any] | None
    moved_from: str | None = None


@dataclass(frozen=True)
class ApplyBatch:
    """One chunk of a version for a route's sink.

    `idempotency_key` identifies the chunk, so that a sink can recognise a repeat. `is_first` and `is_last`
    mark the ends of the version's chunks, which matters to `replace_partition`.
    """

    route: str
    version_id: str
    chunk_no: int
    partition: str
    mode: RouteMode
    records: Sequence[ApplyRecord]
    idempotency_key: str
    is_first: bool
    is_last: bool


@dataclass(frozen=True)
class Rejection:
    """A record that a sink refused. `error_class` names the kind of error, `message` what was wrong."""

    key: Key
    error_class: str
    message: str


@dataclass(frozen=True)
class ApplyResult:
    """What a sink did with a batch: how many records it applied and which it rejected.

    A sink that accepts a batch and reports the outcome later returns `pending_confirmation` instead.
    """

    applied: int
    rejected: Sequence[Rejection] = ()
    pending_confirmation: str | None = None


@dataclass(frozen=True)
class Deleted:
    """What a push source returns for an explicitly deleted record. The payload has at least its key."""

    payload: Mapping[str, Any]


class Source(Protocol):
    """A pull source: the runtime asks it to read a partition."""

    spec: ClassVar[ConnectorSpec]
    connection: str

    def check(self, ctx: Context) -> CheckResult:
        """Test that the connection works."""
        ...

    def discover(self, ctx: Context) -> list[StreamSchema]:
        """List the streams the connection offers."""
        ...

    def read(self, ctx: Context, req: ReadRequest) -> Iterator[Batch]:
        """Yield the records that `req` asks for, in batches."""
        ...


class PushSource(Protocol):
    """A source that receives webhooks or change events instead of being read."""

    spec: ClassVar[ConnectorSpec]
    connection: str

    # `request` will be an `InboundRequest` once the ingress task adds that type.
    def verify(self, ctx: Context, request: Any) -> None:
        """Raise if the request is not genuine, for example because its signature is bad."""
        ...

    def receive(self, ctx: Context, request: Any) -> list[Mapping[str, Any] | Deleted]:
        """Return the records in the request. The runtime derives their keys and hashes."""
        ...


class Sink(Protocol):
    """A destination: the runtime applies each version to it, one batch at a time."""

    spec: ClassVar[ConnectorSpec]
    connection: str

    def check(self, ctx: Context) -> CheckResult:
        """Test that the connection works."""
        ...

    def apply(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        """Apply the batch's records. This must be idempotent per key: a repeat changes nothing more."""
        ...

    def compensate(self, ctx: Context, batch: ApplyBatch) -> ApplyResult:
        """Reverse an applied batch. Only the sinks of side-effect routes implement this."""
        raise PermanentError(f"{self.spec.name} can't compensate a batch")

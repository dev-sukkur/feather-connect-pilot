"""Connector types: the spec, the requests and batches that pass to sources and sinks, and their protocols."""

from collections.abc import Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Literal, Protocol

import pyarrow as pa
from pydantic import BaseModel, SecretStr

from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.partitions import PartitionScheme
from feather_connect.core.records import Op

RouteMode = Literal["upsert", "append", "replace_partition", "side_effect"]
# A key as a sink gets it (see `decode_key`): the text for one key field, a tuple of the parts for several.
Key = str | tuple[Any, ...]
# What a source yields: an Arrow record batch, or rows as dicts.
Batch = pa.RecordBatch | list[dict[str, Any]]
# The type of a secret field in a connector's config model: its value stays out of `repr`, logs and errors.
Secret = SecretStr


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


@dataclass(frozen=True)
class InboundRequest:
    """A webhook as it arrived. `body` is the raw bytes, which is what a signature covers, and it is untrusted until
    `verify` has passed. The names in `headers` are in lower case."""

    method: str
    path: str
    headers: Mapping[str, str]
    body: bytes
    received_at: datetime


class PushSource(Protocol):
    """A source that receives webhooks or change events instead of being read.

    Three members are optional, and `is_unsigned`, `signature_secret_of` and `event_id_of` read them:

    - `signature_secret: str`, the name under the connection's `auth` of the secret that `verify` checks against.
    - `unsigned: bool`, true to say that this source takes requests with no check at all. Loading a project refuses a
      push source that has neither of these (design 13.5).
    - `event_id(request) -> str | None`, the id the sender gives the event, by which a repeat is stored only once.
    """

    spec: ClassVar[ConnectorSpec]
    connection: str

    def verify(self, ctx: Context, request: InboundRequest) -> None:
        """Raise if the request is not genuine, for example because its signature is bad. `ctx.now` is when the request
        arrived, which is what a timestamp in it is to be checked against, however long it waited to be verified."""
        ...

    def receive(self, ctx: Context, request: InboundRequest) -> list[Mapping[str, Any] | Deleted]:
        """Return the records in the request. The runtime derives their keys and hashes."""
        ...


def is_unsigned(source: object) -> bool:
    """Whether a push source has said that it takes requests with no check."""
    return getattr(source, "unsigned", False) is True


def signature_secret_of(source: object) -> str | None:
    """The name of the `auth` entry that a push source verifies against, or None if it names none."""
    name = getattr(source, "signature_secret", None)
    return name if isinstance(name, str) and name else None


def event_id_of(source: object, request: InboundRequest) -> str | None:
    """The id of the event in `request`, if the source can tell one."""
    find = getattr(source, "event_id", None)
    return None if find is None else find(request)


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

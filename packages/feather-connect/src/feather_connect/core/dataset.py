"""Datasets: a named stream of keyed records from one source, with its key, partitions and change strategy."""

from collections.abc import Callable
from dataclasses import KW_ONLY, dataclass, field
from datetime import datetime
from typing import Literal

import pyarrow as pa
from pydantic import BaseModel

from feather_connect.core.connector import PushSource, Source
from feather_connect.core.partitions import PartitionScheme, none
from feather_connect.core.schema_policy import SchemaPolicy
from feather_connect.core.strategies import Strategy, parse_duration
from feather_connect.storage.port import Priority

OnRecordError = Literal["quarantine", "fail"]


@dataclass(frozen=True)
class Dataset:
    """A named stream of keyed records from one source entity.

    `key` names the field, or the fields, that identify a record by its business identity. `partition` cuts
    the dataset into slices and `planner`, if there is one, says which slices to read at a given time. A
    scheme that can list its own partitions doesn't need a planner.

    `keep_state` keeps the latest state of every record, which allows moves between partitions to be
    noticed. `schema` is the shape records are checked against. A record that fails is quarantined, or
    fails the whole version when `on_record_error` is `"fail"`. A complete read that returns no rows fails
    unless `allow_empty` is set. `priority` is the share of a shared budget its reads may use. `schema_policy` says
    which changes of its fields hold a delivery (`feather_connect.core.schema_policy`); a route's overrides it.
    `pii` names the fields the dashboard, the API and the CLI show masked, unless a PII reader asks to see them.
    `max_lag`, a duration such as `"2h"`, is how old its newest sealed version may be before `dataset_stale` alerts.
    """

    name: str
    _: KW_ONLY
    source: Source | PushSource
    key: str | tuple[str, ...]
    strategy: Strategy
    partition: PartitionScheme = field(default_factory=none)
    keep_state: bool = False
    priority: Priority = "normal"
    schema: type[BaseModel] | pa.Schema | None = None
    on_record_error: OnRecordError = "quarantine"
    allow_empty: bool = False
    planner: Callable[[datetime], list[str]] | None = None
    schema_policy: SchemaPolicy = "default"
    pii: tuple[str, ...] = ()  # fields shown masked (M2 plan 2 O4)
    max_lag: str | None = None  # how stale it may get before an alert (M2 plan 2 O6)

    def __post_init__(self) -> None:
        object.__setattr__(self, "pii", tuple(self.pii))
        if self.max_lag is not None:
            parse_duration(self.max_lag)  # raises for a duration it can't read

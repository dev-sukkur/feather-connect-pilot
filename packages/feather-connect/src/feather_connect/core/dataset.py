"""Datasets: a named stream of keyed records from one source, with its key, partitions and change strategy."""

from collections.abc import Callable
from dataclasses import KW_ONLY, dataclass, field
from datetime import datetime
from typing import Literal

import pyarrow as pa
from pydantic import BaseModel

from feather_connect.core.connector import PushSource, Source
from feather_connect.core.partitions import PartitionScheme, none
from feather_connect.core.strategies import Strategy
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
    unless `allow_empty` is set. `priority` is the share of a shared budget its reads may use.
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

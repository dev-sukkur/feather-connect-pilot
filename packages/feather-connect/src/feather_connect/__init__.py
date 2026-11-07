"""Feather Connect: flows written as code. Flow authors import it as `import feather_connect as fc`."""

# Connector distributions add modules to the namespace `feather_connect.connectors` from their own `src` folders. An
# editable install puts each of those folders on the path, and this lets the namespace look in every one of them.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from feather_connect.core import partitions as partition
from feather_connect.core import strategies as strategy
from feather_connect.core.connector import (
    CheckResult,
    ConnectorSpec,
    Deleted,
    InboundRequest,
    PushSource,
    Secret,
    Sink,
    Source,
    Unreadable,
)
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import PermanentError, RecordRejected, TransientError
from feather_connect.core.flow import Flow, to
from feather_connect.core.records import Record
from feather_connect.core.transforms import batch_map
from feather_connect.core.triggers import cron, every, manual, on_event

__version__ = "0.1.0"

__all__ = [
    "CheckResult",
    "ConnectorSpec",
    "Context",
    "Dataset",
    "Deleted",
    "Flow",
    "InboundRequest",
    "PermanentError",
    "PushSource",
    "Record",
    "RecordRejected",
    "Secret",
    "Sink",
    "Source",
    "TransientError",
    "Unreadable",
    "__version__",
    "batch_map",
    "cron",
    "every",
    "manual",
    "on_event",
    "partition",
    "strategy",
    "to",
]

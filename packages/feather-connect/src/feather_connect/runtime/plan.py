"""Planning: which partitions of each dataset a run reads."""

from collections.abc import Sequence

from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import PermanentError
from feather_connect.core.flow import Flow
from feather_connect.core.strategies import ByStamp
from feather_connect.runtime.deps import RuntimeDeps

_ALL_PARTITIONS = "*"


def _partitions_of(dataset: Dataset, explicit: Sequence[str] | None, deps: RuntimeDeps) -> list[str]:
    """Return what to read: the run's own list, or else the planner's, or else the scheme's recent window."""
    if isinstance(dataset.strategy, ByStamp):  # it reads once per run, across partitions, from its watermark
        return [_ALL_PARTITIONS]
    now = deps.clock.now()
    if explicit is not None:
        chosen = explicit
    elif dataset.planner is not None:
        chosen = dataset.planner(now)
    else:
        recent = dataset.partition.recent(now)
        if recent is None:
            raise PermanentError(
                f"dataset {dataset.name!r} is partitioned by a scheme that can't list its partitions: pass planner="
            )
        chosen = recent
    return list(dict.fromkeys(chosen))


def run_plan(flow: Flow, run_id: str, deps: RuntimeDeps) -> dict[str, list[str]]:
    """Return the partitions the run reads of each dataset of `flow`, and register the slices of a multi-slice read.

    A complete dataset read in more than one slice is registered, and that is what later makes its extracts stage
    their versions for `run_seal`. Calling this again registers nothing new.
    """
    explicit = deps.store.runs.get_run(run_id).params.get("partitions")
    planned: dict[str, list[str]] = {}
    for dataset in flow.datasets:
        partitions = _partitions_of(dataset, explicit, deps)
        if dataset.strategy.complete and len(partitions) > 1:
            deps.store.versions.register_slices(run_id, dataset.name, partitions)
        planned[dataset.name] = partitions
    return planned

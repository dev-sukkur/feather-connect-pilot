"""Sealing a run: once every slice of a run has been read, decide its implied deletes and seal it as one (I10)."""

from collections.abc import Sequence

from feather_connect.core.dataset import Dataset
from feather_connect.core.flow import RouteSpec
from feather_connect.runtime.deps import RuntimeDeps
from feather_connect.runtime.extract import finalize_versions
from feather_connect.storage.port import Version


def run_seal(
    dataset: Dataset, run_id: str, *, routes: Sequence[RouteSpec], deps: RuntimeDeps
) -> list[Version]:
    """Finalize all of the run's versions of a dataset and return them. Call it when the last slice has finished.

    A slice that changed nothing has no version, so it is not in the result. Calling this again changes nothing.
    """
    store = deps.store.versions
    versions = [*store.sealed_for_run(run_id, dataset.name), *store.staged_for_run(run_id, dataset.name)]
    return finalize_versions(dataset, versions, routes, deps)

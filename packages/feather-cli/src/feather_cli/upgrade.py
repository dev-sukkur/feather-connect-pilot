"""The upgrade from the project mode (flow packages §13, task 12): a mounted M1 project becomes its environment's first
package, with its history.

`upgrade_project` builds the project into a package, installs it as a promotion from `project` (every install check
but the fetch), load-checks it here in a child process, as a worker would, renames what the store keeps under the
project's names to the package's (`storage.postgres.upgrade`), moves the M1 workers' waiting tasks onto the package's
pool, and activates every flow of it. It runs in the CLI, never in the server, which never loads flow code.

Each step is skipped when it was done before, so running it again, after a crash or not, finishes what is left and
changes nothing else. Activating doesn't restart the flows' ticks: the schedule keeps its last tick under the new name,
so a tick due while the environment was stopped fires, as the flow's `catch_up` says, and no flow misses a run.

The M1 workers must be stopped first: their tasks are moved to the package's hosts, which only works when nothing
else takes them meanwhile.
"""

import hashlib
import tempfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from packaging.utils import canonicalize_name

from feather_connect.clock import Clock, SystemClock
from feather_connect.host import pool_of
from feather_connect.packages.build import build_package
from feather_connect.packages.prepare import prepare_version
from feather_connect.project import Project, load_project
from feather_connect.runtime.catalogue import sync_package_catalog
from feather_connect.storage.port import PackageVersion, Store
from feather_connect.storage.postgres.upgrade import UpgradeError, rename_to_package
from feather_server.auth import Actor
from feather_server.services.accounts import CLI
from feather_server.services.packages import Packages
from feather_server.services.registries import Registries

__all__ = [
    "UpgradeError",
    "Upgraded",
    "install_dev_version",
    "install_version",
    "m1_names",
    "upgrade_project",
]

DEFAULT_VERSION = "1.0.0"  # a project's first package version, when feather.yaml names none
_ALIVE = timedelta(seconds=30)  # a worker heard from this recently may still be taking tasks


@dataclass
class Upgraded:
    """What the upgrade did. `installed` is False when the version was installed already."""

    package: str
    version: str
    installed: bool = False
    renamed: dict[str, int] = field(default_factory=dict[str, int])
    moved: int = 0
    activated: list[str] = field(default_factory=list[str])

    def as_dict(self) -> dict[str, Any]:
        return {
            "package": self.package,
            "version": self.version,
            "installed": self.installed,
            "renamed": {k: v for k, v in self.renamed.items() if v},
            "tasks_moved": self.moved,
            "activated": self.activated,
        }


def m1_names(project: Project, package: str) -> dict[str, dict[str, str]]:
    """Each flow's, dataset's and route's name in the project, mapped to its name in the package."""
    flows = {f.name: f"{package}/{f.name}" for f in project.flows}
    datasets = {d.name: f"{package}/{d.name}" for f in project.flows for d in f.datasets}
    routes = {r.name: f"{package}/{r.name}" for f in project.flows for r in f.routes}
    return {"flow": flows, "dataset": datasets, "route": routes}


def _m1_workers(store: Store, clock: Clock) -> list[str]:
    """The workers of the project mode still heard from: any worker that isn't a package host (which has a pool)."""
    now = clock.now()
    return [
        w.worker_id
        for w in store.workers.list_workers()
        if w.state != "dead" and "pool" not in w.labels and now - w.last_seen < _ALIVE
    ]


def upgrade_project(
    root: Path,
    *,
    env: str,
    database_url: str,
    store: Store,
    version: str | None = None,
    actor: Actor = CLI,
    reason: str | None = "the upgrade from the project mode",
    clock: Clock | None = None,
    move_tasks: Callable[[str], int] | None = None,
    note: Callable[[str], None] = lambda _: None,
    keep_in_registry: bool = True,
) -> Upgraded:
    """Make the project in `root` the package of the same name, as the module says. `version` defaults to
    feather.yaml's, or `DEFAULT_VERSION`. `move_tasks` moves the shared queues' tasks to a pool and says how many
    (the DBOS engine's, by default). Raises `UpgradeError`, `ProjectError` or `ManifestError` saying what to fix."""
    clock = clock or SystemClock()
    project = load_project(root, env)
    package = str(canonicalize_name(project.name))
    chosen = version or str(project.config.get("version") or DEFAULT_VERSION)
    if running := _m1_workers(store, clock):
        raise UpgradeError(
            f"the workers {', '.join(sorted(running))} of the project mode still run: stop them (their runs finish "
            "on the package's hosts), then upgrade"
        )
    done = Upgraded(package, chosen)
    row, done.installed = install_version(
        root,
        env=env,
        store=store,
        package=package,
        version=chosen,
        actor=actor,
        reason=reason,
        clock=clock,
        note=note,
        keep_in_registry=keep_in_registry,
    )
    note("renaming the history to the package's names")
    done.renamed = rename_to_package(database_url, package, chosen, m1_names(project, package), clock.now())
    done.moved = (move_tasks or _dbos_mover(database_url))(pool_of(package, chosen))
    done.activated = _activate(store, row, package, actor, clock, reason)
    store.audit.record(actor.kind, actor.name, "upgrade_project", f"package:{package}", done.as_dict(), "ok")
    return done


def _activate(
    store: Store, row: PackageVersion, package: str, actor: Actor, clock: Clock, reason: str | None
) -> list[str]:
    """Activate each flow of the version that isn't active in some version already, keeping its ticks."""
    serving = {a.flow for a in store.packages.activations(package) if a.state in ("active", "activating")}
    started: list[str] = []
    for flow in _flows(store, row):
        if flow in serving:
            continue
        store.packages.activate(flow, package, row.version, actor.name, clock.now(), reason)
        started.append(flow)
    sync_package_catalog(store, package, row)
    return started


def _flows(store: Store, row: PackageVersion) -> Iterable[str]:
    return sorted({a.flow for a in store.packages.activations(row.name, row.version)})


def _dbos_mover(database_url: str) -> Callable[[str], int]:
    def move(pool: str) -> int:
        from feather_engine_dbos import DbosEngine

        engine = DbosEngine(database_url)
        try:
            return len(engine.move_to_pool(pool))
        finally:
            engine.close()

    return move


def install_version(
    root: Path,
    *,
    env: str,
    store: Store,
    package: str,
    version: str,
    actor: Actor = CLI,
    reason: str | None = None,
    clock: Clock | None = None,
    note: Callable[[str], None] = lambda _: None,
    keep_in_registry: bool = True,
    source: str = "project",
) -> tuple[PackageVersion, bool]:
    """Build the project in `root` as `version` of `package`, install it (every install check but the fetch) and
    load-check it here, unless that was done before. Returns the ready version, and whether it was installed now."""
    clock = clock or SystemClock()
    installed = False
    row = store.packages.version(package, version)
    if row is None:
        note(f"building {package} {version} from {root}")
        with tempfile.TemporaryDirectory(prefix="feather-upgrade-") as out:
            built = build_package(root, Path(out), env=env, version=version)
            content = built.path.read_bytes()
            packages = Packages(store, clock, Registries(store, clock), env=env)
            packages.promote(
                actor,
                built.path.name,
                content,
                sha256=hashlib.sha256(content).hexdigest(),
                source=source,
                reason=reason,
                keep_in_registry=keep_in_registry,
            )
        installed = True
        row = store.packages.version(package, version)
    if row is None or row.state == "uninstalled":
        raise UpgradeError(
            f"{package} {version} is uninstalled here: upgrade with another version (--version)"
        )
    if row.state in ("installed", "preparing", "failed"):
        note(f"load-checking {package} {version}")
        prepare_version(store, row, env=env, envs=None, worker_id="upgrade", clock=clock)
        row = store.packages.version(package, version)
    if row is None or row.state != "ready":
        error = (row.load_check.get("error") if row is not None else None) or "see its load check"
        raise UpgradeError(f"{package} {version} didn't pass its load check: {error}")
    return row, installed


DEV_KEEP = (
    3  # the dev versions `feather dev` keeps installed; older ones are uninstalled once nothing runs on them
)


def install_dev_version(
    root: Path,
    *,
    env: str,
    database_url: str,
    store: Store,
    clock: Clock | None = None,
    note: Callable[[str], None] = lambda _: None,
    keep: int = DEV_KEEP,
) -> Upgraded:
    """What `feather dev` does on start and on each save: install the project as the next dev version,
    `<version>.dev<N>`, make every flow of it the active one, and uninstall dev versions older than the last `keep`.

    A database that has M1 history under the project's old names is renamed to the package's first, as the upgrade
    does (a no-op when there is nothing left to rename). Versions are never replaced, so each save is a new one; the
    package's environment is built once per lock and reused, so a save that changes only code is quick."""
    clock = clock or SystemClock()
    project = load_project(root, env)
    package = str(canonicalize_name(project.name))
    base = str(project.config.get("version") or DEFAULT_VERSION)
    taken = [v.version for v in store.packages.versions(package)]
    numbers = [
        int(v.rsplit(".dev", 1)[1])
        for v in taken
        if v.startswith(f"{base}.dev") and v.rsplit(".dev", 1)[1].isdigit()
    ]
    version = f"{base}.dev{max(numbers, default=0) + 1}"
    done = Upgraded(package, version)
    row, done.installed = install_version(
        root,
        env=env,
        store=store,
        package=package,
        version=version,
        reason="feather dev",
        clock=clock,
        note=note,
        keep_in_registry=False,
        source="dev",
    )
    done.renamed = rename_to_package(database_url, package, version, m1_names(project, package), clock.now())
    for flow in _flows(store, row):
        store.packages.activate(flow, package, version, CLI.name, clock.now(), "feather dev")
        done.activated.append(flow)
    sync_package_catalog(store, package, row)
    _prune_dev(store, package, base, keep, note, clock, env)
    return done


def _prune_dev(
    store: Store, package: str, base: str, keep: int, note: Callable[[str], None], clock: Clock, env: str
) -> None:
    """Uninstall the dev versions older than the newest `keep`; one still in use is left for a later save."""
    dev = [
        v
        for v in store.packages.versions(package)
        if v.version.startswith(f"{base}.dev") and v.state != "uninstalled"
    ]
    dev.sort(
        key=lambda v: int(v.version.rsplit(".dev", 1)[1]) if v.version.rsplit(".dev", 1)[1].isdigit() else 0
    )
    packages = Packages(store, clock, Registries(store, clock), env=env)
    for old in dev[:-keep] if keep > 0 else dev:
        try:
            packages.uninstall(CLI, package, old.version, reason="feather dev keeps the last few")
        except Exception as error:  # noqa: BLE001 - in use, or already gone: try again on a later save
            note(f"kept {package} {old.version} for now: {error}")

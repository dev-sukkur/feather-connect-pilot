"""The scheduler of installed packages (flow packages §7.2): it fires the triggers of active flows from the catalogue,
with no package code in this process.

The catalogue is the store's active flow activations and, for each, the flow's spec as the install's load check
recorded it (`Flow.spec_dict()` of the prefixed flow), with the webhooks its package declares. It is read once and
again whenever the store says that a flow was activated or deactivated (NOTIFY in Postgres), and every `REFRESH_S`
seconds anyway, so a flow activated while the server runs fires at its next tick, with no restart, and one deactivated
stops within seconds. A read that a change overtook is not kept: every change counts a generation, and a read is
cached only if no change came while it ran.

A due tick writes a queued run of the flow, tagged with its package and version, which that version's package host
takes (task 6). Ticks are remembered in the `ScheduleStore`, as for project flows, so a restart neither repeats nor
invents one; a trigger seen for the first time notes the time and fires from then on, and a flow activated again
starts from the time it was (`restart_ticks`), with no catch-up of the time it was inactive.
"""

import logging
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, cast

from feather_connect.core.triggers import Cron, Every, Trigger
from feather_connect.packages.webhooks import Webhook, parse_webhooks
from feather_connect.runtime.scheduler import MAX_CATCH_UP, _due, trigger_id  # pyright: ignore[reportPrivateUsage]
from feather_connect.storage.port import Store

__all__ = ["REFRESH_S", "ActiveFlow", "Catalogue", "restart_ticks", "triggers_of"]

log = logging.getLogger(__name__)
REFRESH_S = 2.0  # the poll that backs NOTIFY up (§7.2)
_ACTOR = "scheduler"
_READS = 3  # reads overtaken by a change before one is used uncached


@dataclass(frozen=True)
class ActiveFlow:
    """An active flow of an installed package, as the catalogue gives it."""

    name: str
    package: str
    version: str
    triggers: tuple[Trigger, ...]
    overlap: str
    catch_up: str
    enabled: bool
    datasets: frozenset[str] = field(default_factory=frozenset[str])


@dataclass(frozen=True)
class _Read:
    flows: list[ActiveFlow]
    webhooks: dict[tuple[str, str], Webhook]


def triggers_of(spec: Mapping[str, Any]) -> tuple[Trigger, ...]:
    """The scheduled triggers of a flow spec: cron and every. The others don't fire on a clock."""
    found: list[Trigger] = []
    for raw in cast(list[dict[str, Any]], spec.get("triggers") or []):
        if raw.get("kind") == "cron":
            found.append(Cron(str(raw["expr"]), str(raw.get("tz") or "UTC")))
        elif raw.get("kind") == "every":
            found.append(Every(timedelta(seconds=float(raw["seconds"]))))
    return tuple(found)


def restart_ticks(store: Store, flow: str, spec: Mapping[str, Any], now: datetime) -> None:
    """Make a flow that is activated (again) fire from `now`: the time it was inactive is not caught up."""
    for trigger in triggers_of(spec):
        tid = trigger_id(trigger)
        if tid is None:
            continue
        last = store.schedules.last_tick(flow, tid)
        if last is not None and last < now:
            store.schedules.advance_tick(flow, tid, last, now)


class Catalogue:
    """The active flows of a store's tenant, and the webhooks they take, read again when they change. `monotonic`
    gives the time the periodic refresh counts by."""

    def __init__(self, store: Store, *, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._store = store
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._read_cache: _Read | None = None
        self._read_at = 0.0
        self._generation = 0
        self._stop: Callable[[], None] | None = None

    def listen(self) -> None:
        """Read again whenever the store says the catalogue changed."""
        if self._stop is None:
            self._stop = self._store.packages.listen(self.invalidate)

    def close(self) -> None:
        if self._stop is not None:
            self._stop()
            self._stop = None

    def invalidate(self) -> None:
        with self._lock:
            self._generation += 1
            self._read_cache = None

    def _current(self) -> _Read:
        for _ in range(_READS):
            with self._lock:
                if self._read_cache is not None and self._monotonic() - self._read_at < REFRESH_S:
                    return self._read_cache
                generation = self._generation
            found = self._read()
            with self._lock:
                if generation == self._generation:  # no change came while it read: keep it
                    self._read_cache, self._read_at = found, self._monotonic()
                    return found
        return self._read()  # changing as fast as it is read: use a fresh read, uncached

    def flows(self) -> list[ActiveFlow]:
        """The active flows, read again when they changed or `REFRESH_S` has passed."""
        return self._current().flows

    def webhook(self, package: str, name: str) -> Webhook | None:
        """The webhook `name` of `package`, when an active flow of it reads the dataset the webhook feeds."""
        return self._current().webhooks.get((package, name))

    def _read(self) -> _Read:
        found: list[ActiveFlow] = []
        hooks: dict[tuple[str, str], Webhook] = {}
        for activation in self._store.packages.active():
            version = self._store.packages.version(activation.package, activation.version)
            check: Mapping[str, Any] = version.load_check if version is not None else {}
            specs = cast(dict[str, Any], check.get("specs") or {})
            spec = cast(dict[str, Any] | None, specs.get(activation.flow))
            if spec is None:
                log.warning(
                    "active flow %s has no spec in %s %s",
                    activation.flow,
                    activation.package,
                    activation.version,
                )
                continue
            datasets = frozenset(
                str(d.get("name")) for d in cast(list[dict[str, Any]], spec.get("datasets") or [])
            )
            found.append(
                ActiveFlow(
                    activation.flow,
                    activation.package,
                    activation.version,
                    triggers_of(spec),
                    str(spec.get("overlap", "queue")),
                    str(spec.get("catch_up", "latest")),
                    bool(spec.get("enabled", True)),
                    datasets,
                )
            )
            try:
                declared = parse_webhooks(cast(dict[str, Any], check.get("webhooks") or {}))
            except ValueError:
                log.warning(
                    "%s %s declares a webhook that isn't valid", activation.package, activation.version
                )
                continue
            for hook in declared.values():
                if f"{activation.package}/{hook.dataset}" in datasets:
                    hooks[(activation.package, hook.name)] = hook
        return _Read(found, hooks)

    def tick(self, now: datetime) -> list[str]:
        """Write a run for every trigger of an active, enabled flow that is due as of `now`, and return their ids."""
        runs: list[str] = []
        for flow in self.flows():
            if not flow.enabled:  # a disabled flow writes nothing; its ticks wait for it
                continue
            for trigger in flow.triggers:
                tid = trigger_id(trigger)
                if tid is None:
                    continue
                try:
                    runs += self._tick(flow, trigger, tid, now)
                except Exception:
                    log.exception("could not tick trigger %s of flow %s", tid, flow.name)
        return runs

    def _busy(self, flow: str) -> bool:
        runs = self._store.runs
        return bool(runs.list_runs(flow, "queued", limit=1) or runs.list_runs(flow, "running", limit=1))

    def _tick(self, flow: ActiveFlow, trigger: Trigger, tid: str, now: datetime) -> list[str]:
        schedules = self._store.schedules
        last = schedules.last_tick(flow.name, tid)
        if last is None:
            schedules.advance_tick(flow.name, tid, None, now)
            return []
        due, _ = _due(trigger, last, now)
        if not due or not schedules.advance_tick(flow.name, tid, last, due[-1]):
            return []
        wanted = due[-1:] if flow.catch_up == "latest" else due[-MAX_CATCH_UP:]
        runs: list[str] = []
        for at in wanted:
            params = {"due_at": at.isoformat(), "package": flow.package, "version": flow.version}
            if flow.overlap == "skip" and self._busy(flow.name):
                runs.append(self._store.runs.create_run(flow.name, tid, _ACTOR, params, "skipped", "overlap"))
            else:
                runs.append(self._store.runs.create_run(flow.name, tid, _ACTOR, params))
        return runs

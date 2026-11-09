"""The scheduler of installed packages (flow packages §7.2): it fires the triggers of active flows from the catalogue,
with no package code in this process.

The catalogue is the store's active flow activations and, for each, the flow's spec as the install's load check
recorded it (`Flow.spec_dict()` of the prefixed flow). It is read once and again whenever the store says that a flow
was activated or deactivated (NOTIFY in Postgres), and every `REFRESH_S` seconds anyway, so a flow activated while the
server runs fires at its next tick, with no restart.

A due tick writes a queued run of the flow, tagged with its package and version, which that version's package host
takes (task 6). Ticks are remembered in the `ScheduleStore`, as for project flows, so a restart neither repeats nor
invents one; a trigger seen for the first time notes the time and fires from then on.
"""

import logging
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

from feather_connect.core.triggers import Cron, Every, Trigger
from feather_connect.runtime.scheduler import MAX_CATCH_UP, _due, trigger_id  # pyright: ignore[reportPrivateUsage]
from feather_connect.storage.port import Store

__all__ = ["REFRESH_S", "ActiveFlow", "Catalogue", "triggers_of"]

log = logging.getLogger(__name__)
REFRESH_S = 30.0
_ACTOR = "scheduler"


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


def triggers_of(spec: Mapping[str, Any]) -> tuple[Trigger, ...]:
    """The scheduled triggers of a flow spec: cron and every. The others don't fire on a clock."""
    found: list[Trigger] = []
    for raw in cast(list[dict[str, Any]], spec.get("triggers") or []):
        if raw.get("kind") == "cron":
            found.append(Cron(str(raw["expr"]), str(raw.get("tz") or "UTC")))
        elif raw.get("kind") == "every":
            found.append(Every(timedelta(seconds=float(raw["seconds"]))))
    return tuple(found)


class Catalogue:
    """The active flows of a store's tenant, read again when they change. `clock` gives the monotonic time the
    periodic refresh counts by."""

    def __init__(self, store: Store, *, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._store = store
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._flows: list[ActiveFlow] | None = None
        self._read_at = 0.0
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
            self._flows = None

    def flows(self) -> list[ActiveFlow]:
        """The active flows, read again when they changed or `REFRESH_S` has passed."""
        with self._lock:
            if self._flows is not None and self._monotonic() - self._read_at < REFRESH_S:
                return self._flows
        found = self._read()
        with self._lock:
            self._flows, self._read_at = found, self._monotonic()
        return found

    def _read(self) -> list[ActiveFlow]:
        found: list[ActiveFlow] = []
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
            found.append(
                ActiveFlow(
                    activation.flow,
                    activation.package,
                    activation.version,
                    triggers_of(spec),
                    str(spec.get("overlap", "queue")),
                    str(spec.get("catch_up", "latest")),
                    bool(spec.get("enabled", True)),
                )
            )
        return found

    def tick(self, now: datetime) -> list[str]:
        """Write a run for every trigger of an active flow that is due as of `now`, and return their ids."""
        runs: list[str] = []
        for flow in self.flows():
            for trigger in flow.triggers:
                tid = trigger_id(trigger)
                if tid is None:
                    continue
                try:
                    runs += self._tick(flow, trigger, tid, now)
                except Exception:
                    log.exception("could not tick trigger %s of flow %s", tid, flow.name)
        return runs

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
            if not flow.enabled:
                runs.append(
                    self._store.runs.create_run(flow.name, tid, _ACTOR, params, "skipped", "disabled")
                )
            elif flow.overlap == "skip" and self._store.runs.list_runs(flow.name, "queued", limit=1):
                runs.append(self._store.runs.create_run(flow.name, tid, _ACTOR, params, "skipped", "overlap"))
            else:
                runs.append(self._store.runs.create_run(flow.name, tid, _ACTOR, params))
        return runs

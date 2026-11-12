"""The readiness evaluator (readiness design §4, §6 and §7; M2 plan 1 §1): which periods of a waiting flow are ready,
worked out in the server from the store and the flows' catalogue specs, with no package code.

A pass looks at each `on_ready` trigger of the active flows. For `each="run"` the periods are the upstream runs that
sealed versions of its dataset; for `hour` and `day` they are the periods of the trigger's time zone from its `start`
(or when the evaluator first saw it) to now, no further back than `WINDOW`, per scope of its `by` field. A period that
is ready and has not fired fires: `ReadinessStore.fire` records it and queues its run, tagged with the flow's package
and version, in one transaction, so it fires once however many servers race to it. A period that fired and whose
versions changed since is late (§6): with `on_late="rerun"` it fires again, once per change.
"""

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

from feather_connect.core.readiness import PUSH_SETTLE
from feather_connect.runtime.catalogue import ActiveFlow
from feather_connect.storage.port import Firing, SealedPartition, Store

__all__ = [
    "ACTOR",
    "WINDOW",
    "Period",
    "evaluate",
    "fire_now",
    "period_of",
    "periods_of",
    "ready_triggers",
    "set_state",
    "status",
]

log = logging.getLogger(__name__)
ACTOR = "readiness"
WINDOW = timedelta(
    days=7
)  # how far back a period is looked at, so that a pass costs the same whatever the history
_TRIGGER = "ready"
_ANY = None  # a part of a partition key that any value matches


@dataclass(frozen=True)
class Period:
    """What one firing covers: `id` is its name in the firing, such as `2026-11-08` or `run:<id>`."""

    id: str
    start: datetime | None
    end: datetime | None


@dataclass(frozen=True)
class DatasetState:
    """How one dataset of a set stands for one period and scope: ready or what it waits for, and what it holds."""

    ready: bool
    waiting_for: str | None
    versions: Mapping[str, str]  # partition -> version id


def _seconds(value: Any) -> timedelta | None:
    return None if value is None else timedelta(seconds=float(value))


def periods_of(each: str, tz: str, low: datetime, now: datetime) -> list[Period]:
    """The hours or days of `tz` that end after `low` and by `now`, oldest first."""
    zone = ZoneInfo(tz)
    found: list[Period] = []
    local = low.astimezone(zone)
    if each == "day":
        day = local.date()
        while True:
            start = datetime(day.year, day.month, day.day, tzinfo=zone)
            nxt = day + timedelta(days=1)
            end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=zone)
            if end > now:
                break
            if end > low:
                found.append(Period(day.isoformat(), start, end))
            day = nxt
        return found
    start = local.replace(minute=0, second=0, microsecond=0)
    while True:
        end = (start + timedelta(hours=1)).astimezone(zone)
        if end > now:
            break
        if end > low:
            found.append(Period(start.strftime("%Y-%m-%dT%H:%M"), start, end))
        start = end
    return found


def _segment(part: Mapping[str, Any], by: str | None, scope: str | None, period: Period) -> str | None:
    """The key segment of one part of a scheme for (scope, period), or `_ANY`."""
    kind, name = part.get("kind"), part.get("field")
    if kind == "field" and name == by:
        return scope
    if kind in ("day", "month") and period.start is not None:
        day: date = period.start.astimezone(ZoneInfo(str(part.get("tz") or "UTC"))).date()
        return f"{name}={day.isoformat()}" if kind == "day" else f"{name}={day.year:04d}-{day.month:02d}"
    if kind == "none":
        return "_"
    return _ANY


def _pattern(
    partition: Mapping[str, Any], by: str | None, scope: str | None, period: Period
) -> list[str | None]:
    parts = (
        cast("list[Mapping[str, Any]]", partition.get("parts") or [])
        if partition.get("kind") == "fields"
        else [partition]
    )
    return [_segment(p, by, scope, period) for p in parts]


def _matches(key: str, pattern: Sequence[str | None]) -> bool:
    segments = key.split("/")
    return len(segments) == len(pattern) and all(
        p is None or p == s for p, s in zip(pattern, segments, strict=True)
    )


def _scopes_of(key: str, partition: Mapping[str, Any], by: str) -> str | None:
    parts = (
        cast("list[Mapping[str, Any]]", partition.get("parts") or [])
        if partition.get("kind") == "fields"
        else [partition]
    )
    segments = key.split("/")
    for part, segment in zip(parts, segments, strict=False):
        if part.get("kind") == "field" and part.get("field") == by:
            return segment
    return None


class _Reads:
    """What a pass reads of the store, each once."""

    def __init__(self, store: Store, now: datetime) -> None:
        self.store = store
        self.now = now
        self._latest: dict[str, dict[str, SealedPartition]] = {}
        self._open: dict[tuple[str, str], dict[str, int]] = {}
        self._unsealed: dict[str, datetime | None] = {}

    def latest(self, dataset: str) -> dict[str, SealedPartition]:
        if dataset not in self._latest:
            self._latest[dataset] = self.store.readiness.latest_sealed(
                dataset, self.now - WINDOW - timedelta(days=2)
            )
        return self._latest[dataset]

    def open(self, route: str, dataset: str) -> dict[str, int]:
        if (route, dataset) not in self._open:
            self._open[(route, dataset)] = self.store.readiness.open_deliveries(route, dataset)
        return self._open[(route, dataset)]

    def unsealed(self, dataset: str) -> datetime | None:
        if dataset not in self._unsealed:
            self._unsealed[dataset] = self.store.readiness.oldest_unsealed(dataset)
        return self._unsealed[dataset]


def _delivered(
    reads: _Reads, route: str | None, dataset: str, held: Iterable[SealedPartition], blocks: bool
) -> str | None:
    """None when `route` has applied every version in `held` in order, or else what it waits for."""
    if route is None:
        return None
    opened = reads.open(route, dataset)
    for version in held:
        first = opened.get(version.partition)
        if first is not None and first <= version.seal_seq:
            return f"{dataset} to be delivered to {route} ({version.partition})"
        if blocks and reads.store.ledger.get(route, version.version_id).rows_rejected:
            return f"{dataset}: {route} turned records down ({version.partition})"
    return None


def dataset_state(
    reads: _Reads,
    dataset: str,
    spec: Mapping[str, Any],
    sets: Mapping[str, Any],
    scope: str | None,
    period: Period,
) -> DatasetState:
    """Whether `dataset` is complete through the period's end for the scope (design §4), and delivered (§4.2)."""
    end = period.end
    assert end is not None
    strategy = str(spec.get("strategy"))
    partition = cast("Mapping[str, Any]", spec.get("partition") or {"kind": "none"})
    by = cast("str | None", sets.get("by"))
    route = cast("Mapping[str, str]", sets.get("delivered_to") or {}).get(dataset)
    blocks = sets.get("quarantine") == "blocks"
    settle = _seconds(sets.get("settle"))
    latest = reads.latest(dataset)
    if strategy == "by_stamp":  # it reads across partitions, from its watermark
        settled = settle if settle is not None else (_seconds(spec.get("lookback")) or timedelta(0))
        succeeded = [s for s in latest.values() if s.run_status == "succeeded"]
        through = max((s.run_created_at for s in succeeded), default=None)
        if through is None or through - settled < end:
            return DatasetState(False, f"{dataset} read through {_at(end + settled)}", {})
        held = list(latest.values())
        waiting = _delivered(reads, route, dataset, held, blocks)
        return DatasetState(waiting is None, waiting, {s.partition: s.version_id for s in held})
    pattern = _pattern(partition, by, scope, period)
    held = [s for key, s in latest.items() if _matches(key, pattern)]
    exact = None not in pattern
    closes = reads.store.readiness.closes(dataset, ["/".join(cast("list[str]", pattern))]) if exact else {}
    if strategy == "push":
        unsealed = reads.unsealed(dataset)
        if unsealed is not None and unsealed < end:
            return DatasetState(False, f"{dataset}: events up to {_at(end)} to be sealed", {})
        if not closes and reads.now < end + (settle if settle is not None else PUSH_SETTLE):
            return DatasetState(False, f"{dataset} to settle until {_at(end + (settle or PUSH_SETTLE))}", {})
    else:
        if not held:
            return DatasetState(False, f"{dataset}: a version of the period", {})
        if not closes and any(
            s.run_created_at < end for s in held
        ):  # a run that began before the period ended
            return DatasetState(False, f"{dataset}: a read after {_at(end)}", {})
    waiting = _delivered(reads, route, dataset, held, blocks)
    return DatasetState(waiting is None, waiting, {s.partition: s.version_id for s in held})


def _at(moment: datetime) -> str:
    return moment.isoformat(timespec="minutes")


def set_state(
    reads: _Reads,
    sets: Mapping[str, Any],
    datasets: Mapping[str, Mapping[str, Any]],
    scope: str | None,
    period: Period,
) -> tuple[bool, str | None, dict[str, dict[str, str]]]:
    """Whether the whole set is ready for (scope, period), the first thing it waits for, and the versions it holds."""
    versions: dict[str, dict[str, str]] = {}
    for name in cast("list[str]", sets.get("datasets") or []):
        spec = datasets.get(name)
        if spec is None:
            return False, f"{name}: upstream inactive", versions
        state = dataset_state(reads, name, spec, sets, scope, period)
        if not state.ready:
            return False, state.waiting_for, versions
        versions[name] = dict(state.versions)
    return True, None, versions


def _first_seen(store: Store, flow: str, tid: str, now: datetime) -> datetime:
    last = store.schedules.last_tick(flow, tid)
    if last is None:
        store.schedules.advance_tick(flow, tid, None, now)
        return now
    return last


def _params(flow: ActiveFlow, ready: Mapping[str, Any]) -> dict[str, Any]:
    return {"package": flow.package, "version": flow.version, "ready": dict(ready)}


def _busy(store: Store, flow: str) -> bool:
    runs = store.runs
    return bool(runs.list_runs(flow, "queued", limit=1) or runs.list_runs(flow, "running", limit=1))


def evaluate(store: Store, flows: Sequence[ActiveFlow], now: datetime) -> list[str]:
    """Fire what is ready of the `on_ready` triggers of `flows`, and return the runs it queued."""
    datasets = {
        str(d["name"]): d
        for f in flows
        for d in cast("list[Mapping[str, Any]]", f.spec.get("datasets") or [])
    }
    reads = _Reads(store, now)
    fired: list[str] = []
    for flow in flows:
        if not flow.enabled:
            continue
        for raw in cast("list[Mapping[str, Any]]", flow.spec.get("triggers") or []):
            if raw.get("kind") != "on_ready":
                continue
            try:
                fired += _trigger(store, reads, flow, raw, datasets, now)
            except Exception:
                log.exception("could not evaluate readiness of flow %s", flow.name)
    return fired


def _trigger(
    store: Store,
    reads: _Reads,
    flow: ActiveFlow,
    trigger: Mapping[str, Any],
    datasets: Mapping[str, Mapping[str, Any]],
    now: datetime,
) -> list[str]:
    sets = cast("Mapping[str, Any]", trigger["ready"])
    name = str(sets["name"])
    seen = _first_seen(store, flow.name, f"ready:{name}", now)
    start = datetime.fromisoformat(trigger["start"]) if trigger.get("start") else None
    low = max(start or seen, now - WINDOW)
    if flow.overlap == "skip" and _busy(store, flow.name):
        return []  # it waits: readiness doesn't drop a period
    if trigger.get("each") == "run":
        return _each_run(store, reads, flow, trigger, sets, datasets, low, now)
    tz = str(trigger.get("tz") or "UTC")
    periods = periods_of(
        str(trigger["each"]), tz, (start - timedelta(microseconds=1)) if start else seen, now
    )
    periods = [p for p in periods if p.end is not None and p.end > low]
    by = cast("str | None", sets.get("by"))
    scopes: list[str | None] = [None]
    if by is not None:
        found: set[str] = set()
        for dataset in cast("list[str]", sets.get("datasets") or []):
            spec = datasets.get(dataset)
            if spec is None:
                continue
            partition = cast("Mapping[str, Any]", spec.get("partition") or {})
            found |= {s for key in reads.latest(dataset) if (s := _scopes_of(key, partition, by)) is not None}
        scopes = list[str | None](sorted(found))
    done: dict[tuple[str, str], Firing] = {}
    for firing in reversed(store.readiness.firings(flow.name, name, limit=1000)):  # the newest of each wins
        done[(firing.period, firing.scope)] = firing
    queued: list[str] = []
    for scope in scopes:
        for period in periods:
            before = done.get((period.id, scope or ""))
            if before is not None and (trigger.get("on_late", "rerun") != "rerun" or before.reason == "manual"):
                continue  # a period fired by hand stays fired: what it read is what someone chose
            ready, _, versions = set_state(reads, sets, datasets, scope, period)
            if not ready:
                continue
            if before is not None and dict(before.versions) == versions:
                continue  # fired with these versions already
            seq, reason = (0, "ready") if before is None else (before.seq + 1, "late data")
            scope_of = {} if scope is None or by is None else {by: scope.split("=", 1)[-1]}
            ready_info = {
                "set": name,
                "period": period.id,
                "start": period.start.isoformat() if period.start else None,
                "end": period.end.isoformat() if period.end else None,
                "scope": scope_of,
                "versions": versions,
                "reason": reason,
            }
            firing = Firing(flow.name, name, period.id, scope or "", seq, reason, versions, now, ACTOR)
            run = store.readiness.fire(firing, _params(flow, ready_info))
            if run is not None:
                log.info("readiness: %s fired for %s %s (%s)", flow.name, period.id, scope or "", reason)
                queued.append(run)
    return queued


def _each_run(
    store: Store,
    reads: _Reads,
    flow: ActiveFlow,
    trigger: Mapping[str, Any],
    sets: Mapping[str, Any],
    datasets: Mapping[str, Mapping[str, Any]],
    low: datetime,
    now: datetime,
) -> list[str]:
    """One firing per upstream run that sealed versions of the set's dataset, once it succeeded and, with
    `delivered_to`, its versions were applied; `at_most_every` gathers the ready runs into one firing."""
    [dataset] = cast("list[str]", sets["datasets"])
    if dataset not in datasets:
        return []  # upstream inactive
    name = str(sets["name"])
    route = cast("Mapping[str, str]", sets.get("delivered_to") or {}).get(dataset)
    blocks = sets.get("quarantine") == "blocks"
    firings = store.readiness.firings(flow.name, name, limit=1000)
    fired_runs = {r for f in firings for r in cast("list[str]", f.versions.get("runs") or [])}
    by_run: dict[str, list[SealedPartition]] = {}
    for version in store.readiness.sealed_since(dataset, low):
        by_run.setdefault(version.run_id, []).append(version)
    ready: list[tuple[str, list[SealedPartition]]] = []
    for run_id, held in by_run.items():
        if run_id in fired_runs or held[0].run_status != "succeeded":
            continue
        if _delivered(reads, route, dataset, held, blocks) is None:
            ready.append((run_id, held))
    if not ready:
        return []
    gather = _seconds(trigger.get("at_most_every"))
    if gather is not None:
        if firings and now - firings[0].fired_at < gather:
            return []
        groups = [ready]
    else:
        groups = [[r] for r in ready]
    queued: list[str] = []
    for group in groups:
        runs = [run_id for run_id, _ in group]
        versions = {dataset: {v.partition: v.version_id for _, held in group for v in held}}
        ready_info = {
            "set": name,
            "period": f"run:{runs[-1]}",
            "runs": runs,
            "versions": versions,
            "reason": "ready",
        }
        firing = Firing(
            flow.name, name, f"run:{runs[-1]}", "", 0, "ready", {"runs": runs, **versions}, now, ACTOR
        )
        run = store.readiness.fire(firing, _params(flow, ready_info))
        if run is not None:
            log.info("readiness: %s fired after run(s) %s", flow.name, ", ".join(runs))
            queued.append(run)
    return queued


SHOWN = {"day": 7, "hour": 24}  # how many of the latest periods a status shows, per scope


def period_of(each: str, tz: str, period: str) -> Period:
    """The period named `period` (`2026-11-08`, `2026-11-08T13:00`) of a trigger counting `each` in `tz`. Raises
    `ValueError` for a name that is not one of its periods."""
    zone = ZoneInfo(tz)
    if each == "day":
        day = date.fromisoformat(period)
        nxt = day + timedelta(days=1)
        return Period(
            period,
            datetime(day.year, day.month, day.day, tzinfo=zone),
            datetime(nxt.year, nxt.month, nxt.day, tzinfo=zone),
        )
    if each == "hour":
        start = datetime.fromisoformat(period).replace(tzinfo=zone)
        if start.minute or start.second:
            raise ValueError(f"{period!r} is not the start of an hour")
        return Period(start.strftime("%Y-%m-%dT%H:%M"), start, (start + timedelta(hours=1)).astimezone(zone))
    raise ValueError(f"a flow that waits for each run has no periods to name, such as {period!r}")


def ready_triggers(flow: ActiveFlow) -> list[Mapping[str, Any]]:
    """The flow's `on_ready` triggers, as its spec gives them."""
    return [
        t
        for t in cast("list[Mapping[str, Any]]", flow.spec.get("triggers") or [])
        if t.get("kind") == "on_ready"
    ]


def status(
    store: Store, flows: Sequence[ActiveFlow], now: datetime, flow: str | None = None
) -> list[dict[str, Any]]:
    """How each readiness set of the active flows stands (design §8), for the API, the CLI and the dashboard: per set,
    its latest periods and scopes, each `fired` (with its run), `late`, or `waiting` (with what for)."""
    datasets = {
        str(d["name"]): d
        for f in flows
        for d in cast("list[Mapping[str, Any]]", f.spec.get("datasets") or [])
    }
    reads = _Reads(store, now)
    found: list[dict[str, Any]] = []
    for active_flow in flows:
        if flow is not None and active_flow.name != flow:
            continue
        for trigger in ready_triggers(active_flow):
            sets = cast("Mapping[str, Any]", trigger["ready"])
            name = str(sets["name"])
            firings = store.readiness.firings(active_flow.name, name, limit=1000)
            rows: list[dict[str, Any]] = []
            if trigger.get("each") == "run":
                rows = [
                    {
                        "period": f.period,
                        "scope": f.scope,
                        "state": "fired",
                        "run_id": f.run_id,
                        "reason": f.reason,
                        "fired_at": f.fired_at.isoformat(),
                        "runs": list(cast("list[str]", f.versions.get("runs") or [])),
                    }
                    for f in firings[:20]
                ]
            else:
                each, tz = str(trigger["each"]), str(trigger.get("tz") or "UTC")
                latest = periods_of(
                    each,
                    tz,
                    now - (timedelta(days=SHOWN["day"]) if each == "day" else timedelta(hours=SHOWN["hour"])),
                    now,
                )
                by = cast("str | None", sets.get("by"))
                scopes: list[str | None] = [None]
                if by is not None:
                    keys: set[str] = set()
                    for dataset in cast("list[str]", sets.get("datasets") or []):
                        spec = datasets.get(dataset)
                        if spec is not None:
                            partition = cast("Mapping[str, Any]", spec.get("partition") or {})
                            keys |= {
                                s
                                for k in reads.latest(dataset)
                                if (s := _scopes_of(k, partition, by)) is not None
                            }
                    scopes = list[str | None](sorted(keys))
                done: dict[tuple[str, str], Firing] = {}
                for firing in reversed(firings):
                    done[(firing.period, firing.scope)] = firing
                for scope in scopes:
                    for period in reversed(latest):
                        ready, waiting, versions = set_state(reads, sets, datasets, scope, period)
                        before = done.get((period.id, scope or ""))
                        row: dict[str, Any] = {"period": period.id, "scope": scope or ""}
                        if before is None:
                            row |= {"state": "ready" if ready else "waiting", "waiting_for": waiting}
                        elif ready and dict(before.versions) != versions and before.reason != "manual":
                            row |= {"state": "late", "run_id": before.run_id, "reason": before.reason}
                        else:
                            row |= {
                                "state": "fired",
                                "run_id": before.run_id,
                                "reason": before.reason,
                                "fired_at": before.fired_at.isoformat(),
                            }
                        rows.append(row)
            found.append(
                {
                    "flow": active_flow.name,
                    "set": name,
                    "datasets": list(cast("list[str]", sets.get("datasets") or [])),
                    "delivered_to": dict(cast("Mapping[str, str]", sets.get("delivered_to") or {})),
                    "by": sets.get("by"),
                    "each": trigger.get("each"),
                    "tz": trigger.get("tz"),
                    "on_late": trigger.get("on_late"),
                    "periods": rows,
                }
            )
    return found


def fire_now(
    store: Store, flow: ActiveFlow, period: str, scope: str, actor: str, reason: str, now: datetime
) -> str:
    """Fire a period of the flow's first readiness set by hand (design §8, *Fire now*), whether or not it is ready: a
    new firing after any it had, with the reason `manual`. Raises `ValueError` for a flow with no readiness trigger or
    a period it doesn't have."""
    triggers = ready_triggers(flow)
    if not triggers:
        raise ValueError(f"flow {flow.name!r} has no on_ready trigger")
    trigger = triggers[0]
    sets = cast("Mapping[str, Any]", trigger["ready"])
    name = str(sets["name"])
    by = cast("str | None", sets.get("by"))
    if trigger.get("each") == "run":
        found = Period(f"manual:{now.isoformat()}", None, None)
    else:
        found = period_of(str(trigger["each"]), str(trigger.get("tz") or "UTC"), period)
    if scope and (by is None or not scope.startswith(f"{by}=")):
        raise ValueError(f"scope {scope!r} is not a value of the set's by field ({by})")
    before = [
        f
        for f in store.readiness.firings(flow.name, name, limit=1000)
        if (f.period, f.scope) == (found.id, scope)
    ]
    seq = max((f.seq for f in before), default=-1) + 1
    info = {
        "set": name,
        "period": found.id,
        "start": found.start.isoformat() if found.start else None,
        "end": found.end.isoformat() if found.end else None,
        "scope": {by: scope.split("=", 1)[1]} if scope and by else {},
        "versions": {},
        "reason": "manual",
        "note": reason,
    }
    firing = Firing(flow.name, name, found.id, scope, seq, "manual", {}, now, actor)
    run = store.readiness.fire(firing, _params(flow, info))
    if run is None:  # another firing took this seq meanwhile
        raise ValueError(f"period {found.id} of {flow.name!r} fired meanwhile; look again")
    return run

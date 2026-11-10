"""Everything an operator or a token can do. Each action takes the `Actor`, goes to the runtime and writes an audit row.

A mass action takes two steps: `preview_*` says what it would touch and hands out a confirmation token, and the action
itself takes the token (see `previews`). Previews change nothing and are not audited. Every other call is audited,
whether it worked or not, with the actor and the reason. Before anything, each checks the actor's role may do it
(`access.require`, by the capability `CAPABILITIES` names for the action).
"""

import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Literal
from zoneinfo import ZoneInfo

from feather_connect.core.connector import CheckResult
from feather_connect.core.context import Context
from feather_connect.core.dataset import Dataset
from feather_connect.core.errors import PermanentError
from feather_connect.core.flow import Flow
from feather_connect.logs import redact, register_secrets
from feather_connect.project import ProjectError
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.runtime.catalogue import ActiveFlow
from feather_connect.runtime.extract import VerifyResult
from feather_connect.runtime.runtime import FlowPaused
from feather_connect.storage.port import Delivery, DeliveryStatus, InvalidTransition, split_error
from feather_server.auth import Actor
from feather_server.catalog_view import CatalogDataset, dataset_of
from feather_server.services.access import Capability, require
from feather_server.services.previews import (
    Impact,
    Preview,
    PreviewError,
    PreviewInvalid,
    PreviewTokens,
    fingerprint,
)

if TYPE_CHECKING:
    from feather_server.app import AppState

log = logging.getLogger(__name__)

DeliveryAction = Literal["retry", "skip", "replay"]
MAX_SELECTION = 10_000  # deliveries one confirmation may touch
MAX_BACKFILL_DAYS = 366
_PAGE = 500
_SAMPLES = 5
_STATUSES: dict[str, tuple[DeliveryStatus, ...]] = {
    "retry": ("failed",),
    "skip": ("failed", "held"),
    "replay": ("applied",),
}
CHECK_TIMEOUT_S = 15  # how long a connection check may take before it is given up on
_SIMULATED = ("simulation.", "memory.")  # connectors of tests and demos, which connect to nothing
CAPABILITIES: dict[str, Capability] = {  # each action, as its audit rows name it, and the capability it needs
    "run_flow": "runs.start",
    "rerun": "runs.rerun",
    "backfill": "runs.backfill",
    "cancel_run": "runs.cancel",
    "verify_partition": "partitions.verify",
    "retry_deliveries": "deliveries.retry",
    "skip_deliveries": "deliveries.skip",
    "replay_deliveries": "deliveries.replay",
    "dismiss_quarantine": "quarantine.dismiss",
    "reprocess_event": "events.reprocess",
    "release_event": "events.release",
    "pause_flow": "flows.pause",
    "resume_flow": "flows.pause",
    "pause_route": "routes.pause",
    "resume_route": "routes.pause",
    "check_connection": "connections.check",
}


@dataclass(frozen=True)
class DeliverySelection:
    """Which deliveries a bulk action is about. Every field given narrows the selection; `error` and `error_class` are
    those of one failure group, and `version_ids` names the versions to take.

    `follow_latest` is what a replay preview sets when it wasn't given versions: `version_ids` holds the latest version
    of each stream as the preview saw it, and the replay itself goes to whatever is the latest by then. So new versions
    don't make the preview stale, and no older data goes out because of them."""

    route: str | None = None
    dataset: str | None = None
    partition: str | None = None
    error: str | None = None
    error_class: str | None = None
    version_ids: tuple[str, ...] | None = None
    follow_latest: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "route": self.route,
            "dataset": self.dataset,
            "partition": self.partition,
            "error": self.error,
            "error_class": self.error_class,
            "version_ids": None if self.version_ids is None else list(self.version_ids),
            "follow_latest": self.follow_latest,
        }

    @classmethod
    def from_dict(cls, found: Mapping[str, Any]) -> "DeliverySelection":
        versions = found.get("version_ids")
        return cls(
            found.get("route"),
            found.get("dataset"),
            found.get("partition"),
            found.get("error"),
            found.get("error_class"),
            None if versions is None else tuple(versions),
            bool(found.get("follow_latest")),
        )


def _label(dataset: Dataset | None, partition: str) -> str:
    if dataset is None:  # a packaged dataset: the server has no partition object for it (catalog_view)
        return partition
    try:
        return dataset.partition.label_of(partition)
    except (ValueError, KeyError):
        return partition


def _date_of(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else None


def backfill_partitions(flow: Flow, start: date, end: date, timezone: str) -> list[str]:
    """The partitions of `flow` that cover the days from `start` to `end` (both included).

    Each dataset's scheme names its own partitions, so this asks for the ones planned around each day and keeps those
    whose date limits overlap the range. Every dataset of the flow must come out with the same partitions, because a run
    reads one list of them.
    """
    if start > end:
        raise ValueError("start must be on or before end")
    if (end - start).days + 1 > MAX_BACKFILL_DAYS:
        raise ValueError(f"a backfill covers at most {MAX_BACKFILL_DAYS} days")
    zone = ZoneInfo(timezone)
    found: list[set[str]] = []
    for dataset in flow.datasets:
        scheme, keys = dataset.partition, set[str]()
        for offset in range((end - start).days + 3):
            moment = datetime.combine(start + timedelta(days=offset - 1), time(12), zone)
            planned = scheme.recent(moment)
            if planned is None:
                raise ValueError(
                    f"dataset {dataset.name!r} can't list its partitions, so it can't be backfilled"
                )
            for key in planned:
                for bound in scheme.bounds(key).values():
                    low, high = _date_of(bound.low), _date_of(bound.high)
                    if low is None or high is None:
                        continue
                    if (low == high and start <= low <= end) or (low != high and low <= end and high > start):
                        keys.add(key)
        if not keys:
            raise ValueError(
                f"dataset {dataset.name!r} is not partitioned by date, so it can't be backfilled"
            )
        found.append(keys)
    if any(keys != found[0] for keys in found):
        raise ValueError("the datasets of this flow are partitioned differently, so it can't be backfilled")
    return sorted(found[0])


class Actions:
    """The actions of the dashboard and the API, over one `AppState`."""

    def __init__(self, state: "AppState") -> None:
        self._state = state
        self._rt = state.runtime
        self._store = state.store
        self._tokens = PreviewTokens(state.store, state.settings.secret_key, state.clock)

    @property
    def protected(self) -> bool:
        """Whether this is a protected environment (`settings.protected` in the project), where a skip needs a
        reason."""
        return bool(self._state.project.settings.get("protected"))

    # Bookkeeping.

    def _record(
        self, actor: Actor, action: str, target: str | None, request: Mapping[str, Any], result: str
    ) -> None:
        self._store.audit.record(actor.kind, actor.name, action, target, request, result)

    def _require(self, actor: Actor, action: str, target: str | None = None) -> None:
        require(actor, CAPABILITIES[action], self._store.audit, target)

    def _issue(self, actor: Actor, action: str, selection: Mapping[str, Any], print_: str) -> str:
        """A confirmation token for a preview, once the actor may do the action."""
        self._require(actor, action)
        return self._tokens.issue(action, actor.name, selection, print_)

    def _do[T](
        self,
        actor: Actor,
        action: str,
        target: str | None,
        request: Mapping[str, Any],
        fn: Callable[[], T],
        note: Callable[[T], str] = lambda _: "ok",
    ) -> T:
        """Do the action and write its audit row: `ok` and what `note` says of the result, or `failed` and why."""
        self._require(actor, action, target)
        try:
            result = fn()
        except Exception as error:
            self._record(actor, action, target, request, f"failed: {redact(str(error))}")
            raise
        self._record(actor, action, target, request, note(result))
        return result

    def _redeem(
        self,
        actor: Actor,
        action: str,
        token: str,
        reason: str | None,
        fingerprint_of: Callable[[Mapping[str, Any]], str],
    ) -> Mapping[str, Any]:
        self._require(actor, action)  # before the token is spent
        try:
            return self._tokens.redeem(token, action, actor.name, fingerprint_of)
        except PreviewError as error:
            self._record(actor, action, None, {"reason": reason}, f"failed: {error}")
            raise

    def _dataset_flow(self, name: str) -> tuple[Flow, Dataset]:
        for flow in self._rt.flows():
            for dataset in flow.datasets:
                if dataset.name == name:
                    return flow, dataset
        raise KeyError(f"no dataset named {name!r}")

    def _rows(self, dataset: Dataset, partitions: Sequence[str]) -> int | None:
        """How many records the partitions held when last sealed, or None if any was never."""
        total = 0
        for partition in partitions:
            version = self._store.versions.latest_sealed(dataset.name, partition)
            if version is None:
                return None
            total += version.upserts + version.deletes
        return total

    def _impact(self, flow: Flow, partitions: Sequence[str]) -> Impact:
        """What reading `partitions` of every dataset of `flow` would touch."""
        units: int | None = 0
        for dataset in flow.datasets:
            cost = getattr(dataset.source, "spec", None)
            per_row = None if cost is None else cost.cost.get("read")
            rows = self._rows(dataset, partitions)
            units = None if units is None or per_row is None or rows is None else units + rows * per_row
        first = flow.datasets[0]
        return Impact(
            len(partitions),
            len(partitions) * len(flow.datasets) * len(flow.routes),
            units,
            tuple(_label(first, p) for p in partitions[:_SAMPLES]),
        )

    def _check_partitions(self, dataset: Dataset, partitions: Sequence[str]) -> list[str]:
        chosen = list(dict.fromkeys(partitions))
        if not chosen:
            raise ValueError("choose at least one partition")
        for partition in chosen:
            try:
                dataset.partition.label_of(partition)
            except (ValueError, KeyError):
                raise ValueError(f"{partition!r} is not a partition of dataset {dataset.name!r}") from None
        return chosen

    # Runs.

    def run_flow(
        self,
        actor: Actor,
        flow: str,
        *,
        reason: str | None = None,
        partitions: Sequence[str] | None = None,
        max_concurrent: int | None = None,
    ) -> str:
        """Start a run of the flow now, of `partitions` if they are given (as a re-run with new inputs does). Raises
        `FlowPaused` for a paused flow, `KeyError` for an unknown one and `ValueError` for a partition that is not one
        of the flow's.

        A flow of an installed package (§13: every flow, since the project mode is retired) runs on its active
        version's host: the run is queued, tagged with that version, as a tick queues one, and the host plans it."""
        if (packaged := self._packaged(flow)) is not None:
            return self._do(
                actor,
                "run_flow",
                f"flow:{flow}",
                {"reason": reason, **({} if partitions is None else {"partitions": list(partitions)})},
                lambda: self._queue_packaged(packaged, actor, reason, partitions, max_concurrent),
                lambda run_id: f"ok: run {run_id}",
            )
        found = self._rt.flow(flow)
        chosen = None if partitions is None else list(dict.fromkeys(partitions))
        if chosen is not None:
            for dataset in found.datasets:
                self._check_partitions(dataset, chosen)
        return self._do(
            actor,
            "run_flow",
            f"flow:{flow}",
            {"reason": reason, **({} if chosen is None else {"partitions": chosen})},
            lambda: self._rt.start_run(
                flow,
                trigger="manual",
                actor=actor.name,
                partitions=chosen,
                reason=reason,
                max_concurrent=max_concurrent,
            ),
            lambda run_id: f"ok: run {run_id}",
        )

    def _dataset_or_none(self, name: str) -> Dataset | None:
        try:
            return self._rt.dataset(name)
        except KeyError:
            return None

    def _packaged(self, flow: str) -> ActiveFlow | None:
        """The active package flow of that name, when the runtime here has no flow of it."""
        if flow in {f.name for f in self._rt.flows()}:
            return None
        catalogue = self._state.catalogue
        if catalogue is None:
            return None
        return next((f for f in catalogue.flows() if f.name == flow), None)

    def _queue_packaged(
        self,
        flow: ActiveFlow,
        actor: Actor,
        reason: str | None,
        partitions: Sequence[str] | None,
        max_concurrent: int | None,
        *,
        trigger: str = "manual",
    ) -> str:
        if self._rt.is_flow_paused(flow.name):
            raise FlowPaused(f"flow {flow.name!r} is paused")
        params: dict[str, Any] = {"package": flow.package, "version": flow.version}
        if partitions is not None:
            params["partitions"] = list(dict.fromkeys(partitions))
        if max_concurrent is not None:
            params["max_concurrent"] = max_concurrent
        return self._store.runs.create_run(flow.name, trigger, actor.name, params, reason=reason)

    def _pause_target(self, flow: str) -> None:
        """Raises `KeyError` for a flow that is neither the runtime's nor an installed package's."""
        package = flow.split("/", 1)[0] if "/" in flow else None
        if package is None or not any(a.flow == flow for a in self._store.packages.activations(package)):
            self._rt.flow(flow)

    def _packaged_dataset(self, dataset: str) -> tuple[ActiveFlow, CatalogDataset] | None:
        """The active flow and catalogue entry of a packaged dataset; None when the runtime here has the dataset.
        Raises `KeyError` for a dataset neither has."""
        try:
            self._rt.dataset(dataset)
        except KeyError:
            found = dataset_of(self._state, dataset)
            active = self._packaged(found.flow)
            if active is None:
                raise KeyError(f"no dataset is named {dataset!r}") from None
            return active, found
        return None

    def preview_rerun(self, actor: Actor, dataset: str, partitions: Sequence[str]) -> Preview:
        """Say what reading these partitions of the dataset again would touch."""
        packaged = self._packaged_dataset(dataset)
        if packaged is not None:  # its host checks the partitions as it plans; its read cost is in its code
            chosen = list(dict.fromkeys(partitions))
            if not chosen:
                raise ValueError("choose at least one partition")
            selection = {"dataset": dataset, "partitions": chosen}
            token = self._issue(actor, "rerun", selection, self._rerun_print(selection))
            impact = Impact(
                len(chosen), len(chosen) * len(packaged[1].routes), None, tuple(chosen[:_SAMPLES])
            )
            return Preview(token, "rerun", impact)
        flow, found = self._dataset_flow(dataset)
        chosen = self._check_partitions(found, partitions)
        selection = {"dataset": dataset, "partitions": chosen}
        token = self._issue(actor, "rerun", selection, self._rerun_print(selection))
        return Preview(token, "rerun", self._impact(flow, chosen))

    @staticmethod
    def _rerun_print(selection: Mapping[str, Any]) -> str:
        return fingerprint("rerun", selection["dataset"], sorted(selection["partitions"]))

    def rerun(self, actor: Actor, token: str, *, reason: str | None = None) -> str:
        """Read the previewed partitions again, in a run of their flow. Returns the run."""
        selection = self._redeem(actor, "rerun", token, reason, self._rerun_print)
        packaged = self._packaged_dataset(selection["dataset"])
        if packaged is not None:
            active = packaged[0]
            return self._do(
                actor,
                "rerun",
                f"dataset:{selection['dataset']}",
                {"reason": reason, "partitions": selection["partitions"]},
                lambda: self._queue_packaged(
                    active, actor, reason, selection["partitions"], None, trigger="rerun"
                ),
                lambda run_id: f"ok: run {run_id}",
            )
        flow, _ = self._dataset_flow(selection["dataset"])
        return self._do(
            actor,
            "rerun",
            f"dataset:{selection['dataset']}",
            {"reason": reason, "partitions": selection["partitions"]},
            lambda: self._rt.start_run(
                flow.name,
                trigger="rerun",
                actor=actor.name,
                partitions=selection["partitions"],
                reason=reason,
            ),
            lambda run_id: f"ok: run {run_id}",
        )

    def preview_backfill(
        self, actor: Actor, flow: str, *, start: date, end: date, max_concurrent: int = 4
    ) -> Preview:
        """Say what reading the days from `start` to `end` (both included) would touch, `max_concurrent` slices at a
        time."""
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent is {max_concurrent}: it must be at least 1")
        found = self._rt.flow(flow)
        keys = backfill_partitions(found, start, end, str(self._state.project.settings["timezone"]))
        selection = {
            "flow": flow,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "max_concurrent": max_concurrent,
            "partitions": keys,
        }
        token = self._issue(actor, "backfill", selection, self._backfill_print(selection))
        return Preview(token, "backfill", self._impact(found, keys))

    @staticmethod
    def _backfill_print(selection: Mapping[str, Any]) -> str:
        return fingerprint("backfill", selection)

    def backfill(self, actor: Actor, token: str, *, reason: str | None = None) -> str:
        """Run the previewed backfill, with the limit it was previewed with. Returns the run."""
        selection = self._redeem(actor, "backfill", token, reason, self._backfill_print)
        return self._do(
            actor,
            "backfill",
            f"flow:{selection['flow']}",
            {
                "reason": reason,
                "start": selection["start"],
                "end": selection["end"],
                "max_concurrent": selection["max_concurrent"],
                "partitions": len(selection["partitions"]),
            },
            lambda: self._rt.start_run(
                selection["flow"],
                trigger="backfill",
                actor=actor.name,
                partitions=selection["partitions"],
                reason=reason,
                max_concurrent=selection["max_concurrent"],
            ),
            lambda run_id: f"ok: run {run_id}",
        )

    def cancel_run(self, actor: Actor, run_id: str, *, reason: str | None = None) -> None:
        """Stop the run's queued and running tasks and end it `cancelled`, with the reason."""
        self._do(
            actor,
            "cancel_run",
            f"run:{run_id}",
            {"reason": reason},
            lambda: self._rt.cancel_run(run_id, actor=actor.name, reason=reason),
        )

    def verify_partition(self, actor: Actor, dataset: str, partition: str) -> VerifyResult:
        """Read the partition from its source and report what differs from the last sealed version. It changes
        nothing, but it does call the source, and that costs budget like any read."""
        _, found = self._dataset_flow(dataset)
        (chosen,) = self._check_partitions(found, [partition])
        return self._do(
            actor,
            "verify_partition",
            f"dataset:{dataset}",
            {"partition": chosen},
            lambda: self._rt.verify_partition(dataset, chosen),
            lambda r: f"ok: {r.upserts} upserts and {r.deletes} deletes differ",
        )

    # Deliveries.

    def _resolve(self, action: str, selection: DeliverySelection) -> list[Delivery]:
        """The deliveries the selection takes now, in seal order."""
        statuses = _STATUSES.get(action)
        if statuses is None:
            raise ValueError(f"unknown action {action!r}: use one of {', '.join(_STATUSES)}")
        if action == "replay" and not (
            selection.route or selection.dataset or selection.partition or selection.version_ids
        ):
            raise ValueError("replay needs a route, a dataset, a partition or versions to replay")
        ledger, found = self._store.ledger, []
        for status in statuses:
            cursor = None
            while page := ledger.list_deliveries(
                route=selection.route,
                status=status,
                partition=selection.partition,
                limit=_PAGE,
                cursor=cursor,
            ):
                for d in page:
                    error_class, message = split_error(d.last_error)
                    if (
                        (selection.dataset is None or d.dataset == selection.dataset)
                        and (selection.error is None or message == selection.error)
                        and (selection.error_class is None or error_class == selection.error_class)
                        and (selection.version_ids is None or d.version_id in selection.version_ids)
                    ):
                        found.append(d)
                if len(found) > MAX_SELECTION:
                    raise ValueError(f"the selection has more than {MAX_SELECTION} deliveries: narrow it")
                cursor = f"{page[-1].seal_seq}:{page[-1].route}"
        return sorted(found, key=lambda d: (d.seal_seq, d.route))

    @staticmethod
    def _latest_of_each_stream(found: Sequence[Delivery]) -> list[Delivery]:
        """Keep the newest of each (route, dataset, partition). `found` is in seal order."""
        latest = {(d.route, d.dataset, d.partition): d for d in found}
        return sorted(latest.values(), key=lambda d: (d.seal_seq, d.route))

    def _chosen(self, action: str, selection: DeliverySelection) -> list[Delivery]:
        found = self._resolve(action, selection)
        return self._latest_of_each_stream(found) if selection.follow_latest else found

    def _newest_now(self, delivery: Delivery) -> Delivery:
        """Return the applied delivery of the stream's latest version, or `delivery` if the latest isn't applied, which
        the runtime then refuses, or takes if the newer ones were skipped."""
        latest = self._store.versions.latest_sealed(delivery.dataset, delivery.partition)
        if latest is None:
            return delivery
        try:
            newest = self._store.ledger.get(delivery.route, latest.version_id)
        except KeyError:
            return delivery
        return newest if newest.status == "applied" else delivery

    @staticmethod
    def _deliveries_print(action: str, found: Sequence[Delivery]) -> str:
        return fingerprint(
            action,
            [
                (d.route, d.version_id, d.status, d.attempts, d.retries, d.replays, d.last_error)
                for d in found
            ],
        )

    def preview_deliveries(
        self, actor: Actor, action: DeliveryAction, selection: DeliverySelection
    ) -> Preview:
        """Say what retrying, skipping or replaying the selected deliveries would touch."""
        found = self._resolve(action, selection)
        if not found:
            raise ValueError("the selection matches nothing")
        if action == "replay":
            if selection.version_ids is None:
                found = self._latest_of_each_stream(found)
                selection = replace(
                    selection, version_ids=tuple(d.version_id for d in found), follow_latest=True
                )
                found = self._chosen(action, selection)
            for d in found:
                refusal = self._rt.replay_refusal(d.route, d.version_id)
                if refusal is not None:
                    raise ValueError(refusal)
        token = self._issue(
            actor, f"{action}_deliveries", selection.as_dict(), self._deliveries_print(action, found)
        )
        return Preview(token, f"{action}_deliveries", self._delivery_impact(action, found))

    def _delivery_impact(self, action: str, found: Sequence[Delivery]) -> Impact:
        units: int | None = 0
        if action != "skip":  # a skipped delivery makes no calls
            versions = {v.version_id: v for v in self._store.versions.get_many({d.version_id for d in found})}
            for d in found:
                try:
                    cost = self._rt.route(d.route).sink.spec.cost.get("write")
                except KeyError:
                    cost = None  # a packaged route: its cost is in its package's code (catalog_view)
                version = versions.get(d.version_id)
                units = (
                    None
                    if units is None or cost is None or version is None
                    else units + (version.upserts + version.deletes) * cost
                )
        by_dataset = {d.dataset: self._dataset_or_none(d.dataset) for d in found}
        return Impact(
            len({(d.dataset, d.partition) for d in found}),
            len(found),
            units,
            tuple(
                f"{d.route} · {_label(by_dataset[d.dataset], d.partition)}"
                + (f" · version {d.version_id}" if action == "replay" else "")
                for d in found[:_SAMPLES]
            ),
        )

    @staticmethod
    def tokens_action(token: str) -> str:
        """The action a confirmation token says it confirms, before it is checked."""
        return PreviewTokens.action_of(token)

    def apply_deliveries(self, actor: Actor, token: str, *, reason: str | None = None) -> list[str]:
        """Do the previewed retry, skip or replay. Returns `route:version` for each delivery it moved.

        A delivery the runtime refuses to move is left as it is and named in the audit row; the others go ahead. When
        it refuses them all, this raises `ValueError` with the reason, since nothing was done to report as done.
        """
        confirmed = self._tokens.action_of(token)
        action = confirmed.removesuffix("_deliveries")
        if action not in _STATUSES or action == confirmed:
            raise PreviewInvalid(f"this token confirms {confirmed!r}, not a delivery action")
        if action == "skip" and self.protected and not (reason and reason.strip()):
            raise ValueError("a reason is required to skip deliveries in a protected environment")
        selection = self._redeem(
            actor,
            confirmed,
            token,
            reason,
            lambda stored: self._deliveries_print(
                action, self._chosen(action, DeliverySelection.from_dict(stored))
            ),
        )
        wanted = DeliverySelection.from_dict(selection)
        found = self._chosen(action, wanted)
        if wanted.follow_latest:
            found = self._latest_of_each_stream([self._newest_now(d) for d in found])
        runtime_call = {"retry": self._rt.retry, "skip": self._rt.skip, "replay": self._rt.replay}[action]
        refused: list[str] = []

        def move() -> list[str]:
            moved: list[str] = []
            for d in found:
                try:
                    runtime_call(d.route, d.version_id, actor=actor.name, reason=reason)
                    moved.append(f"{d.route}:{d.version_id}")
                except (InvalidTransition, PermanentError) as error:
                    refused.append(f"{d.route}:{d.version_id}: {error}")
            if refused and not moved:
                raise ValueError(refused[0])
            return moved

        def note(moved: list[str]) -> str:
            if not refused:
                return f"ok: {len(moved)} deliveries"
            return f"partial: {len(moved)} of {len(found)} deliveries; refused {refused[0]}"

        return self._do(
            actor,
            confirmed,
            f"route:{wanted.route}" if wanted.route else "deliveries",
            {"reason": reason, "selection": wanted.as_dict(), "count": len(found)},
            move,
            note,
        )

    def dismiss_quarantine(
        self, actor: Actor, route: str, version_id: str, key: str, *, reason: str | None = None
    ) -> None:
        """Close the quarantine entry of the record `key`, and any older one of the same record on the route.

        Raises `LookupError` when the route has no open entry for it from that version or before.
        """

        def dismiss() -> None:
            sealed = self._store.versions.get(version_id).seal_seq
            if sealed is None or not self._store.quarantine.supersede(route, key, sealed + 1):
                raise LookupError(
                    f"route {route!r} has no open quarantine entry for {key!r} from {version_id}"
                )

        self._do(
            actor,
            "dismiss_quarantine",
            f"route:{route}",
            {"reason": reason, "version_id": version_id, "key": key},
            dismiss,
        )

    # Push events that the sealer held in the quarantine, whole.

    def reprocess_event(
        self, actor: Actor, dataset: str, event_seq: int, *, reason: str | None = None
    ) -> None:
        """Give a held event back to the sealer, which reads it again from the request it stored. It seals after what
        has sealed since, so a keyed route ends up with its records unless a later event of the same key follows.

        Closes its quarantine entry; reading it again opens a new one if it still fails. Raises `LookupError` when the
        dataset holds no such event.
        """
        self._settle_event(actor, "reprocess_event", dataset, event_seq, reason, self._store.inbox.reprocess)

    def release_event(self, actor: Actor, dataset: str, event_seq: int, *, reason: str | None = None) -> None:
        """Let a held event go: it is sealed with no version, its quarantine entry closes and retention may drop its day.

        Raises `LookupError` when the dataset holds no such event.
        """
        self._settle_event(actor, "release_event", dataset, event_seq, reason, self._store.inbox.release)

    def _settle_event(
        self,
        actor: Actor,
        action: str,
        dataset: str,
        event_seq: int,
        reason: str | None,
        settle: Callable[[str, int], bool],
    ) -> None:
        def do() -> None:
            if not settle(dataset, event_seq):
                raise LookupError(f"dataset {dataset!r} holds no event {event_seq} in the quarantine")
            self._store.quarantine.close_extraction(dataset, [f"event:{event_seq}"])

        self._do(actor, action, f"dataset:{dataset}", {"reason": reason, "event_seq": event_seq}, do)

    # Pausing.

    def pause_flow(self, actor: Actor, flow: str, *, reason: str | None = None) -> None:
        self._do(
            actor,
            "pause_flow",
            f"flow:{flow}",
            {"reason": reason},
            lambda: self._set_paused(flow, actor, reason, True),
        )

    def resume_flow(self, actor: Actor, flow: str, *, reason: str | None = None) -> None:
        self._do(
            actor,
            "resume_flow",
            f"flow:{flow}",
            {"reason": reason},
            lambda: self._set_paused(flow, actor, reason, False),
        )

    def _set_paused(self, flow: str, actor: Actor, reason: str | None, paused: bool) -> None:
        """Pause or resume a flow of the runtime, or of an installed package, whose host reads the same pause."""
        if flow in {f.name for f in self._rt.flows()}:
            if paused:
                self._rt.pause_flow(flow, actor=actor.name, reason=reason)
            else:
                self._rt.resume_flow(flow, actor=actor.name, reason=reason)
            return
        self._pause_target(flow)
        if paused:
            self._store.overrides.set(f"flow:{flow}", "paused", True, actor=actor.name, reason=reason)
        else:
            self._store.overrides.clear(f"flow:{flow}", "paused", actor=actor.name, reason=reason)

    def pause_route(self, actor: Actor, route: str, *, reason: str | None = None) -> None:
        self._do(
            actor,
            "pause_route",
            f"route:{route}",
            {"reason": reason},
            lambda: self._rt.pause_route(route, actor=actor.name, reason=reason),
        )

    def resume_route(self, actor: Actor, route: str, *, reason: str | None = None) -> None:
        self._do(
            actor,
            "resume_route",
            f"route:{route}",
            {"reason": reason},
            lambda: self._rt.resume_route(route, actor=actor.name, reason=reason),
        )

    # Connections.

    def check_connection(self, actor: Actor, name: str) -> CheckResult:
        """Test the connection with its connector's own check. A simulated connection has none: it reports ok and says
        that nothing was checked. Raises `KeyError` for a connection the project doesn't define."""
        if name not in self._state.project.connections:
            raise KeyError(f"no connection named {name!r}")
        result = self._do(
            actor,
            "check_connection",
            f"connection:{name}",
            {},
            lambda: self._check_in_time(name),
            lambda found: "ok" if found.ok else f"failed: {found.message}",
        )
        self._store.overrides.set(
            f"connection:{name}",
            "last_check",
            {"at": self._state.clock.now().isoformat(), "ok": result.ok, "message": result.message},
            actor=actor.name,
        )
        return result

    def _check_in_time(self, name: str) -> CheckResult:
        """Run the check, and give up on it after `CHECK_TIMEOUT_S`. A connector that hangs is left running in its own
        thread: it can't be stopped, but it no longer holds up the request."""
        found: list[CheckResult] = []
        worker = threading.Thread(target=lambda: found.append(self._check(name)), daemon=True)
        worker.start()
        worker.join(CHECK_TIMEOUT_S)
        if not found:
            return CheckResult(False, f"The system did not answer within {CHECK_TIMEOUT_S:g} seconds.")
        return found[0]

    def _check(self, name: str) -> CheckResult:
        connectors = [d.source for f in self._rt.flows() for d in f.datasets if d.source.connection == name]
        connectors += [r.sink for f in self._rt.flows() for r in f.routes if r.sink.connection == name]
        if not connectors:
            return CheckResult(True, "no flow uses this connection, so nothing was checked")
        connector = connectors[0]
        if connector.spec.name.startswith(_SIMULATED):
            return CheckResult(True, "this is a simulated connection, so nothing was checked")
        connection = self._state.project.connections[name]
        try:
            config = connection.resolve()
            register_secrets(*(config[key] for key in connection.secret_refs))
            budget = None
            if (
                connection.budget is not None
            ):  # an operator is waiting, so the check takes from the high reserve
                budget = BudgetHandle(self._store.budgets, connection.budget, "high")
            ctx = Context(
                config,
                logging.getLogger(name),
                self._state.clock.now(),
                str(self._state.project.settings["timezone"]),
                budget=budget,
            )
            check = getattr(connector, "check", None)
            if check is None:  # a push source has nothing to call
                return CheckResult(True, "this connector has no check, so nothing was checked")
            found = check(ctx)
        except ProjectError as error:
            return CheckResult(False, redact(str(error)))
        except Exception as error:  # noqa: BLE001 - whatever a connector raises is what the check found
            return CheckResult(False, redact(f"{type(error).__name__}: {error}"))
        if not isinstance(found, CheckResult):  # pyright: ignore[reportUnnecessaryIsInstance]
            return CheckResult(True, "this connector has no check, so nothing was checked")
        return CheckResult(found.ok, redact(found.message))

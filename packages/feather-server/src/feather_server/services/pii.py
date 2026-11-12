"""PII masking (M2 plan 2 O4): the fields a dataset names in `pii` are shown masked wherever its records are shown,
in a task's records, a version's, a trace and a diff, on the dashboard, the API and the CLI alike. A PII reader (the
`pii-reader` grant, or an admin) may ask to see them as they are; each time is audited, and a refusal too.

Keys are not masked: they identify a record in every page and link, so a key should not be PII."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from feather_server.auth import Actor
from feather_server.catalog_view import dataset_of
from feather_server.services.access import can, require

if TYPE_CHECKING:
    from feather_server.app import AppState
    from feather_server.services.task_records import TaskRecords
    from feather_server.services.trace import Trace

MASK = "•••"


@dataclass(frozen=True)
class Masking:
    """What a page or an answer says of PII: the dataset's PII fields, whether they are masked here, and whether the
    actor may ask to see them."""

    fields: tuple[str, ...]
    masked: bool
    may_unmask: bool

    @property
    def hidden(self) -> frozenset[str]:
        return frozenset(self.fields) if self.masked else frozenset()


def pii_fields(state: "AppState", dataset: str) -> tuple[str, ...]:
    """The PII fields of the dataset, from the runtime here or the catalogue; none for a dataset neither knows."""
    try:
        return tuple(state.runtime.dataset(dataset).pii)
    except KeyError:
        pass
    try:
        return dataset_of(state, dataset).pii
    except KeyError:
        return ()


def masking(
    state: "AppState", actor: Actor, dataset: str, *, unmask: bool = False, target: str | None = None
) -> Masking:
    """How the dataset's records are shown to the actor. `unmask` asks to see the PII: it needs `pii.unmask`, raises
    `Forbidden` otherwise, and is audited either way."""
    fields = pii_fields(state, dataset)
    may = can(actor, "pii.unmask")
    if not fields or not unmask:
        return Masking(fields, bool(fields), may)
    where = target or f"dataset:{dataset}"
    require(actor, "pii.unmask", state.store.audit, where)
    state.store.audit.record(actor.kind, actor.name, "pii.unmask", where, {"fields": list(fields)}, "ok")
    return Masking(fields, False, True)


def mask(payload: Mapping[str, Any] | None, hidden: frozenset[str]) -> Mapping[str, Any] | None:
    """The payload with its hidden fields masked; a null stays null, as it gives nothing away."""
    if payload is None or not hidden:
        return payload
    return {k: (MASK if k in hidden and v is not None else v) for k, v in payload.items()}


def mask_rows(rows: Sequence[Mapping[str, Any]], hidden: frozenset[str]) -> list[Mapping[str, Any]]:
    return [mask(r, hidden) or {} for r in rows] if hidden else list(rows)


def mask_task_records(found: "TaskRecords", hidden: frozenset[str]) -> "TaskRecords":
    if not hidden:
        return found
    return replace(
        found,
        records=tuple(replace(r, payload=mask(r.payload, hidden)) for r in found.records),
        quarantine=tuple(replace(q, payload=mask(q.payload, hidden)) for q in found.quarantine),
    )


def mask_trace(trace: "Trace", hidden: frozenset[str]) -> "Trace":
    if not hidden:
        return trace

    def hide(field: str, value: Any) -> Any:
        return MASK if field in hidden and value is not None else value

    def version(v: Any) -> Any:
        return replace(
            v,
            record=mask(v.record, hidden),
            changes=tuple(
                replace(c, before=hide(c.field, c.before), after=hide(c.field, c.after)) for c in v.changes
            ),
            deliveries=tuple(
                replace(
                    d, quarantine=tuple(replace(q, payload=mask(q.payload, hidden)) for q in d.quarantine)
                )
                for d in v.deliveries
            ),
        )

    return replace(
        trace,
        versions=tuple(version(v) for v in trace.versions),
        extraction=tuple(replace(q, payload=mask(q.payload, hidden)) for q in trace.extraction),
    )


def as_json(m: Masking) -> dict[str, Any]:
    return {"masked": m.masked, "pii": list(m.fields)}

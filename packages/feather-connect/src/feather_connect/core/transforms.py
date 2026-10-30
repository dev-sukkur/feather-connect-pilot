"""Transforms: what a route does to a record on its way to a sink."""

from collections.abc import Callable, Mapping
from typing import Any, cast

from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError
from feather_connect.core.flow import RecordMap, RouteSpec
from feather_connect.core.records import Record

_BATCH_MAP = "_fc_batch_map"


def batch_map[F: Callable[..., Any]](fn: F) -> F:
    """Mark `fn` as a batch map: it takes a whole batch as an Arrow table, with the context, and returns one.

    That is much faster than a call for each record. The function is returned as it is, only marked.
    """
    setattr(fn, _BATCH_MAP, True)
    return fn


def is_batch_map(fn: object) -> bool:
    """Return whether `fn` was marked by `batch_map`."""
    return getattr(fn, _BATCH_MAP, False) is True


def _payload(route: RouteSpec, item: object) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise PermanentError(
            f"the map of route {route.name!r} produced a {type(item).__name__}; "
            "a map must give a dict, a list of dicts or None"
        )
    return dict(cast(Mapping[str, Any], item))


def apply_map(route: RouteSpec, record: Record, ctx: Context) -> list[dict[str, Any]]:
    """Return the payloads that `record` becomes on `route`, one for each record its sink gets.

    A route with no map passes the payload on as it is. Otherwise the map gives a dict for one payload, a
    list of dicts to split the record into several, or None to filter it out.

    Raises `PermanentError` if the map gives anything else, or is a batch map, which works on whole batches.
    """
    if route.map is None:
        return [dict(record.payload)]
    if is_batch_map(route.map):
        raise PermanentError(
            f"the map of route {route.name!r} is a batch map, which can't be applied to one record"
        )
    result: object = cast(RecordMap, route.map)(record, ctx)
    if result is None:
        return []
    if isinstance(result, list):
        return [_payload(route, item) for item in cast(list[object], result)]
    return [_payload(route, result)]

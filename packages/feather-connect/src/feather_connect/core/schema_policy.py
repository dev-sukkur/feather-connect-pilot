"""Schema policy (design §4.7, H12, M2 plan 2 D1): what a route does when a version's fields differ from the last
version it applied.

A version's schema is its fields and their Arrow types, read from its chunks. Against the last applied version on the
route and partition:

- `"default"`: added fields and widened types (a narrower integer or float to a wider one, an integer to a float, a
  field that was all null to a type) pass; a dropped field, a narrowed type or another type change holds the delivery
  `held`, with a reason that starts `schema_change`;
- `"strict"`: any change holds;
- `"permissive"`: nothing holds;
- a function `(before, after) -> str | None`, both `{field: type}`, holds with the reason it returns.

It is set on the dataset (`fc.Dataset(..., schema_policy=...)`) and a route's (`fc.to(..., schema_policy=...)`)
overrides it. A field that is null in every row of a version tells nothing of its type, so it counts as unchanged.
"""

from collections.abc import Callable, Mapping
from typing import Literal

SCHEMA_CHANGE = "schema_change"
NULL = "null"

Fields = Mapping[str, str]
SchemaPolicy = Literal["default", "strict", "permissive"] | Callable[[Fields, Fields], str | None]

_INTS = ("int8", "int16", "int32", "int64")
_UINTS = ("uint8", "uint16", "uint32", "uint64")
_FLOATS = ("halffloat", "float", "double")


def _widens(before: str, after: str) -> bool:
    """Whether a value of type `before` fits `after` without loss, as Arrow names the types."""
    if before == after or before == NULL:
        return True
    for family in (_INTS, _UINTS, _FLOATS):
        if before in family and after in family:
            return family.index(after) > family.index(before)
    if before in _UINTS and after in _INTS:
        return _INTS.index(after) > _UINTS.index(before)
    return (before in _INTS or before in _UINTS) and after in _FLOATS[1:]


def changes(before: Fields, after: Fields) -> tuple[list[str], list[str], list[str]]:
    """Return the fields added, dropped, and changed in type (`name: before → after`) from `before` to `after`."""
    added = [n for n in after if n not in before]
    dropped = [n for n in before if n not in after]
    changed = [
        f"{n}: {before[n]} → {after[n]}"
        for n in before
        if n in after and after[n] != before[n] and after[n] != NULL and before[n] != NULL
    ]
    return added, dropped, changed


def judge(policy: SchemaPolicy, before: Fields, after: Fields) -> str | None:
    """Return why the change from `before` to `after` holds the delivery, starting `schema_change`, or None."""
    if policy == "permissive":
        return None
    if callable(policy):
        why = policy(dict(before), dict(after))
        return None if why is None else f"{SCHEMA_CHANGE}: {why}"
    added, dropped, changed = changes(before, after)
    if policy == "default":
        added = []
        changed = [c for c in changed if not _widens(before[c.split(":", 1)[0]], after[c.split(":", 1)[0]])]
    parts = [
        *([f"added {', '.join(added)}"] if added else []),
        *([f"dropped {', '.join(dropped)}"] if dropped else []),
        *([f"changed {'; '.join(changed)}"] if changed else []),
    ]
    return f"{SCHEMA_CHANGE}: {'; '.join(parts)}" if parts else None

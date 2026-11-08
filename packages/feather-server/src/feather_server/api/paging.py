"""Cursor paging for the JSON API. A cursor is opaque to callers: it wraps the store's own cursor, so a changed store
cursor never looks like a valid page."""

import base64
import binascii
from collections.abc import Sequence
from typing import Any

from fastapi.encoders import jsonable_encoder

from feather_server.api.errors import ApiProblem

_PREFIX = "c1:"
MAX_LIMIT = 500


def encode(raw: str | None) -> str | None:
    if raw is None:
        return None
    return base64.urlsafe_b64encode((_PREFIX + raw).encode()).decode().rstrip("=")


def decode(cursor: str | None) -> str | None:
    """The store cursor inside `cursor`. Raises a 422 problem for one this server didn't hand out."""
    if cursor is None or cursor == "":
        return None
    try:
        text = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    except (binascii.Error, UnicodeDecodeError, ValueError):
        text = ""
    if not text.startswith(_PREFIX):
        raise ApiProblem(422, "Unprocessable Content", "this cursor is not one this server handed out")
    return text.removeprefix(_PREFIX)


def check_limit(limit: int) -> int:
    if not 1 <= limit <= MAX_LIMIT:
        raise ApiProblem(422, "Unprocessable Content", f"limit is {limit}: it must be from 1 to {MAX_LIMIT}")
    return limit


def page(items: Sequence[Any], next_raw: str | None) -> dict[str, Any]:
    """A page of `items`, with the cursor of the next one, or None on the last page."""
    return {"items": jsonable_encoder(list(items)), "next_cursor": encode(next_raw)}

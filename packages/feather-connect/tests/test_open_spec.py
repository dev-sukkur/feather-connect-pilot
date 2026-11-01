"""The reference implementation against the open specification: its vectors, schemas and examples in `spec/`."""

import base64
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, get_args

import jsonschema
import pyarrow as pa
import pytest

from feather_connect import partition as partitions
from feather_connect.clock import FrozenClock
from feather_connect.core.connector import RouteMode
from feather_connect.core.errors import MissingKeyError, PartitionError
from feather_connect.core.partitions import PartitionScheme
from feather_connect.core.records import canonical_json, make_key, row_hash
from feather_connect.core.strategies import Strategy, by_stamp, parse_duration, push, replace, snapshot_diff
from feather_connect.engine.port import RetryPolicy
from feather_connect.ids import stable_id
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import TRANSITIONS, BudgetSpec, DeliveryStatus, parse_limits

SPEC = Path(__file__).parents[3] / "spec"
VECTORS: dict[str, Any] = json.loads((SPEC / "conformance" / "vectors.json").read_text(encoding="utf-8"))
STRATEGIES: dict[str, Strategy] = {
    "by_stamp": by_stamp("t"),
    "snapshot_diff": snapshot_diff(),
    "replace": replace(),
    "push": push(),
}
EXAMPLES = ["flow", "connector-spec", "version", "delivery", "attempt", "problem"]


def _typed(value: Any) -> Any:
    """Turn the tagged inputs of the vectors into the values they stand for."""
    if isinstance(value, list):
        return [_typed(item) for item in value]
    if not isinstance(value, dict):
        return value
    if len(value) == 1:
        tag, text = next(iter(value.items()))
        match tag:
            case "$date":
                return date.fromisoformat(text)
            case "$datetime":
                return datetime.fromisoformat(text)
            case "$decimal":
                return Decimal(text)
            case "$bytes":
                return base64.b64decode(text)
            case "$float":
                return float(text)
            case _:
                pass
    return {name: _typed(item) for name, item in value.items()}


def _scheme(spec: Any) -> Any:
    if isinstance(spec, str):
        return spec
    match spec["kind"]:
        case "none":
            return partitions.none()
        case "by_day":
            return partitions.by_day(spec["field"], spec["tz"])
        case "by_month":
            return partitions.by_month(spec["field"], spec["tz"])
        case "by_bucket":
            return partitions.by_bucket(spec["field"], spec["n"])
        case _:
            return partitions.by_fields(*[_scheme(part) for part in spec["parts"]])


def _snapshot(rows: list[list[Any]]) -> pa.Table:
    keys: Any = pa.array([row[0] for row in rows], pa.string())  # pyright: ignore[reportUnknownMemberType]
    hashes: Any = pa.array([row[1] for row in rows], pa.uint64())  # pyright: ignore[reportUnknownMemberType]
    return pa.table({"_fc_key": keys, "_fc_row_hash": hashes})  # pyright: ignore[reportUnknownMemberType]


@pytest.mark.parametrize("case", VECTORS["canonical_json"])
def test_canonical_json(case: dict[str, Any]) -> None:
    assert canonical_json(_typed(case["input"])).decode("utf-8") == case["expected"]


@pytest.mark.parametrize("case", VECTORS["row_hash"])
def test_row_hash(case: dict[str, Any]) -> None:
    assert str(row_hash(_typed(case["payload"]))) == case["expected"]


@pytest.mark.parametrize("case", VECTORS["record_key"])
def test_record_key(case: dict[str, Any]) -> None:
    key = tuple(case["key"]) if isinstance(case["key"], list) else case["key"]
    if case["expected"] is None:
        with pytest.raises(MissingKeyError):
            make_key(case["payload"], key)
    else:
        assert make_key(case["payload"], key) == case["expected"]


@pytest.mark.parametrize("case", VECTORS["stable_id"])
def test_stable_id(case: dict[str, Any]) -> None:
    assert stable_id(*case["parts"]) == case["expected"]


@pytest.mark.parametrize("case", VECTORS["content_hash"])
def test_content_hash_formula(case: dict[str, Any]) -> None:
    items = sorted([key, op, int(hash_)] for key, op, hash_ in case["items"])
    assert hashlib.blake2b(canonical_json(items), digest_size=16).hexdigest() == case["expected"]


@pytest.mark.parametrize("case", VECTORS["partition_key"])
def test_partition_key(case: dict[str, Any]) -> None:
    scheme: PartitionScheme = _scheme(case["scheme"])
    if case["key"] is None:
        with pytest.raises(PartitionError):
            scheme.key_of(_typed(case["payload"]))
    else:
        assert scheme.key_of(_typed(case["payload"])) == case["key"]
        assert scheme.label_of(case["key"]) == case["label"]


@pytest.mark.parametrize("case", VECTORS["partition_window"])
def test_partition_window(case: dict[str, Any]) -> None:
    scheme: PartitionScheme = _scheme(case["scheme"])
    assert scheme.recent(datetime.fromisoformat(case["now"])) == case["expected"]


@pytest.mark.parametrize("case", VECTORS["duration"])
def test_duration(case: dict[str, Any]) -> None:
    if case["seconds"] is None:
        with pytest.raises(ValueError):
            parse_duration(case["text"])
    else:
        assert parse_duration(case["text"]) == timedelta(seconds=case["seconds"])


@pytest.mark.parametrize("case", VECTORS["limit"])
def test_limit(case: dict[str, Any]) -> None:
    if case["units"] is None:
        with pytest.raises(ValueError):
            parse_limits([case["text"]])
    else:
        window = parse_limits([case["text"]])[0]
        assert (window.units, window.seconds) == (case["units"], case["seconds"])


@pytest.mark.parametrize("case", VECTORS["strategy_diff"])
def test_strategy_diff(case: dict[str, Any]) -> None:
    strategy = STRATEGIES[case["strategy"]]
    old = None if case["old"] is None else _snapshot(case["old"])
    diff = strategy.diff(_snapshot(case["new"]), old)
    assert strategy.complete == case["complete"]
    assert sorted(diff.upsert_keys.to_pylist()) == case["upserts"]  # pyright: ignore[reportArgumentType]
    assert sorted(diff.delete_keys.to_pylist()) == case["deletes"]  # pyright: ignore[reportArgumentType]


def test_delivery_transitions() -> None:
    expected: dict[DeliveryStatus, list[DeliveryStatus]] = VECTORS["delivery_transitions"]
    assert {status: sorted(targets) for status, targets in TRANSITIONS.items()} == expected


@pytest.mark.parametrize("case", VECTORS["retry_delay"])
def test_retry_delay(case: dict[str, Any]) -> None:
    delay = RetryPolicy().delay(case["failed_attempt"], rand=lambda: case["rand"])
    assert delay.total_seconds() == case["seconds"]


@pytest.mark.parametrize("case", VECTORS["budget"])
def test_budget(case: dict[str, Any]) -> None:
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=UTC))
    budgets = MemoryStore(clock).budgets
    budgets.define(BudgetSpec("b", parse_limits(case["limits"]), case["reserve"]))
    for step in case["steps"]:
        clock.advance(timedelta(seconds=step["advance_s"]))
        if step["error"]:
            with pytest.raises(ValueError):
                budgets.acquire("b", step["units"], step["priority"])
        else:
            grant = budgets.acquire("b", step["units"], step["priority"])
            assert grant.granted == step["granted"]
            assert grant.wait // timedelta(microseconds=1) == step["wait_us"]


@pytest.mark.parametrize("name", EXAMPLES)
def test_examples_fit_their_schemas(name: str) -> None:
    schema = json.loads((SPEC / "schemas" / f"{name}.schema.json").read_text(encoding="utf-8"))
    example = json.loads((SPEC / "examples" / f"{name}.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(example, schema, cls=jsonschema.Draft202012Validator)


def test_schemas_name_what_the_code_names() -> None:
    delivery = json.loads((SPEC / "schemas" / "delivery.schema.json").read_text(encoding="utf-8"))
    flow = json.loads((SPEC / "schemas" / "flow.schema.json").read_text(encoding="utf-8"))
    assert set(delivery["properties"]["status"]["enum"]) == set(TRANSITIONS)
    assert set(flow["$defs"]["route"]["properties"]["mode"]["enum"]) == set(get_args(RouteMode))

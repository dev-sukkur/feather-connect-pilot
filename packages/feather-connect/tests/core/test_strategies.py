from datetime import timedelta

import pyarrow as pa
import pytest

import feather_connect as fc


def snap(pairs):
    return pa.table(
        {
            "_fc_key": pa.array([key for key, _ in pairs], pa.string()),
            "_fc_row_hash": pa.array([row_hash for _, row_hash in pairs], pa.uint64()),
        }
    )


def keys(array):
    # The keys of a diff are in no particular order.
    return sorted(array.to_pylist())


def test_snapshot_diff_finds_changes_and_deletes():
    new = snap([("a", 1), ("b", 9), ("d", 4)])
    old = snap([("a", 1), ("b", 2), ("c", 3)])
    diff = fc.strategy.snapshot_diff().diff(new, old)
    assert set(diff.upsert_keys.to_pylist()) == {"b", "d"} and diff.delete_keys.to_pylist() == ["c"]


def test_replace_upserts_everything_and_implies_deletes():
    diff = fc.strategy.replace().diff(snap([("a", 1)]), snap([("a", 1), ("c", 3)]))
    assert diff.upsert_keys.to_pylist() == ["a"] and diff.delete_keys.to_pylist() == ["c"]


def test_by_stamp_suppresses_unchanged_and_never_implies_deletes():
    s = fc.strategy.by_stamp("u", lookback="2h")
    assert s.lookback == timedelta(hours=2) and s.complete is False
    diff = s.diff(snap([("a", 1), ("b", 2)]), snap([("a", 1), ("c", 3)]))
    assert diff.upsert_keys.to_pylist() == ["b"] and len(diff.delete_keys) == 0


# Beyond the brief: what each strategy declares

STRATEGIES = [
    fc.strategy.by_stamp("u"),
    fc.strategy.snapshot_diff(),
    fc.strategy.replace(),
    fc.strategy.push(),
]


def test_strategies_declare_their_name_and_whether_their_versions_are_complete():
    assert {s.name: s.complete for s in STRATEGIES} == {
        "by_stamp": False,
        "snapshot_diff": True,
        "replace": True,
        "push": False,
    }


def test_by_stamp_remembers_its_field_lookback_and_deleted_field():
    s = fc.strategy.by_stamp("u", deleted_field="gone")
    assert (s.field, s.lookback, s.deleted_field) == ("u", timedelta(0), "gone")
    assert fc.strategy.by_stamp("u").deleted_field is None


# Beyond the brief: what a diff holds


@pytest.mark.parametrize("strategy", STRATEGIES, ids=lambda s: s.name)
def test_with_nothing_known_before_every_key_is_an_upsert_and_none_is_a_delete(strategy):
    diff = strategy.diff(snap([("b", 2), ("a", 1)]), None)
    assert keys(diff.upsert_keys) == ["a", "b"] and len(diff.delete_keys) == 0


def test_push_suppresses_unchanged_events_and_never_implies_deletes():
    diff = fc.strategy.push().diff(snap([("a", 1), ("b", 2)]), snap([("a", 1), ("c", 3)]))
    assert diff.upsert_keys.to_pylist() == ["b"] and len(diff.delete_keys) == 0


def test_snapshot_diff_of_an_unchanged_read_is_empty():
    same = snap([("a", 1), ("b", 2)])
    diff = fc.strategy.snapshot_diff().diff(same, same)
    assert len(diff.upsert_keys) == 0 and len(diff.delete_keys) == 0


def test_a_complete_read_that_returns_nothing_deletes_everything_known():
    old = snap([("a", 1), ("b", 2)])
    assert keys(fc.strategy.snapshot_diff().diff(snap([]), old).delete_keys) == ["a", "b"]
    assert keys(fc.strategy.replace().diff(snap([]), old).delete_keys) == ["a", "b"]
    assert len(fc.strategy.by_stamp("u").diff(snap([]), old).delete_keys) == 0


def test_hashes_above_the_signed_range_compare_correctly():
    # Half of all row hashes are 2**63 or more, which a signed column could not hold.
    top = 2**64 - 1
    new = snap([("a", top), ("b", 2**63), ("c", top)])
    old = snap([("a", top), ("b", 2**63 + 1), ("c", top - 1)])
    assert keys(fc.strategy.snapshot_diff().diff(new, old).upsert_keys) == ["b", "c"]


def test_snapshots_held_in_several_chunks_are_diffed_as_a_whole():
    new = pa.concat_tables([snap([("m", 1), ("c", 1)]), snap([("z", 1), ("a", 1)])])
    old = pa.concat_tables([snap([("q", 1), ("c", 1)]), snap([("a", 2)]), snap([("x", 1)])])
    assert new["_fc_key"].num_chunks == 2 and old["_fc_key"].num_chunks == 3
    diff = fc.strategy.snapshot_diff().diff(new, old)
    assert keys(diff.upsert_keys) == ["a", "m", "z"] and keys(diff.delete_keys) == ["q", "x"]


def test_diff_keys_are_string_arrays_even_when_there_are_none():
    diff = fc.strategy.by_stamp("u").diff(snap([("a", 1)]), snap([("a", 1)]))
    assert diff.upsert_keys.type == pa.string() and diff.delete_keys.type == pa.string()
    assert len(diff.upsert_keys) == 0 and len(diff.delete_keys) == 0


# Beyond the brief: durations


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0s", timedelta(0)),
        ("90s", timedelta(seconds=90)),
        ("15m", timedelta(minutes=15)),
        ("2h", timedelta(hours=2)),
        ("7d", timedelta(days=7)),
        ("1h30m", timedelta(hours=1, minutes=30)),
    ],
)
def test_parse_duration_reads_whole_numbers_with_units(text, expected):
    assert fc.strategy.parse_duration(text) == expected


@pytest.mark.parametrize(
    "text", ["", "2", "h", "-2h", "2 h", "1.5h", "2H", "2x", "h2", "1h 30m", "99999999999d"]
)
def test_parse_duration_rejects_anything_else(text):
    with pytest.raises(ValueError, match="duration"):
        fc.strategy.parse_duration(text)

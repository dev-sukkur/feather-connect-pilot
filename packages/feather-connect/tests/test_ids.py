import itertools
import re
import time

from feather_connect import ids
from feather_connect.ids import new_id, stable_id

ULID = re.compile(r"[0-7][0-9A-HJKMNP-TV-Z]{25}")  # 128 bits as 26 characters of Crockford base32


def test_new_id_is_a_ulid():
    assert ULID.fullmatch(new_id())


def test_new_ids_are_unique_and_sort_in_creation_order():
    made = [new_id() for _ in range(5000)]
    assert all(earlier < later for earlier, later in itertools.pairwise(made))


def test_new_id_starts_with_the_time_in_milliseconds():
    before = time.time_ns() // 1_000_000
    made = new_id()
    after = time.time_ns() // 1_000_000
    stamp = 0
    for char in made[:10]:
        stamp = stamp * 32 + ids._ALPHABET.index(char)
    assert before <= stamp <= after


def test_encoding_matches_the_ulid_reference_timestamp():
    # The ULID reference README gives encodeTime(1469918176385) as "01ARYZ6S41".
    assert ids._encode(1469918176385 << 80) == "01ARYZ6S41" + "0" * 16
    assert ids._encode(0) == "0" * 26
    assert ids._encode(2**128 - 1) == "7" + "Z" * 25


def test_new_ids_keep_increasing_when_the_clock_stalls_or_steps_back(monkeypatch):
    now = time.time_ns()
    hour = 3_600_000_000_000
    readings = iter([now, now, now - hour, now - hour, now])
    monkeypatch.setattr(ids, "time_ns", lambda: next(readings))
    monkeypatch.setattr(ids, "randbits", lambda _bits: 0)  # the worst case: randomness never helps
    made = [new_id() for _ in range(5)]
    assert made == sorted(set(made))


def test_stable_id_is_26_base32_characters():
    assert re.fullmatch(r"[A-Z2-7]{26}", stable_id("run", "ds", "p"))


def test_stable_id_depends_on_the_order_and_boundaries_of_the_parts():
    assert stable_id("a", "b") != stable_id("b", "a")
    assert stable_id("ab", "c") != stable_id("a", "bc")
    assert stable_id("a") != stable_id("a", "")


def test_stable_id_never_changes_between_releases():
    # Version ids built from it are stored, so these values must stay the same for good.
    assert stable_id("run", "ds", "p") == "XX72GHJJF7APKVCULX5OG2MSPN"
    assert stable_id("run", "dé", "日本") == "NKCDFEBUY3IOJTYC4HL24SEDI7"

"""The conformance kit: what every connector must do, as test classes that a connector's tests subclass.

A source's subclass gives the fixtures `source` and `seed(rows)`, which makes `rows` all that the source holds. A
sink's gives `sink` and `read_back()`, which returns what the destination holds by the `id` field. Both may give
`ctx` (a plain context by default), `transient_fault()` and `permanent_fault()`, which make the next call fail in that
way; the fault tests are skipped without them. A sink that keeps what it applied in the destination itself, so that a
new instance answers a repeat as the first did (S21), sets `restart_safe = True` and gives `fresh_sink()`.

    class TestMySink(SinkConformance):
        @pytest.fixture
        def sink(self): ...
        @pytest.fixture
        def read_back(self, sink): ...

The kit's rows have a text `id` and a few plain fields. A connector marks itself `beta` once its subclass passes.
"""

import gc
import json
import logging
import socket
import tracemalloc
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, ClassVar, get_args

import pyarrow as pa
import pytest
from pydantic import BaseModel

from feather_connect.clock import FrozenClock
from feather_connect.core.connector import (
    ApplyBatch,
    ApplyRecord,
    Batch,
    ConnectorSpec,
    Deleted,
    InboundRequest,
    Maturity,
    PushSource,
    ReadRequest,
    RouteMode,
    Sink,
    Source,
    Unreadable,
    event_id_of,
    is_unsigned,
    signature_secret_of,
)
from feather_connect.core.context import Context
from feather_connect.core.errors import PermanentError, TransientError
from feather_connect.core.partitions import none
from feather_connect.core.webhooks import SignatureInvalid
from feather_connect.runtime.budgets import BudgetHandle
from feather_connect.storage.memory import MemoryStore
from feather_connect.storage.port import BudgetSpec, parse_limits

__all__ = ["LARGE_READ_ROWS", "PushSourceConformance", "SinkConformance", "SourceConformance", "kit_rows"]

NOW = datetime(2026, 11, 7, 9, tzinfo=UTC)
LARGE_READ_ROWS = 200_000
_PEAK_BYTES = 64 * 1024 * 1024  # what a large read may grow memory by at its peak
_BUDGET = "conformance"


def kit_rows(count: int, start: int = 0) -> list[dict[str, Any]]:
    """Rows the kit seeds and applies: a text `id`, a number, a text and a stamp that grows with the id."""
    return [
        {
            "id": f"k{n}",
            "n": n,
            "name": f"row {n}",
            "updated": f"2026-11-07T09:{n // 60 % 60:02d}:{n % 60:02d}",
        }
        for n in range(start, start + count)
    ]


def _rows(batch: Batch) -> list[dict[str, Any]]:
    if isinstance(batch, pa.RecordBatch):
        return batch.to_pylist()
    return [row for row in batch if not isinstance(row, Unreadable)]


def _budgeted(ctx: Context) -> tuple[Context, MemoryStore]:
    store = MemoryStore(FrozenClock(NOW))
    store.budgets.define(BudgetSpec(_BUDGET, parse_limits(["100000/minute"])))
    return replace(ctx, budget=BudgetHandle(store.budgets, _BUDGET, sleep=lambda s: None)), store


def _used(store: MemoryStore) -> float:
    return sum(usage.used for usage in store.budgets.usage(_BUDGET))


def _fixture(request: pytest.FixtureRequest, name: str) -> Any:
    try:
        return request.getfixturevalue(name)
    except pytest.FixtureLookupError:
        pytest.skip(f"no {name} fixture: this connector's tests don't arm that fault")


def _check_spec(spec: ConnectorSpec) -> None:
    assert "." in spec.name, f"a connector name is `<package>.<kind>`, not {spec.name!r}"
    assert spec.config is None or issubclass(spec.config, BaseModel)
    assert spec.maturity in get_args(Maturity)
    assert set(spec.modes) <= set(get_args(RouteMode))
    assert all(isinstance(units, int) and units >= 0 for units in spec.cost.values())


class _Common:
    @pytest.fixture
    def ctx(self) -> Context:
        return Context({}, logging.getLogger("conformance"), NOW, "UTC")


class SourceConformance(_Common):
    """What every pull source must do."""

    def read_all(self, source: Source, ctx: Context, req: ReadRequest | None = None) -> list[dict[str, Any]]:
        return [row for batch in source.read(ctx, req or ReadRequest("*", none())) for row in _rows(batch)]

    def test_spec_is_valid(self, source: Source):
        _check_spec(source.spec)
        assert source.spec.strategies, "a source serves at least one strategy"

    def test_check_reports_ok(
        self, source: Source, ctx: Context, seed: Callable[[list[dict[str, Any]]], None]
    ):
        seed(kit_rows(3))
        result = source.check(ctx)
        assert result.ok, result.message

    def test_discover_lists_a_stream(
        self, source: Source, ctx: Context, seed: Callable[[list[dict[str, Any]]], None]
    ):
        seed(kit_rows(3))
        streams = source.discover(ctx)
        assert streams and all(stream.name for stream in streams)

    def test_read_returns_seeded_rows(
        self, source: Source, ctx: Context, seed: Callable[[list[dict[str, Any]]], None]
    ):
        rows = kit_rows(25)
        seed(rows)
        read = {row["id"]: row for row in self.read_all(source, ctx)}
        assert sorted(read) == sorted(row["id"] for row in rows)
        for row in rows:
            assert {k: read[row["id"]].get(k) for k in row} == row

    def test_resume_continues_from_a_checkpoint(
        self, source: Source, ctx: Context, seed: Callable[[list[dict[str, Any]]], None]
    ):
        if not source.spec.resumable:
            pytest.skip("the spec does not say the source is resumable")
        rows = kit_rows(250)
        seed(rows)
        pages = iter(source.read(ctx, ReadRequest("*", none())))
        nothing: list[dict[str, Any]] = []
        taken = _rows(next(pages)) + _rows(next(pages, nothing))  # a read cut short after two batches
        saved = ctx.checkpoint()
        assert saved is not None, "a resumable source saves a checkpoint as it reads"
        rest = self.read_all(source, replace(ctx), ReadRequest("*", none(), resume=saved))
        assert {row["id"] for row in taken + rest} == {row["id"] for row in rows}
        assert len(rest) < len(rows)

    def test_bounded_memory_on_large_read(
        self,
        source: Source,
        ctx: Context,
        seed: Callable[[list[dict[str, Any]]], None],
        large_read_rows: int,
    ):
        seed(kit_rows(large_read_rows))
        gc.collect()
        tracemalloc.start()
        try:
            count = 0
            for batch in source.read(ctx, ReadRequest("*", none())):
                count += len(_rows(batch))
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert count == large_read_rows
        assert peak < _PEAK_BYTES, f"a read of {count} rows grew memory by {peak / 2**20:.0f} MB at its peak"

    @pytest.fixture
    def large_read_rows(self) -> int:
        return LARGE_READ_ROWS

    def test_retryable_errors_raise_transient(
        self,
        source: Source,
        ctx: Context,
        seed: Callable[[list[dict[str, Any]]], None],
        request: pytest.FixtureRequest,
    ):
        seed(kit_rows(3))
        _fixture(request, "transient_fault")()
        with pytest.raises(TransientError):
            self.read_all(source, ctx)

    def test_permanent_errors_raise_permanent(
        self,
        source: Source,
        ctx: Context,
        seed: Callable[[list[dict[str, Any]]], None],
        request: pytest.FixtureRequest,
    ):
        seed(kit_rows(3))
        _fixture(request, "permanent_fault")()
        with pytest.raises(PermanentError):
            self.read_all(source, ctx)

    def test_calls_are_metered_when_a_budget_is_set(
        self, source: Source, ctx: Context, seed: Callable[[list[dict[str, Any]]], None]
    ):
        if not source.spec.cost:
            pytest.skip("the spec names no costs: the source makes no metered calls")
        seed(kit_rows(3))
        budgeted, store = _budgeted(ctx)
        self.read_all(source, budgeted)
        assert _used(store) > 0


class SinkConformance(_Common):
    """What every sink must do. `deletes_supported` is False for a sink that turns deletes down by design, and
    `default_mode` the mode of the tests that name none, for a sink that does not upsert."""

    restart_safe: ClassVar[bool] = True
    deletes_supported: ClassVar[bool] = True
    default_mode: ClassVar[RouteMode] = "upsert"

    def batch(
        self,
        records: list[ApplyRecord],
        *,
        mode: RouteMode | None = None,
        version: str = "v1",
        partition: str = "*",
        key: str | None = None,
    ) -> ApplyBatch:
        mode = mode or self.default_mode
        idempotency_key = key or f"conformance:{version}:{mode}:{partition}"
        return ApplyBatch("conformance", version, 0, partition, mode, records, idempotency_key, True, True)

    @staticmethod
    def upserts(rows: list[dict[str, Any]]) -> list[ApplyRecord]:
        return [ApplyRecord(str(row["id"]), "upsert", row) for row in rows]

    def test_spec_is_valid(self, sink: Sink):
        _check_spec(sink.spec)
        assert sink.spec.modes, "a sink applies at least one mode"

    def test_check_reports_ok(self, sink: Sink, ctx: Context):
        result = sink.check(ctx)
        assert result.ok, result.message

    def test_apply_twice_changes_nothing(
        self, sink: Sink, ctx: Context, read_back: Callable[[], Mapping[Any, Mapping[str, Any]]]
    ):
        batch = self.batch(self.upserts(kit_rows(5)))
        first = sink.apply(ctx, batch)
        held = dict(read_back())
        again = sink.apply(ctx, batch)
        assert dict(read_back()) == held and len(held) == 5
        assert (again.applied, len(again.rejected)) == (first.applied, len(first.rejected))

    def test_apply_again_after_a_restart_changes_nothing(
        self,
        sink: Sink,
        ctx: Context,
        read_back: Callable[[], Mapping[Any, Mapping[str, Any]]],
        request: pytest.FixtureRequest,
    ):
        if not self.restart_safe:
            pytest.xfail(
                "this sink keeps what it applied in the instance, so a new one can't tell a repeat (S21)"
            )
        mode: RouteMode = "append" if "append" in sink.spec.modes else "upsert"
        batch = self.batch(self.upserts(kit_rows(5)), mode=mode)
        first = sink.apply(ctx, batch)
        held = dict(read_back())
        fresh: Sink = request.getfixturevalue("fresh_sink")()
        again = fresh.apply(ctx, batch)
        assert dict(read_back()) == held
        assert again.applied == first.applied

    def test_delete_removes_the_key(
        self, sink: Sink, ctx: Context, read_back: Callable[[], Mapping[Any, Mapping[str, Any]]]
    ):
        rows = kit_rows(3)
        sink.apply(ctx, self.batch(self.upserts(rows)))
        result = sink.apply(ctx, self.batch([ApplyRecord("k1", "delete", None)], version="v2"))
        if not self.deletes_supported:
            assert [r.key for r in result.rejected] == ["k1"] and "k1" in read_back()
            return
        assert "k1" not in read_back() and {"k0", "k2"} <= set(read_back())

    def test_modes_declared_in_spec_work(
        self, sink: Sink, ctx: Context, read_back: Callable[[], Mapping[Any, Mapping[str, Any]]]
    ):
        wanted: dict[RouteMode, list[dict[str, Any]]] = {
            "upsert": kit_rows(2, 0),
            "append": kit_rows(2, 10),
            "replace_partition": kit_rows(2, 20),
        }
        for mode, rows in wanted.items():
            if mode not in sink.spec.modes:
                continue
            partition = "p=1" if mode == "replace_partition" else "*"
            result = sink.apply(ctx, self.batch(self.upserts(rows), mode=mode, partition=partition))
            assert result.applied == len(rows), f"mode {mode}: {result.rejected}"
            assert {row["id"] for row in rows} <= set(read_back()), f"mode {mode} wrote nothing"

    def test_retryable_errors_raise_transient(self, sink: Sink, ctx: Context, request: pytest.FixtureRequest):
        _fixture(request, "transient_fault")()
        with pytest.raises(TransientError):
            sink.apply(ctx, self.batch(self.upserts(kit_rows(2))))

    def test_permanent_errors_raise_permanent(self, sink: Sink, ctx: Context, request: pytest.FixtureRequest):
        _fixture(request, "permanent_fault")()
        with pytest.raises(PermanentError):
            sink.apply(ctx, self.batch(self.upserts(kit_rows(2))))

    def test_calls_are_metered_when_a_budget_is_set(self, sink: Sink, ctx: Context):
        if not sink.spec.cost:
            pytest.skip("the spec names no costs: the sink makes no metered calls")
        budgeted, store = _budgeted(ctx)
        sink.apply(budgeted, self.batch(self.upserts(kit_rows(2))))
        assert _used(store) > 0


class PushSourceConformance(_Common):
    """What every push source must do (design 13.5, S2, S4).

    A subclass gives `source`, `ctx` (holding the secret under the source's `signature_secret`) and
    `sign_with(secret, body) -> InboundRequest`, a request signed the source's way with `secret`; `secret` is the class
    attribute that `ctx` holds. `body` is one record as JSON, a kit row by default. An unsigned source skips the
    signature tests.
    """

    secret: ClassVar[str] = "conformance-secret"

    @pytest.fixture
    def body(self) -> bytes:
        return json.dumps(kit_rows(1)[0]).encode()

    @staticmethod
    def arrived(ctx: Context, request: InboundRequest) -> Context:
        """The context an ingress verifies with: `now` is when the request arrived."""
        return replace(ctx, now=request.received_at)

    def test_spec_is_valid(self, source: PushSource):
        _check_spec(source.spec)
        assert "push" in source.spec.strategies, "a push source serves the push strategy"

    def test_says_how_it_verifies(self, source: PushSource):
        assert is_unsigned(source) or signature_secret_of(source), (
            "name a signature_secret, or say unsigned=True"
        )

    def test_a_signed_request_passes(
        self, source: PushSource, ctx: Context, sign_with: Callable[[str, bytes], InboundRequest], body: bytes
    ):
        request = sign_with(self.secret, body)
        source.verify(self.arrived(ctx, request), request)

    def test_a_request_signed_with_another_secret_is_refused(
        self, source: PushSource, ctx: Context, sign_with: Callable[[str, bytes], InboundRequest], body: bytes
    ):
        if is_unsigned(source):
            pytest.skip("the source takes requests with no check")
        request = sign_with("another-secret-altogether", body)
        with pytest.raises(SignatureInvalid):
            source.verify(self.arrived(ctx, request), request)

    def test_a_changed_body_is_refused(
        self, source: PushSource, ctx: Context, sign_with: Callable[[str, bytes], InboundRequest], body: bytes
    ):
        if is_unsigned(source):
            pytest.skip("the source takes requests with no check")
        request = replace(sign_with(self.secret, body), body=body.replace(b"row", b"ROW"))
        with pytest.raises(SignatureInvalid):
            source.verify(self.arrived(ctx, request), request)

    def test_a_missing_secret_is_permanent(
        self, source: PushSource, ctx: Context, sign_with: Callable[[str, bytes], InboundRequest], body: bytes
    ):
        if is_unsigned(source):
            pytest.skip("the source takes requests with no check")
        request = sign_with(self.secret, body)
        bare = replace(ctx, config={}, now=request.received_at)
        with pytest.raises(PermanentError):  # the ingress answers 503, and the sender tries again
            source.verify(bare, request)

    def test_receive_only_parses(
        self,
        source: PushSource,
        ctx: Context,
        sign_with: Callable[[str, bytes], InboundRequest],
        body: bytes,
        monkeypatch: pytest.MonkeyPatch,
    ):
        def no_network(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("receive opened a connection")

        monkeypatch.setattr(socket, "socket", no_network)
        monkeypatch.setattr(socket, "create_connection", no_network)
        records = source.receive(ctx, sign_with(self.secret, body))
        assert records and all(isinstance(r, Mapping | Deleted) for r in records)

    def test_event_id_is_the_same_for_the_same_request(
        self, source: PushSource, sign_with: Callable[[str, bytes], InboundRequest], body: bytes
    ):
        request = sign_with(self.secret, body)
        assert event_id_of(source, request) == event_id_of(source, request)

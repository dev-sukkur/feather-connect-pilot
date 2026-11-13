"""SQL transforms and reference lookups (M3.1 plan 1 F1)."""

from typing import Any

import pyarrow as pa
import pytest

import feather_connect as fc
from feather_connect.connectors import memory
from feather_connect.core.errors import PermanentError
from feather_connect.core.sql import references_in, run_query
from feather_connect.runtime.references import CACHE, key_text
from feather_connect.testing import FlowHarness

MATERIALS = [
    {"material": "M1", "description": "Steel bar", "unit": "KG"},
    {"material": "M2", "description": "Copper wire", "unit": "M"},
]
LINES = [
    {"line": 1, "material": "M1", "qty": 3},
    {"line": 2, "material": "M2", "qty": 5},
    {"line": 3, "material": "M9", "qty": 1},
]


@pytest.fixture(autouse=True)
def fresh_cache():
    CACHE.clear()
    yield
    CACHE.clear()


def material_flow() -> fc.Flow:
    materials = fc.Dataset(
        "sap.material",
        source=memory.Source("sap"),
        key="material",
        strategy=fc.strategy.snapshot_diff(),
        keep_state=True,
    )
    return fc.Flow("materials", reads=materials, deliver=[fc.to(memory.Sink("lake"), name="materials-lake")])


def lines_flow(map_: Any) -> fc.Flow:
    lines = fc.Dataset(
        "zakya.line", source=memory.Source("zakya"), key="line", strategy=fc.strategy.snapshot_diff()
    )
    return fc.Flow(
        "lines", reads=lines, deliver=[fc.to(memory.Sink("staging"), name="lines-staging", map=map_)]
    )


def harness(map_: Any) -> FlowHarness:
    h = FlowHarness(lines_flow(map_), also=[material_flow()])
    h.source("sap.material").load(MATERIALS)
    h.run(flow="materials")
    h.source("zakya.line").load(LINES)
    return h


ENRICH = fc.sql(
    """select b.*, m.description, m.unit
       from batch b left join {{ dataset('sap.material') }} m using (material)
       order by b.line"""
)


def test_references_are_found_in_a_query_and_other_templates_are_refused():
    assert references_in(
        "select * from {{ dataset('a') }} join {{dataset(\"b\")}} using (k) join {{ dataset('a') }}"
    ) == ("a", "b")
    with pytest.raises(ValueError, match="not a reference"):
        references_in("select {{ partition }} from batch")


def test_a_query_runs_over_the_batch():
    batch = pa.table({"x": [1, 2, 3]})
    assert run_query("select x * 2 as y from batch order by x", {"batch": batch}, None).column(
        "y"
    ).to_pylist() == [2, 4, 6]


def test_a_query_cannot_read_files_or_urls(tmp_path):
    secret = tmp_path / "secret.csv"
    secret.write_text("a\n1\n")
    with pytest.raises(PermanentError, match="SQL failed"):
        run_query(f"select * from read_csv('{secret.as_posix()}')", {"batch": pa.table({"x": [1]})}, None)


def test_a_query_with_a_reference_and_nothing_to_read_it_fails():
    with pytest.raises(PermanentError, match="nothing here can read other datasets"):
        run_query("select * from {{ dataset('a') }}", {}, None)


def test_a_sql_transform_joins_a_reference_and_records_its_stamp():
    h = harness(ENRICH)
    h.run()
    rows = sorted(h.sink("lines-staging").rows, key=lambda r: r["line"])
    assert [(r["line"], r["description"], r["unit"]) for r in rows] == [
        (1, "Steel bar", "KG"),
        (2, "Copper wire", "M"),
        (3, None, None),  # a left join keeps a line whose material isn't known yet (H17)
    ]
    [version] = h.versions("zakya.line")
    applied = h.ledger.attempts("lines-staging", version.version_id)[-1]
    assert applied.to_status == "applied" and set(applied.references or {}) == {"sap.material"}


def test_a_sql_transform_that_drops_the_key_fails_the_version():
    h = harness(fc.sql("select material from batch"))
    h.run()
    [version] = h.versions("zakya.line")
    delivery = h.ledger.get("lines-staging", version.version_id)
    assert delivery.status == "failed" and "_fc_key" in (delivery.last_error or "")


def test_a_failing_query_fails_the_version_with_its_error():
    h = harness(fc.sql("select nope from batch"))
    h.run()
    [version] = h.versions("zakya.line")
    delivery = h.ledger.get("lines-staging", version.version_id)
    assert delivery.status == "failed" and "SQL failed" in (delivery.last_error or "")


def test_a_record_map_looks_a_reference_up():
    def describe(record, ctx):
        found = ctx.lookup("sap.material", record["material"])
        return {**record, "description": None if found is None else found["description"]}

    h = harness(describe)
    h.run()
    got = {r["line"]: r["description"] for r in h.sink("lines-staging").rows}
    assert got == {1: "Steel bar", 2: "Copper wire", 3: None}


def test_a_batch_map_reads_a_whole_reference():
    @fc.batch_map
    def count(table, ctx):
        n = ctx.reference("sap.material").num_rows
        return table.append_column("materials", pa.array([n] * table.num_rows))

    h = harness(count)
    h.run()
    assert {r["materials"] for r in h.sink("lines-staging").rows} == {2}


def test_a_new_reference_version_is_read_by_the_next_delivery():
    h = harness(ENRICH)
    h.run()
    h.source("sap.material").load([*MATERIALS, {"material": "M9", "description": "Bolt", "unit": "EA"}])
    h.run(flow="materials")
    h.source("zakya.line").load([*LINES[:2], {"line": 3, "material": "M9", "qty": 2}])
    h.run()
    latest = {r["line"]: r["description"] for r in h.sink("lines-staging").rows}
    assert latest[3] == "Bolt"
    first, second = sorted(h.versions("zakya.line"), key=lambda v: v.seal_seq or 0)
    stamps = [h.ledger.attempts("lines-staging", v.version_id)[-1].references for v in (first, second)]
    assert stamps[0] is not None and stamps[1] is not None
    assert stamps[1]["sap.material"] > stamps[0]["sap.material"]


def test_a_reference_with_no_state_yet_is_retried_not_failed():
    h = FlowHarness(lines_flow(ENRICH), also=[material_flow()])
    h.source("zakya.line").load(LINES)
    h.run()
    [version] = h.versions("zakya.line")
    assert h.ledger.get("lines-staging", version.version_id).status != "applied"
    assert "no sealed state yet" in (h.ledger.get("lines-staging", version.version_id).last_error or "")


def test_lookup_keys_are_named_as_the_store_names_them():
    assert key_text("M1") == "M1" and key_text(7) == "7"
    assert key_text(("s1", 2)) == '["s1",2]'

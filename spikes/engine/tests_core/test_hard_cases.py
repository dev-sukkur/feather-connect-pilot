"""S2: hard cases from design §16 against the core-model prototype."""

import json
from pathlib import Path

import pytest

from coremodel import Dataset, Model, Route
from spike.config import PG_URL

RESULTS = Path(__file__).resolve().parents[1] / "results"

INVOICES = Dataset("zakya.invoice", key=lambda r: r["invoice_id"],
                   partition=lambda r: f"store={r['store']}/date={r['date']}")


def record(case: str, **facts) -> None:
    RESULTS.mkdir(exist_ok=True)
    with open(RESULTS / "core.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps({"case": case, **facts}) + "\n")


@pytest.fixture
def m() -> Model:
    return Model(PG_URL)


def inv(i: str, date: str, total: float = 100.0, store: int = 1509) -> dict:
    return {"invoice_id": i, "store": store, "date": date, "total": total}


def test_h1a_record_moves_partition_incrementally(m):
    wh, sl = Route("wh", INVOICES.name), Route("slice", INVOICES.name, mode="replace_partition")
    m.routes = [wh, sl]
    m.extract(INVOICES, [inv("INV-1", "2026-10-18"), inv("INV-2", "2026-10-18")], run_id="r1")
    m.apply(wh), m.apply(sl)
    m.extract(INVOICES, [inv("INV-1", "2026-10-19")], run_id="r2")  # date edited at the source
    m.apply(wh), m.apply(sl)

    for route in ("wh", "slice"):
        d = m.dest(route)
        assert d["INV-1"]["partition_key"].endswith("2026-10-19")
        assert sum(1 for k in d if k == "INV-1") == 1
    record("H1a", passed=True, note="move arrives as an upsert with moved_from; no stale copy")


def test_h1b_move_between_slices_of_one_complete_run(m):
    wh = Route("wh", INVOICES.name)
    m.routes = [wh]
    p18, p19 = "store=1509/date=2026-10-18", "store=1509/date=2026-10-19"
    m.extract(INVOICES, [inv("INV-1", "2026-10-18"), inv("INV-2", "2026-10-18")],
              run_id="r1", complete_partitions={p18})
    m.apply(wh)

    # Naive: slices sealed one at a time -> INV-2 is deleted, then re-inserted.
    m.extract(INVOICES, [inv("INV-1", "2026-10-18")], run_id="naive", complete_partitions={p18})
    m.extract(INVOICES, [inv("INV-2", "2026-10-19")], run_id="naive", complete_partitions={p19})
    m.apply(wh)
    naive_deletes = m.scalar("select count(*) from core.dest_log where key = 'INV-2' and op = 'delete'")

    # Run-level reconciliation: both slices in one extract -> a move, never a delete.
    m.extract(INVOICES, [inv("INV-1", "2026-10-18"), inv("INV-2", "2026-10-20")],
              run_id="run-level", complete_partitions={p18, p19, "store=1509/date=2026-10-20"})
    m.apply(wh)
    run_level_deletes = m.scalar(
        "select count(*) from core.dest_log l join core.version v on v.id = l.version_id"
        " where l.key = 'INV-2' and l.op = 'delete' and v.run_id = 'run-level'")
    record("H1b", naive_transient_deletes=naive_deletes, run_level_deletes=run_level_deletes)
    assert naive_deletes == 1 and run_level_deletes == 0
    assert m.dest("wh")["INV-2"]["partition_key"].endswith("2026-10-20")


def test_h2_snapshot_diff_detects_inserts_updates_hard_deletes(m):
    mbewh = Dataset("sap.mbewh", key=lambda r: r["matnr"], partition=lambda r: "all")
    wh = Route("wh", mbewh.name)
    m.routes = [wh]
    rows1 = [{"matnr": k, "stock": 10} for k in ("A", "B", "C")]
    m.extract(mbewh, rows1, run_id="r1", complete_partitions={"all"})
    m.apply(wh)
    rows2 = [{"matnr": "A", "stock": 11}, {"matnr": "C", "stock": 10}, {"matnr": "D", "stock": 5}]
    (vid,) = m.extract(mbewh, rows2, run_id="r2", complete_partitions={"all"})
    m.apply(wh)

    v = m.c.execute("select upserts, deletes from core.version where id = %s", (vid,)).fetchone()
    record("H2", upserts=v["upserts"], deletes=v["deletes"])
    assert (v["upserts"], v["deletes"]) == (2, 1)  # A changed, D new; B hard-deleted; C untouched
    assert {k: d["payload"]["stock"] for k, d in m.dest("wh").items()} == {"A": 11, "C": 10, "D": 5}


def test_h3_key_rewritten_in_place(m):
    by_business = Dataset("sap.price", key=lambda r: f"{r['material']}|{r['list']}", partition=lambda r: "all")
    by_source_id = Dataset("sap.price_by_cond", key=lambda r: r["cond_id"], partition=lambda r: "all")
    good, bad = Route("good", by_business.name), Route("bad", by_source_id.name)
    m.routes = [good, bad]
    for ds in (by_business, by_source_id):
        m.extract(ds, [{"cond_id": "0001", "material": "M1", "list": "RET", "amount": 100}], run_id="r1")
        m.extract(ds, [{"cond_id": "0002", "material": "M1", "list": "RET", "amount": 110}], run_id="r2")
    m.apply(good), m.apply(bad)
    record("H3", business_key_rows=len(m.dest("good")), source_id_rows=len(m.dest("bad")))
    assert len(m.dest("good")) == 1 and m.dest("good")["M1|RET"]["payload"]["amount"] == 110
    assert len(m.dest("bad")) == 2  # the bug the business key avoids


def test_h4_lookback_rereads_do_not_churn(m):
    wh = Route("wh", INVOICES.name)
    m.routes = [wh]
    m.extract(INVOICES, [inv("INV-1", "2026-10-18"), inv("INV-2", "2026-10-18")], run_id="r1")
    m.apply(wh)
    before = m.scalar("select count(*) from core.dest_log")
    vids = m.extract(INVOICES, [inv("INV-1", "2026-10-18"), inv("INV-2", "2026-10-18"),
                                inv("INV-3", "2026-10-18")], run_id="r2")  # lookback window re-read
    m.apply(wh)
    sent = m.scalar("select count(*) from core.dest_log") - before
    ups = m.scalar("select coalesce(sum(upserts), 0) from core.version where id = any(%s)", (vids,))
    record("H4", reread_rows=3, version_upserts=ups, records_sent=sent)
    assert ups == 1 and sent == 1


def test_h6_destination_down_does_not_block_extraction(m):
    wh = Route("wh", INVOICES.name, destination_up=False)
    m.routes = [wh]
    for run, total in (("r1", 100), ("r2", 120), ("r3", 130)):
        m.extract(INVOICES, [inv("INV-1", "2026-10-18", total), inv("INV-5", "2026-10-19", total)], run_id=run)
    down = m.apply(wh)
    sealed = m.scalar("select count(*) from core.version")
    wh.destination_up = True
    up = m.apply(wh)
    order = [r["version_id"] for r in m.c.execute(
        "select version_id from core.dest_log where key = 'INV-1' order by ctid")]
    record("H6", versions_sealed_while_down=sealed, failed_while_down=down["failed"],
           applied_after_recovery=up["applied"])
    assert sealed == 6 and down["applied"] == 0 and up["applied"] == 6
    assert order == sorted(order) and m.dest("wh")["INV-1"]["payload"]["total"] == 130


def test_h10_quarantined_record_superseded_by_newer_version(m):
    wh = Route("wh", INVOICES.name, validate=lambda p: "negative total" if p["total"] < 0 else None)
    m.routes = [wh]
    m.extract(INVOICES, [inv("INV-9", "2026-10-18", -5), inv("INV-8", "2026-10-18", 10)], run_id="r1")
    first = m.apply(wh)
    m.extract(INVOICES, [inv("INV-9", "2026-10-18", 50)], run_id="r2")
    m.apply(wh)
    status = m.scalar("select status from core.quarantine where key = 'INV-9'")
    record("H10", quarantined_first=first["quarantined"], quarantine_status_after=status)
    assert first["quarantined"] == 1 and "INV-8" in m.dest("wh")
    assert status == "superseded" and m.dest("wh")["INV-9"]["payload"]["total"] == 50


def test_h13_lines_not_blocked_by_headers_and_readiness_gates(m):
    headers = Dataset("zakya.header", key=lambda r: r["invoice_id"], partition=lambda r: f"date={r['date']}")
    lines = Dataset("zakya.line", key=lambda r: f"{r['invoice_id']}:{r['line']}", partition=lambda r: f"date={r['date']}")
    lines_route = Route("lines_wh", lines.name)
    m.routes = [lines_route]
    m.extract(lines, [{"invoice_id": "INV-1", "line": 1, "date": "2026-10-18"}], run_id="r1",
              complete_partitions={"date=2026-10-18"})
    m.apply(lines_route)
    lines_landed = len(m.dest("lines_wh"))
    ready_before = m.ready([headers.name, lines.name], "2026-10-18")
    m.extract(headers, [{"invoice_id": "INV-1", "date": "2026-10-18"}], run_id="r2",
              complete_partitions={"date=2026-10-18"})
    ready_after = m.ready([headers.name, lines.name], "2026-10-18")
    record("H13", lines_landed_without_headers=lines_landed, ready_before=ready_before, ready_after=ready_after)
    assert lines_landed == 1 and not ready_before and ready_after

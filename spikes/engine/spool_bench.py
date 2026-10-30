"""S4: Postgres spool benchmark (ADR-0005, NFR-02, design D5). Throwaway.

Measures, on the spike Postgres:
1. write throughput of zstd Parquet chunks into a day-partitioned bytea table (1 and 4 writers);
2. read throughput back to Arrow;
3. compressed bytes per record for invoice-line-like rows;
4. snapshot_diff of a 1M-row partition (key hash + row hash anti-join in DuckDB);
5. retention by dropping a day partition;
6. tiny push micro-batches (10 events): Parquet chunk vs JSONB rows insert latency.

Usage:  uv run python spool_bench.py [rows]
"""

from __future__ import annotations

import io
import json
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spike.config import PG_URL

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
CHUNK_ROWS = 50_000
RESULTS = Path(__file__).parent / "results"


def make_rows(n: int, seed: int = 7) -> pa.Table:
    rng = np.random.default_rng(seed)
    invoice = rng.integers(1, n // 4 + 1, n)
    store = rng.integers(1000, 1600, n)
    qty = rng.integers(1, 10, n)
    price = np.round(rng.uniform(10, 5000, n), 2)
    sku = np.char.add("SKU", rng.integers(100000, 999999, n).astype(str))
    desc_pool = np.array([f"Cotton shirt size {s} colour {c}" for s in "SMLX" for c in
                          ("red", "blue", "black", "white", "olive")])
    table = pa.table({
        "invoice_id": pa.array(np.char.add("INV-", invoice.astype(str))),
        "line_no": pa.array(rng.integers(1, 12, n), pa.int16()),
        "store_id": pa.array(store, pa.int32()),
        "business_date": pa.array(np.full(n, np.datetime64("2026-10-18")).astype("datetime64[D]")),
        "sku": pa.array(sku),
        "description": pa.array(desc_pool[rng.integers(0, len(desc_pool), n)]),
        "qty": pa.array(qty, pa.int32()),
        "unit_price": pa.array(price),
        "discount": pa.array(np.round(price * rng.choice([0, 0, 0.05, 0.1], n), 2)),
        "tax": pa.array(np.round(price * qty * 0.12, 2)),
        "cashier": pa.array(np.char.add("C", rng.integers(1, 400, n).astype(str))),
        "payment_mode": pa.array(rng.choice(["cash", "card", "upi", "wallet"], n)),
        "customer_id": pa.array(rng.integers(1, 2_000_000, n), pa.int64()),
        "updated_at": pa.array(np.datetime64("2026-10-18T10:00:00") +
                               rng.integers(0, 36000, n).astype("timedelta64[s]")),
    })
    key = pc.binary_join_element_wise(  # unique per row: invoice + row number
        table["invoice_id"], pa.array(np.arange(n).astype(str)), ":")
    con = duckdb.connect()
    con.register("t", table)
    hashes = con.sql("select hash(t) as h from t").to_arrow_table()["h"]  # whole row as a struct, uint64
    return table.append_column("_fc_key", key) \
                .append_column("_fc_op", pa.array(np.full(n, "upsert"))) \
                .append_column("_fc_row_hash", hashes)


def to_parquet(t: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf, compression="zstd")
    return buf.getvalue()


def setup(c: psycopg.Connection) -> None:
    c.execute("create schema if not exists spool")
    c.execute("drop table if exists spool.spool_chunk cascade")
    c.execute("""create table spool.spool_chunk (
        version_id uuid not null, chunk_no int not null, rows int not null,
        bytes bytea not null, created_on date not null default current_date
    ) partition by range (created_on)""")
    today = date.today()
    for d in (today - timedelta(days=1), today, today + timedelta(days=1)):
        c.execute(f"create table spool.spool_chunk_{d:%Y%m%d} partition of spool.spool_chunk"
                  f" for values from ('{d}') to ('{d + timedelta(days=1)}')")
        c.execute(f"alter table spool.spool_chunk_{d:%Y%m%d} alter column bytes set storage external")
    c.execute("""create table if not exists spool.inline_record (
        version_id uuid not null, seq int not null, key text not null, op text not null,
        payload jsonb not null)""")


def write_version(table: pa.Table, writers: int, created_on: date | None = None) -> tuple[uuid.UUID, dict]:
    version = uuid.uuid4()
    slices = [table.slice(i, CHUNK_ROWS) for i in range(0, table.num_rows, CHUNK_ROWS)]

    def put(item: tuple[int, pa.Table]) -> int:
        n, part = item
        blob = to_parquet(part)
        with psycopg.connect(PG_URL, autocommit=True) as c:
            c.execute("insert into spool.spool_chunk (version_id, chunk_no, rows, bytes, created_on)"
                      " values (%s, %s, %s, %s, coalesce(%s, current_date))",
                      (version, n, part.num_rows, blob, created_on))
        return len(blob)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(writers) as pool:
        sizes = list(pool.map(put, enumerate(slices)))
    elapsed = time.perf_counter() - t0
    total = sum(sizes)
    return version, {"writers": writers, "rows": table.num_rows, "seconds": round(elapsed, 2),
                     "rows_per_s": round(table.num_rows / elapsed),
                     "mb_per_s": round(total / 1e6 / elapsed, 1),
                     "bytes_per_record": round(total / table.num_rows, 1),
                     "chunk_kb_avg": round(total / len(sizes) / 1024)}


def read_version(version: uuid.UUID) -> dict:
    t0 = time.perf_counter()
    rows = 0
    with psycopg.connect(PG_URL, autocommit=True) as c:
        for (blob,) in c.execute(
                "select bytes from spool.spool_chunk where version_id = %s order by chunk_no", (version,)):
            rows += pq.read_table(pa.BufferReader(blob)).num_rows
    elapsed = time.perf_counter() - t0
    return {"rows": rows, "seconds": round(elapsed, 2), "rows_per_s": round(rows / elapsed)}


def snapshot_diff(table: pa.Table) -> dict:
    rng = np.random.default_rng(11)
    n = table.num_rows
    keys = duckdb.connect()
    keys.register("t", table)
    t_snap = time.perf_counter()
    old = pa.table({"k": keys.sql("select hash(_fc_key) from t").to_arrow_table().column(0),
                    "h": table["_fc_row_hash"]})  # both uint64
    snapshot_build_s = time.perf_counter() - t_snap
    new_h = old["h"].to_numpy().copy()
    changed = rng.choice(n, n // 100, replace=False)          # 1% updated
    new_h[changed] += np.uint64(1)
    keep = np.ones(n, bool)
    keep[rng.choice(n, n // 200, replace=False)] = False      # 0.5% deleted
    added = pa.table({"k": pa.array(rng.integers(0, 2**63, n // 200, dtype=np.uint64)),  # 0.5% inserted
                      "h": pa.array(rng.integers(0, 2**63, n // 200, dtype=np.uint64))})
    new = pa.concat_tables([pa.table({"k": old["k"].filter(pa.array(keep)),
                                      "h": pa.array(new_h[keep])}), added])
    snap_bytes = len(to_parquet(old))
    con = duckdb.connect()
    con.register("old", old)
    con.register("new", new)
    t0 = time.perf_counter()
    upserts = con.execute("select count(*) from new left join old using (k)"
                          " where old.h is null or old.h <> new.h").fetchone()[0]
    deletes = con.execute("select count(*) from old anti join new using (k)").fetchone()[0]
    elapsed = time.perf_counter() - t0
    return {"rows": n, "diff_seconds": round(elapsed, 3), "upserts": upserts, "deletes": deletes,
            "snapshot_bytes_per_row": round(snap_bytes / n, 1),
            "snapshot_build_seconds": round(snapshot_build_s, 3)}


def retention_drop(c: psycopg.Connection, table: pa.Table) -> dict:
    yesterday = date.today() - timedelta(days=1)
    write_version(table, writers=4, created_on=yesterday)
    size = c.execute(f"select pg_total_relation_size('spool.spool_chunk_{yesterday:%Y%m%d}')").fetchone()[0]
    t0 = time.perf_counter()
    c.execute(f"drop table spool.spool_chunk_{yesterday:%Y%m%d}")
    return {"partition_mb": round(size / 1e6, 1), "drop_seconds": round(time.perf_counter() - t0, 3)}


def micro_batches(c: psycopg.Connection, table: pa.Table, n: int = 500, size: int = 10) -> dict:
    parquet_ms, jsonb_ms = [], []
    sample = table.slice(0, n * size)
    for i in range(n):
        part = sample.slice(i * size, size)
        t0 = time.perf_counter()
        c.execute("insert into spool.spool_chunk (version_id, chunk_no, rows, bytes) values (%s, 0, %s, %s)",
                  (uuid.uuid4(), size, to_parquet(part)))
        parquet_ms.append((time.perf_counter() - t0) * 1000)
        rows = part.to_pylist()
        t0 = time.perf_counter()
        v = uuid.uuid4()
        with c.cursor() as cur:
            cur.executemany(
                "insert into spool.inline_record (version_id, seq, key, op, payload) values (%s, %s, %s, %s, %s)",
                [(v, j, r["_fc_key"], r["_fc_op"], json.dumps(r, default=str)) for j, r in enumerate(rows)])
        jsonb_ms.append((time.perf_counter() - t0) * 1000)
    q = lambda xs: {"p50_ms": round(statistics.median(xs), 2),
                    "p95_ms": round(sorted(xs)[int(0.95 * (len(xs) - 1))], 2)}
    return {"batches": n, "events_per_batch": size, "parquet": q(parquet_ms), "jsonb_rows": q(jsonb_ms)}


def main() -> None:
    RESULTS.mkdir(exist_ok=True)
    out: dict = {"rows": ROWS}
    t0 = time.perf_counter()
    table = make_rows(ROWS)
    out["generate_seconds"] = round(time.perf_counter() - t0, 1)
    out["arrow_mb"] = round(table.nbytes / 1e6, 1)
    with psycopg.connect(PG_URL, autocommit=True) as c:
        setup(c)
        v1, out["write_1_writer"] = write_version(table, writers=1)
        v4, out["write_4_writers"] = write_version(table, writers=4)
        out["read"] = read_version(v4)
        out["snapshot_diff"] = snapshot_diff(table)
        out["retention"] = retention_drop(c, table)
        out["micro_batches"] = micro_batches(c, table)
        out["spool_table_mb"] = round(float(c.execute(
            "select sum(pg_total_relation_size(inhrelid)) from pg_inherits"
            " where inhparent = 'spool.spool_chunk'::regclass").fetchone()[0]) / 1e6, 1)
    print(json.dumps(out, indent=2))
    (RESULTS / "spool.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

"""S2: minimal prototype of the unified data model (ADR-0004) on Postgres. Throwaway.

Engine-free on purpose: the question is whether dataset -> version -> route ledger survives
the hard cases, independent of the engine.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DDL = """
drop schema if exists core cascade;
create schema core;
create table core.version (
  id bigserial primary key, dataset text not null, partition_key text not null,
  complete bool not null, run_id text not null, upserts int not null, deletes int not null);
create table core.version_record (
  version_id bigint not null references core.version (id), seq int not null, key text not null,
  op text not null, payload jsonb, row_hash text, moved_from text, primary key (version_id, seq));
create table core.record_state (
  dataset text not null, key text not null, partition_key text not null, row_hash text not null,
  payload jsonb not null, deleted bool not null default false, last_version bigint not null,
  primary key (dataset, key));
create table core.delivery (
  route text not null, version_id bigint not null references core.version (id),
  partition_key text not null, status text not null default 'pending',
  attempts int not null default 0, error text, primary key (route, version_id));
create table core.quarantine (
  route text not null, version_id bigint not null, key text not null, error text not null,
  status text not null default 'open', primary key (route, version_id, key));
create table core.dest (
  route text not null, key text not null, partition_key text not null, payload jsonb not null,
  primary key (route, key));
create table core.dest_log (
  route text not null, key text not null, op text not null, version_id bigint not null);
create table core.complete_through (
  dataset text not null, day text not null, primary key (dataset, day));
"""


def row_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


@dataclass
class Dataset:
    name: str
    key: Callable[[dict], str]
    partition: Callable[[dict], str]


@dataclass
class Route:
    name: str
    dataset: str
    mode: str = "upsert"  # upsert | replace_partition
    validate: Callable[[dict], str | None] | None = None
    destination_up: bool = True


class Model:
    def __init__(self, url: str) -> None:
        self.c = psycopg.connect(url, autocommit=True, row_factory=dict_row)
        self.c.execute(DDL)
        self.routes: list[Route] = []

    # ---- extraction -------------------------------------------------------------------
    def extract(self, ds: Dataset, rows: list[dict], *, run_id: str,
                complete_partitions: Iterable[str] = (), deletes: Iterable[str] = ()) -> list[int]:
        """Seal versions for one extraction run.

        rows: records read by this run. complete_partitions: slices this run read in full
        (snapshot_diff / replace); keys previously in those slices and absent from *every*
        slice of the run become deletes (run-level reconciliation, so a record that moved
        between two slices of the same run is a move, not a delete + re-insert).
        deletes: explicit deletes (tombstones) for incomplete strategies.
        """
        complete_partitions = set(complete_partitions)
        seen = {ds.key(r) for r in rows}
        changes: dict[str, list[dict]] = {}
        with self.c.transaction():
            state = {s["key"]: s for s in self.c.execute(
                "select * from core.record_state where dataset = %s", (ds.name,))}
            for r in rows:
                key, part, h = ds.key(r), ds.partition(r), row_hash(r)
                old = state.get(key)
                if old and not old["deleted"] and old["row_hash"] == h and old["partition_key"] == part:
                    continue  # no-op: unchanged (lookback re-reads don't churn routes)
                moved = old["partition_key"] if old and not old["deleted"] and old["partition_key"] != part else None
                changes.setdefault(part, []).append(
                    {"key": key, "op": "upsert", "payload": r, "hash": h, "moved_from": moved})
            gone = [s for s in state.values() if not s["deleted"] and s["key"] not in seen
                    and s["partition_key"] in complete_partitions]
            gone += [state[k] for k in deletes if k in state and not state[k]["deleted"]]
            for s in gone:
                changes.setdefault(s["partition_key"], []).append(
                    {"key": s["key"], "op": "delete", "payload": None, "hash": None, "moved_from": None})
            for part in complete_partitions:
                changes.setdefault(part, [])  # a complete read seals a version even if nothing changed
            return [self._seal(ds, part, recs, run_id, part in complete_partitions)
                    for part, recs in changes.items() if recs or part in complete_partitions]

    def _seal(self, ds: Dataset, part: str, recs: list[dict], run_id: str, complete: bool) -> int:
        ups = sum(1 for r in recs if r["op"] == "upsert")
        vid = self.c.execute(
            "insert into core.version (dataset, partition_key, complete, run_id, upserts, deletes)"
            " values (%s, %s, %s, %s, %s, %s) returning id",
            (ds.name, part, complete, run_id, ups, len(recs) - ups)).fetchone()["id"]
        for seq, r in enumerate(recs):
            self.c.execute(
                "insert into core.version_record values (%s, %s, %s, %s, %s, %s, %s)",
                (vid, seq, r["key"], r["op"], Jsonb(r["payload"]) if r["payload"] else None,
                 r["hash"], r["moved_from"]))
            if r["op"] == "upsert":
                self.c.execute(
                    "insert into core.record_state values (%s, %s, %s, %s, %s, false, %s)"
                    " on conflict (dataset, key) do update set partition_key = excluded.partition_key,"
                    " row_hash = excluded.row_hash, payload = excluded.payload, deleted = false,"
                    " last_version = excluded.last_version",
                    (ds.name, r["key"], part, r["hash"], Jsonb(r["payload"]), vid))
            else:
                self.c.execute("update core.record_state set deleted = true, last_version = %s"
                               " where dataset = %s and key = %s", (vid, ds.name, r["key"]))
        for route in self.routes:
            if route.dataset == ds.name:
                self.c.execute("insert into core.delivery (route, version_id, partition_key)"
                               " values (%s, %s, %s)", (route.name, vid, part))
        if complete:
            self.c.execute("insert into core.complete_through values (%s, %s) on conflict do nothing",
                           (ds.name, part.split("date=")[-1]))
        return vid

    # ---- delivery ---------------------------------------------------------------------
    def apply(self, route: Route) -> dict[str, int]:
        """Apply pending versions in seal order per partition; a failed version blocks its partition."""
        stats = {"applied": 0, "failed": 0, "quarantined": 0}
        blocked: set[str] = set()
        pending = self.c.execute(
            "select d.version_id, d.partition_key, v.complete from core.delivery d"
            " join core.version v on v.id = d.version_id"
            " where d.route = %s and d.status in ('pending', 'failed') order by d.version_id",
            (route.name,)).fetchall()
        for p in pending:
            if p["partition_key"] in blocked:
                continue
            if not route.destination_up:
                self.c.execute("update core.delivery set status = 'failed', attempts = attempts + 1,"
                               " error = 'destination down' where route = %s and version_id = %s",
                               (route.name, p["version_id"]))
                blocked.add(p["partition_key"])
                stats["failed"] += 1
                continue
            with self.c.transaction():
                stats["quarantined"] += self._apply_version(route, p)
                self.c.execute("update core.delivery set status = 'applied', attempts = attempts + 1,"
                               " error = null where route = %s and version_id = %s",
                               (route.name, p["version_id"]))
            stats["applied"] += 1
        return stats

    def _apply_version(self, route: Route, p: dict) -> int:
        quarantined = 0
        recs = self.c.execute("select * from core.version_record where version_id = %s order by seq",
                              (p["version_id"],)).fetchall()
        for r in recs:
            if r["op"] == "upsert" and route.validate and (err := route.validate(r["payload"])):
                self.c.execute("insert into core.quarantine (route, version_id, key, error)"
                               " values (%s, %s, %s, %s)", (route.name, p["version_id"], r["key"], err))
                quarantined += 1
                continue
            if r["op"] == "upsert":
                if route.mode == "replace_partition" and r["moved_from"]:
                    self.c.execute("delete from core.dest where route = %s and key = %s and partition_key = %s",
                                   (route.name, r["key"], r["moved_from"]))
                self.c.execute(
                    "insert into core.dest values (%s, %s, %s, %s) on conflict (route, key) do update"
                    " set partition_key = excluded.partition_key, payload = excluded.payload",
                    (route.name, r["key"], p["partition_key"], Jsonb(r["payload"])))
                self.c.execute("update core.quarantine set status = 'superseded' where route = %s"
                               " and key = %s and version_id < %s and status = 'open'",
                               (route.name, r["key"], p["version_id"]))
            else:
                self.c.execute("delete from core.dest where route = %s and key = %s", (route.name, r["key"]))
            self.c.execute("insert into core.dest_log values (%s, %s, %s, %s)",
                           (route.name, r["key"], r["op"], p["version_id"]))
        return quarantined

    # ---- queries used by the tests ------------------------------------------------------
    def dest(self, route: str) -> dict[str, dict]:
        return {r["key"]: r for r in self.c.execute(
            "select key, partition_key, payload from core.dest where route = %s", (route,))}

    def ready(self, datasets: list[str], day: str) -> bool:
        have = self.c.execute("select count(*) as n from core.complete_through where day = %s"
                              " and dataset = any(%s)", (day, datasets)).fetchone()["n"]
        return have == len(datasets)

    def scalar(self, sql: str, params: tuple = ()) -> Any:
        return next(iter(self.c.execute(sql, params).fetchone().values()))

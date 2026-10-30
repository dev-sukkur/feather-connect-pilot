"""Execution journal that every conformance test asserts on, plus small helper tables."""

from __future__ import annotations

import atexit
import json
import os
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from spike.config import PG_URL

DDL = """
create schema if not exists spike;
create table if not exists spike.exec_log (
  id bigserial primary key,
  run text not null,
  task_id text not null,
  task text not null,
  attempt int not null,
  worker text not null,
  code_version text not null,
  event text not null,
  at timestamptz not null default clock_timestamp(),
  info jsonb not null default '{}'
);
create index if not exists exec_log_run_idx on spike.exec_log (run, task_id);
create table if not exists spike.effects (
  id bigserial primary key,
  run text not null,
  effect_key text not null,
  worker text not null,
  at timestamptz not null default clock_timestamp()
);
create table if not exists spike.checkpoint (
  run text not null,
  task_id text not null,
  chunk text not null,
  result jsonb,
  primary key (run, task_id, chunk)
);
create table if not exists spike.counter (
  run text not null,
  task_id text not null,
  n int not null default 0,
  primary key (run, task_id)
);
create table if not exists spike.submit_log (
  run text not null,
  task_id text not null,
  at timestamptz not null
);
create table if not exists spike.worker_ready (
  worker text primary key,
  pid int not null,
  at timestamptz not null default clock_timestamp()
);
create table if not exists spike.recover_request (
  id bigserial primary key,
  dead_worker text not null,
  done_at timestamptz
);
create table if not exists spike.agent_task (
  id bigserial primary key,
  run text not null,
  task_ref text not null,
  args jsonb not null,
  leased_at timestamptz,
  done_at timestamptz
);
create table if not exists spike.budget (
  name text primary key,
  limit_units int not null,
  period_s double precision not null,
  reserve_high double precision not null
);
create table if not exists spike.budget_use (
  name text not null,
  window_start double precision not null,
  used int not null default 0,
  used_high int not null default 0,
  primary key (name, window_start)
);
create table if not exists spike.budget_grant (
  id bigserial primary key,
  name text not null,
  window_start double precision not null,
  priority text not null,
  run text not null,
  at timestamptz not null default clock_timestamp()
);
"""

_pool: ConnectionPool | None = None


def pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            PG_URL, min_size=1, max_size=20, kwargs={"autocommit": True}, open=True
        )
        atexit.register(_pool.close)
    return _pool


def init() -> None:
    with psycopg.connect(PG_URL, autocommit=True) as c:
        c.execute(DDL)


def execute(sql: str, params: tuple = ()) -> None:
    with pool().connection() as c:
        c.execute(sql, params)


def query(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with pool().connection() as c:
        with c.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def scalar(sql: str, params: tuple = ()) -> Any:
    rows = query(sql, params)
    return next(iter(rows[0].values())) if rows else None


def log(run: str, task_id: str, task: str, attempt: int, worker: str, version: str,
        event: str, **info: Any) -> None:
    execute(
        "insert into spike.exec_log (run, task_id, task, attempt, worker, code_version, event, info)"
        " values (%s, %s, %s, %s, %s, %s, %s, %s)",
        (run, task_id, task, attempt, worker, version, event, json.dumps(info)),
    )


def checkpoint_effect(run: str, task_id: str, chunk: str, worker: str) -> bool:
    """Write a chunk's effect and mark it done atomically. False if it was already done."""
    with pool().connection() as c:
        with c.transaction():
            row = c.execute(
                "insert into spike.checkpoint (run, task_id, chunk) values (%s, %s, %s)"
                " on conflict do nothing returning 1",
                (run, task_id, chunk),
            ).fetchone()
            if row is None:
                return False
            c.execute(
                "insert into spike.effects (run, effect_key, worker) values (%s, %s, %s)",
                (run, f"{task_id}:{chunk}", worker),
            )
            return True


def memo_get(run: str, task_id: str, chunk: str) -> tuple[bool, Any]:
    rows = query(
        "select result from spike.checkpoint where run = %s and task_id = %s and chunk = %s",
        (run, task_id, chunk),
    )
    return (True, rows[0]["result"]) if rows else (False, None)


def memo_put(run: str, task_id: str, chunk: str, result: Any) -> None:
    execute(
        "insert into spike.checkpoint (run, task_id, chunk, result) values (%s, %s, %s, %s)"
        " on conflict (run, task_id, chunk) do update set result = excluded.result",
        (run, task_id, chunk, json.dumps(result)),
    )


def bump_counter(run: str, task_id: str) -> int:
    return scalar(
        "insert into spike.counter (run, task_id, n) values (%s, %s, 1)"
        " on conflict (run, task_id) do update set n = spike.counter.n + 1 returning n",
        (run, task_id),
    )


def mark_ready(worker: str) -> None:
    execute(
        "insert into spike.worker_ready (worker, pid) values (%s, %s)"
        " on conflict (worker) do update set pid = excluded.pid, at = clock_timestamp()",
        (worker, os.getpid()),
    )


def clear_ready(worker: str) -> None:
    execute("delete from spike.worker_ready where worker = %s", (worker,))


def create_agent_task(run: str, task_ref: str, args: dict) -> int:
    return scalar(
        "insert into spike.agent_task (run, task_ref, args) values (%s, %s, %s) returning id",
        (run, task_ref, json.dumps(args)),
    )

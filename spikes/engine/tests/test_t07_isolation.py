import time
import uuid

import psycopg

from conftest import count, record, wait_until
from spike.config import PG_URL
from spike.port import TaskCall


def test_t07_schema_isolation_and_locks(client, workers, run, engine_name):
    workers.start("w1")
    client.submit(TaskCall(task="hold", args={"seconds": 15}, idempotency_key=f"{run}-hold"), run)
    wait_until(lambda: count(run, "start") >= 1, timeout=60, what="long task running")

    table = f"feather.t07_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(PG_URL, autocommit=True) as c:
        c.execute("set lock_timeout = '5s'")
        t0 = time.monotonic()
        c.execute("create schema if not exists feather")
        c.execute(f"create table {table} (id int)")
        c.execute(f"alter table {table} add column note text")
        c.execute(f"create index on {table} (id)")
        c.execute(f"drop table {table}")
        ddl_s = time.monotonic() - t0
        foreign_locks = c.execute(
            "select count(*) from pg_locks l join pg_class r on r.oid = l.relation"
            " join pg_namespace n on n.oid = r.relnamespace"
            " where n.nspname = 'feather' and l.pid <> pg_backend_pid()").fetchone()[0]
        schemas = sorted(r[0] for r in c.execute(
            "select distinct table_schema from information_schema.tables"
            " where table_schema not in ('pg_catalog', 'information_schema', 'spike', 'feather')"))
        extensions = sorted(r[0] for r in c.execute("select extname from pg_extension"))
        databases = sorted(r[0] for r in c.execute(
            "select datname from pg_database where not datistemplate"))

    record(engine_name, "T7", ddl_s=round(ddl_s, 3), foreign_locks_on_feather=foreign_locks,
           engine_schemas_in_app_db=schemas, extensions=extensions, databases=databases)
    assert ddl_s < 2 and foreign_locks == 0 and "public" not in schemas

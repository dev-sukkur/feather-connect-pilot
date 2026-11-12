"""Makes the demo's staging table in the Postgres that `feather dev` runs, then waits: the schema `zakya_demo` and its
table `invoices`, which the `staging` route upserts into and `invoices-to-sap` reads.

    python sims/staging.py          # reads FEATHER_DATABASE_URL, as `feather dev` gives it
"""

import os
import threading

import psycopg

DDL = """
create schema if not exists zakya_demo;
create table if not exists zakya_demo.invoices (
    invoice_id text primary key,
    invoice_number text,
    customer_name text,
    total numeric,
    status text,
    last_modified_time text
);
"""


def main() -> None:
    url = os.environ["FEATHER_DATABASE_URL"]
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(DDL)
    print("staging table zakya_demo.invoices is there", flush=True)
    threading.Event().wait()


if __name__ == "__main__":
    main()

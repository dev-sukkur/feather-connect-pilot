"""Makes the demo's staging tables in the Postgres that `feather dev` runs, then waits: the schema `zakya_demo` with
`invoices` and `credit_notes` (each with its channel, B2B or B2C, and its lines as JSON), which the `staging` and
`staging-returns` routes upsert into and the flows to SAP read, and `materials`, SAP's material master. A table made
by an earlier version of the demo gets the columns it lacks.

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
alter table zakya_demo.invoices
    add column if not exists customer_id text,
    add column if not exists channel text,
    add column if not exists gst_no text,
    add column if not exists invoice_date text,
    add column if not exists lines jsonb;
create table if not exists zakya_demo.credit_notes (
    creditnote_id text primary key,
    creditnote_number text,
    invoice_id text,
    invoice_number text,
    customer_id text,
    customer_name text,
    channel text,
    gst_no text,
    note_date text,
    total numeric,
    status text,
    reason text,
    last_modified_time text,
    lines jsonb
);
create table if not exists zakya_demo.materials (
    ean text primary key,
    material text,
    description text,
    pos_code text,
    item_id text,
    uom text,
    price numeric
);
"""


def main() -> None:
    url = os.environ["FEATHER_DATABASE_URL"]
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(DDL)
    print("staging tables zakya_demo.invoices, credit_notes and materials are there", flush=True)
    threading.Event().wait()


if __name__ == "__main__":
    main()

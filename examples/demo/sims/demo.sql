-- The warehouse's table, which postgres.table expects to find; Feather adds its own _fc_* columns. The simulation
-- sends every field as text, as JSON has it, so the columns are text: cast them in a view to work with them.
create schema if not exists demo;
create table if not exists demo.orders (
    order_id text primary key,
    store_id text,
    customer text,
    status text,
    total text,
    placed_at text
);

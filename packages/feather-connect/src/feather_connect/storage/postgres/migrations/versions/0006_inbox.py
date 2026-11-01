"""The inbox as the sealer needs it, with event ids kept beside it.

0001 made `feather.inbox` with no partitions, so nothing can have been written to it, and it can be replaced. An event is
kept whole: the raw body and headers are what a signature covers and what `receive` parses, and they need not be JSON.
It has no `signature_ok`, because an event that fails its check is never stored.

An event id is unique across days, and a unique index on a partitioned table has to include the day. So the ids that have
been seen are in a plain table of their own, with the day whose retention frees them.

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"

_STATEMENTS = [
    "drop table feather.inbox",
    """create table feather.inbox (
        tenant_id text not null,
        event_seq bigint not null default nextval('feather.event_seq'),
        day date not null,
        dataset text not null,
        event_id text,
        method text not null,
        path text not null,
        headers jsonb not null,
        body bytea not null,
        received_at timestamptz not null,
        sealed_at timestamptz,
        primary key (tenant_id, event_seq, day)
    ) partition by range (day)""",
    # What the sealer looks for. Most of the table is sealed and waiting for retention (design 7.4).
    """create index inbox_open_idx on feather.inbox (tenant_id, dataset, event_seq)
        where sealed_at is null""",
    """create table feather.inbox_event_id (
        tenant_id text not null,
        dataset text not null,
        event_id text not null,
        day date not null,
        primary key (tenant_id, dataset, event_id)
    )""",
    "create index inbox_event_id_day_idx on feather.inbox_event_id (day)",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)

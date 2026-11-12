"""Readiness (M2 plan 1 §1): the partitions someone closed, and the firings of waiting flows, each once by its key.
What is sealed and applied is read from `version` and `delivery`, which hold it already.

Revision ID: 0028
Revises: 0027
"""

from alembic import op

revision = "0028"
down_revision = "0027"

_STATEMENTS = [
    """create table feather.readiness_close (
        tenant_id text not null,
        dataset text not null,
        partition text not null,
        closed_at timestamptz not null,
        closed_by text not null,
        reason text not null,
        primary key (tenant_id, dataset, partition)
    )""",
    """create table feather.readiness_firing (
        tenant_id text not null,
        flow text not null,
        ready_set text not null,
        period text not null,
        scope text not null,
        seq int not null,
        reason text not null,
        versions jsonb not null,
        fired_at timestamptz not null,
        actor text not null,
        run_id text,
        primary key (tenant_id, flow, ready_set, period, scope, seq)
    )""",
    "create index readiness_firing_at_idx on feather.readiness_firing (tenant_id, flow, fired_at desc)",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("drop table feather.readiness_firing")
    op.execute("drop table feather.readiness_close")

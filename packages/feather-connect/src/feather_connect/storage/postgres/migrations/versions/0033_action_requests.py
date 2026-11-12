"""Destructive actions asked for in a protected environment, waiting for a second person (M3 plan 1 E3).

Revision ID: 0033
Revises: 0032
"""

from alembic import op

revision = "0033"
down_revision = "0032"


def upgrade() -> None:
    op.execute(
        """create table feather.action_request (
            tenant_id text not null,
            request_id text not null,
            action text not null,
            target text not null,
            params jsonb not null default '{}',
            requested_by text not null,
            reason text,
            status text not null check (status in ('waiting', 'approved', 'rejected')),
            created_at timestamptz not null,
            decided_by text,
            decided_at timestamptz,
            decision_reason text,
            result text,
            primary key (tenant_id, request_id)
        )"""
    )
    op.execute(
        "create index action_request_created_idx on feather.action_request (tenant_id, created_at desc)"
    )


def downgrade() -> None:
    op.execute("drop table feather.action_request")

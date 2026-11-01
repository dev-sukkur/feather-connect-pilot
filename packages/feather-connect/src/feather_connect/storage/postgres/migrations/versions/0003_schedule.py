"""The scheduler remembers the last tick of each trigger, so that a restart doesn't fire it twice or lose it.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"


def upgrade() -> None:
    op.execute(
        """create table feather.schedule (
            tenant_id text not null,
            flow text not null,
            trigger_id text not null,
            last_tick timestamptz not null,
            primary key (tenant_id, flow, trigger_id)
        )"""
    )

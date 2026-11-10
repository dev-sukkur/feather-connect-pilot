"""The dependency environments each worker built (flow packages §9, task 8): one row per worker and lock hash, so the
Packages page can say where a version's environment is ready. `tenant_id` leads the key (ADR-0013 rule 1).

Revision ID: 0022
Revises: 0021
"""

from alembic import op

revision = "0022"
down_revision = "0021"


def upgrade() -> None:
    op.execute(
        """create table feather.package_env (
            tenant_id text not null,
            worker_id text not null,
            lock_hash text not null,
            state text not null check (state in ('building', 'ready', 'failed')),
            error text,
            built_at timestamptz not null,
            primary key (tenant_id, worker_id, lock_hash)
        )"""
    )


def downgrade() -> None:
    op.execute("drop table feather.package_env")

"""Index runs by when they were created and by status, for the dashboard's counts and its runs a day (stage U).

Revision ID: 0021
Revises: 0020
"""

from alembic import op

revision = "0021"
down_revision = "0020"


def upgrade() -> None:
    op.execute("create index run_created_idx on feather.run (tenant_id, created_at)")
    op.execute("create index run_status_idx on feather.run (tenant_id, status, created_at)")


def downgrade() -> None:
    op.execute("drop index feather.run_status_idx")
    op.execute("drop index feather.run_created_idx")

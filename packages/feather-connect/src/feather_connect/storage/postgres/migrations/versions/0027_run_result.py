"""What a run found, for a run that answers a question rather than delivering: a partition check of a packaged dataset
runs on its version's host, the only process that has its code, and leaves its counts here for the server to read.

Revision ID: 0027
Revises: 0026
"""

from alembic import op

revision = "0027"
down_revision = "0026"


def upgrade() -> None:
    op.execute("alter table feather.run add column result jsonb")


def downgrade() -> None:
    op.execute("alter table feather.run drop column result")

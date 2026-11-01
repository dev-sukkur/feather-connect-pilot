"""Who may write a version: the execution that last claimed it (invariant I1).

Revision ID: 0005
Revises: 0004
"""

from alembic import op

revision = "0005"
down_revision = "0004"


def upgrade() -> None:
    op.execute("alter table feather.spool_version add column writer text")

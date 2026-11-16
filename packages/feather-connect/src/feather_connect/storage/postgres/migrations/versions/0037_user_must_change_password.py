"""A user whose password an admin set chooses their own when they next sign in. Additive: nobody must change theirs.

Revision ID: 0037
Revises: 0036
"""

from alembic import op

revision = "0037"
down_revision = "0036"


def upgrade() -> None:
    op.execute("alter table feather.app_user add column must_change_password boolean not null default false")


def downgrade() -> None:
    op.execute("alter table feather.app_user drop column must_change_password")

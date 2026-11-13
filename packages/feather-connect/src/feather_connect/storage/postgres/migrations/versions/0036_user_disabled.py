"""A user can be disabled: they can't sign in, and their tokens are refused, while their audit trail stays.
Additive: every user is enabled.

Revision ID: 0036
Revises: 0035
"""

from alembic import op

revision = "0036"
down_revision = "0035"


def upgrade() -> None:
    op.execute("alter table feather.app_user add column disabled_at timestamptz")


def downgrade() -> None:
    op.execute("alter table feather.app_user drop column disabled_at")

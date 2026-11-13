"""The reference datasets a delivery attempt's transforms read, each with the stamp of the state it read (M3.1 plan 1
F1), so that a delivery made with a SQL transform or a lookup can be explained afterwards. Additive: older attempts
have none.

Revision ID: 0034
Revises: 0033
"""

from alembic import op

revision = "0034"
down_revision = "0033"


def upgrade() -> None:
    op.execute("alter table feather.delivery_attempt add column reference_versions jsonb")


def downgrade() -> None:
    op.execute("alter table feather.delivery_attempt drop column reference_versions")

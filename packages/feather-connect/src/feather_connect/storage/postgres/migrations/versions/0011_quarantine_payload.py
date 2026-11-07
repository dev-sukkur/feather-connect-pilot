"""A quarantined record keeps what it was: the payload a sink turned down, or the raw text of a row its source could
not read, so that it can be looked at and retried (ruling 11). Both are redacted, and are null for older entries.

Revision ID: 0011
Revises: 0010
"""

from alembic import op

revision = "0011"
down_revision = "0010"


def upgrade() -> None:
    op.execute("alter table feather.quarantine add column payload jsonb, add column raw text")

"""The key index by version and then key in code point order, so that a page of a version's records (a task's Records
panel) starts where it left off and reads only that page, however many keys the version holds.

Revision ID: 0014
Revises: 0013
"""

from alembic import op

revision = "0014"
down_revision = "0013"


def upgrade() -> None:
    op.execute(
        'create index record_key_version_key_idx on feather.record_key (tenant_id, version_id, key collate "C")'
    )
    op.execute("drop index feather.record_key_version_idx")  # the new one serves what it did

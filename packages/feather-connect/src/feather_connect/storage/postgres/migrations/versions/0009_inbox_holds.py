"""What the sealer needs to read an event again later: a failed read that may pass is counted and put off, and an event
that can't be sealed is held, unsealed, until someone reprocesses or releases it. A held event keeps its day, as any
unsealed event does, so its body is there to read again.

Revision ID: 0009
Revises: 0008
"""

from alembic import op

revision = "0009"
down_revision = "0008"

_STATEMENTS = [
    """alter table feather.inbox
        add column tries integer not null default 0,
        add column retry_at timestamptz,
        add column held_at timestamptz""",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)

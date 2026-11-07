"""What destinations answered, per record of a delivered chunk: the HTTP status and the fields a sink's `response_map`
took from the answer, never the raw body or headers (ruling 5). A rejected record keeps the body it was sent with, so
that it can be looked at and retried. The rows go when their delivery goes.

Revision ID: 0010
Revises: 0009
"""

from alembic import op

revision = "0010"
down_revision = "0009"

_STATEMENTS = [
    """create table feather.delivery_response (
        tenant_id text not null,
        route text not null,
        version_id text not null,
        chunk_no int not null,
        key text not null,
        status int not null,
        fields jsonb not null,
        sent jsonb,
        at timestamptz not null,
        primary key (tenant_id, route, version_id, chunk_no, key)
    )""",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)

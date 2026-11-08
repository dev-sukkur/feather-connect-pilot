"""The key index: each key of a sealed version, written by the seal in its own transaction, so that the versions that
held one record are found without reading every spool chunk (ruling 11). Versions sealed before this are not in it.

Revision ID: 0013
Revises: 0012
"""

from alembic import op

revision = "0013"
down_revision = "0012"

_STATEMENTS = [
    """create table feather.record_key (
        tenant_id text not null,
        dataset text not null,
        key text not null,
        version_id text not null,
        partition text not null,
        op text not null,
        row_hash bigint not null,
        chunk_no int not null,
        primary key (tenant_id, dataset, key, version_id)
    )""",
    "create index record_key_version_idx on feather.record_key (tenant_id, version_id)",
    "create index quarantine_key_idx on feather.quarantine (tenant_id, key)",
    "create index audit_target_idx on feather.audit (tenant_id, target, at)",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)

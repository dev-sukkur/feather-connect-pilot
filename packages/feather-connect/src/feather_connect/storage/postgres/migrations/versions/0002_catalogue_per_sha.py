"""The catalogue keeps a row per code SHA (design 6.11), and says which project a flow is in and which flow a
route is in, so that a later sync can retire what a project no longer has.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"

_CATALOGUE = ("project", "flow", "dataset", "route")

_EXTRA = {"flow": "project", "route": "flow"}


def upgrade() -> None:
    for table in _CATALOGUE:
        op.execute(f"update feather.{table} set code_sha = '' where code_sha is null")
        op.execute(f"alter table feather.{table} alter column code_sha set not null")
        op.execute(f"alter table feather.{table} drop constraint {table}_pkey")
        op.execute(f"alter table feather.{table} add primary key (tenant_id, name, code_sha)")
        if table in _EXTRA:
            op.execute(f"alter table feather.{table} add column {_EXTRA[table]} text not null default ''")

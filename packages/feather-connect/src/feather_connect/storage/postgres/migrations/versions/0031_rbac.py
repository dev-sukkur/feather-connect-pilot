"""The roles `viewer` and `developer` beside `operator` and `admin`, and a user's add-on grants, `approver` and
`pii-reader` (design §13.2, M2 plan 2 O2).

Revision ID: 0031
Revises: 0030
"""

from alembic import op

revision = "0031"
down_revision = "0030"

_ROLES = "('admin', 'developer', 'operator', 'viewer')"


def upgrade() -> None:
    for table in ("app_user", "api_token"):
        op.execute(f"alter table feather.{table} drop constraint {table}_role_check")
        op.execute(f"alter table feather.{table} add constraint {table}_role_check check (role in {_ROLES})")
    op.execute("alter table feather.app_user add column grants text[] not null default '{}'")


def downgrade() -> None:
    op.execute("alter table feather.app_user drop column grants")
    for table in ("app_user", "api_token"):
        op.execute(f"update feather.{table} set role = 'operator' where role in ('developer', 'viewer')")
        op.execute(f"alter table feather.{table} drop constraint {table}_role_check")
        op.execute(
            f"alter table feather.{table} add constraint {table}_role_check check (role in ('admin', 'operator'))"
        )

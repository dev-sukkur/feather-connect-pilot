"""The roles `admin` and `operator` on users and API tokens (flow packages design §11, pulled forward from M2's RBAC).

Every user and token there is becomes `admin`, so nothing changes for anyone. A token also names the user whose it is,
as its effective role is the lower of its own and that user's; tokens from before have none.

Revision ID: 0015
Revises: 0014
"""

from alembic import op

revision = "0015"
down_revision = "0014"


def upgrade() -> None:
    for table in ("app_user", "api_token"):
        op.execute(f"alter table feather.{table} add column role text not null default 'admin'")
        op.execute(f"alter table feather.{table} alter column role drop default")  # a new row says its role
        op.execute(
            f"alter table feather.{table} add constraint {table}_role_check check (role in ('admin', 'operator'))"
        )
    op.execute("alter table feather.api_token add column created_by text")

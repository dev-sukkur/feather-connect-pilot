"""Each dataset's greatest sealed watermark, kept in one row that sealing updates, so that a `by_stamp` tick reads one
row rather than every sealed version (S6). A dataset sealed before this has no row; the store fills it on first read.

Revision ID: 0012
Revises: 0011
"""

from alembic import op

revision = "0012"
down_revision = "0011"


def upgrade() -> None:
    op.execute(
        """create table feather.dataset_watermark (
               tenant_id text not null,
               dataset text not null,
               watermark jsonb not null,
               primary key (tenant_id, dataset)
           )"""
    )

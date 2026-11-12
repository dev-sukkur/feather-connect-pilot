"""The applied deliveries of a route's partition by seal order, for the schema policy's last applied version (M2 plan
2 D1). `delivery_open_idx` covers only the deliveries that aren't applied.

Revision ID: 0029
Revises: 0028
"""

from alembic import op

revision = "0029"
down_revision = "0028"


def upgrade() -> None:
    op.execute(
        """create index delivery_applied_idx on feather.delivery (tenant_id, route, partition, seal_seq desc)
            where status = 'applied'"""
    )


def downgrade() -> None:
    op.execute("drop index feather.delivery_applied_idx")

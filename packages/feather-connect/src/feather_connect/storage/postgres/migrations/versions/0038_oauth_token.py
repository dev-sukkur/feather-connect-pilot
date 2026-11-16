"""OAuth2 access tokens shared by a tenant's processes, so that the server, the worker and each package host don't
each fetch their own (the M1 open item "OAuth2 tokens"). A token is sealed with a key derived from its grant's
secrets, which the database never holds, and its row is named by a hash of them. Additive.

Revision ID: 0038
Revises: 0037
"""

from alembic import op

revision = "0038"
down_revision = "0037"


def upgrade() -> None:
    op.execute(
        """create table feather.oauth_token (
            tenant_id text not null,
            token_key text not null,
            sealed bytea,
            expires_at timestamptz,
            fetching_until timestamptz,
            primary key (tenant_id, token_key)
        )"""
    )


def downgrade() -> None:
    op.execute("drop table feather.oauth_token")

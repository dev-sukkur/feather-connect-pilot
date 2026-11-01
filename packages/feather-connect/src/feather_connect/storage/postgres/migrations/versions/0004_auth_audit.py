"""Local users, API tokens and the audit log. A token is stored as its SHA-256, never as itself.

Revision ID: 0004
Revises: 0003
"""

from alembic import op

revision = "0004"
down_revision = "0003"


def upgrade() -> None:
    op.execute(
        """create table feather.app_user (
            tenant_id text not null,
            user_id text not null,
            username text not null,
            password_hash text not null,
            created_at timestamptz not null,
            primary key (tenant_id, user_id),
            unique (tenant_id, username)
        )"""
    )
    op.execute(
        """create table feather.api_token (
            tenant_id text not null,
            token_id text not null,
            name text not null,
            token_hash text not null,
            created_at timestamptz not null,
            expires_at timestamptz,
            revoked_at timestamptz,
            primary key (tenant_id, token_id),
            unique (tenant_id, token_hash)
        )"""
    )
    op.execute(
        """create table feather.audit (
            tenant_id text not null,
            audit_id text not null,
            at timestamptz not null,
            actor_type text not null,
            actor text not null,
            action text not null,
            target text,
            request jsonb not null,
            result text not null,
            primary key (tenant_id, audit_id)
        )"""
    )

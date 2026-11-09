"""The environment's own configuration, its encrypted secrets and the raw events of declared webhooks (flow packages
§7.2 and §8), each of a tenant (ADR-0013 rule 1: `tenant_id` leads every key).

`env_config` holds the connections, budgets and settings of each environment, outside any package, as plain values:
a connection's secret fields are references, never values. `secret` holds only ciphertext, sealed with the
environment's master key, which is never in the database. `webhook_event` keeps each verified event of a webhook a
package declared once by its key, until a worker parses it with the package's transform.

Revision ID: 0019
Revises: 0018
"""

from alembic import op

revision = "0019"
down_revision = "0018"


def upgrade() -> None:
    op.execute(
        """create table feather.env_config (
            tenant_id text not null,
            env text not null,
            kind text not null check (kind in ('connection', 'budget', 'setting')),
            name text not null,
            value jsonb not null,
            changed_by text not null,
            changed_at timestamptz not null,
            primary key (tenant_id, env, kind, name)
        )"""
    )
    op.execute(
        """create table feather.secret (
            tenant_id text not null,
            env text not null,
            name text not null,
            ciphertext bytea not null,
            nonce bytea not null,
            key_id text not null,
            updated_by text not null,
            updated_at timestamptz not null,
            primary key (tenant_id, env, name)
        )"""
    )
    op.execute(
        """create table feather.webhook_event (
            tenant_id text not null,
            package text not null,
            webhook text not null,
            event_key text not null,
            stamp text,
            body bytea not null,
            headers jsonb not null,
            received_at timestamptz not null,
            primary key (tenant_id, package, webhook, event_key)
        )"""
    )
    op.execute(
        "create index webhook_event_received on feather.webhook_event (tenant_id, package, webhook, received_at)"
    )


def downgrade() -> None:
    # The secrets go with the table: set them again after an upgrade. The other two can be imported or resent.
    op.execute("drop table feather.webhook_event")
    op.execute("drop table feather.secret")
    op.execute("drop table feather.env_config")

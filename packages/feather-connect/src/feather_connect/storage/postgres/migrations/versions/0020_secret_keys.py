"""The active master key of each environment, and the retention of declared webhook events (flow packages §7.2
and §8, the P3 review).

`secret_key` holds the id of the master key every secret of an environment is sealed with, never a key: setting a
secret locks it to share, so a process with another key is refused, and a rotation locks it to update and swaps it in
the same transaction as it seals every secret again. `webhook_event.processed_at` says when a worker parsed an event,
after which retention may delete it.

Revision ID: 0020
Revises: 0019
"""

from alembic import op

revision = "0020"
down_revision = "0019"


def upgrade() -> None:
    op.execute(
        """create table feather.secret_key (
            tenant_id text not null,
            env text not null,
            key_id text not null,
            changed_at timestamptz not null,
            primary key (tenant_id, env)
        )"""
    )
    # An environment that has secrets already has the key they are sealed with active.
    op.execute(
        """insert into feather.secret_key (tenant_id, env, key_id, changed_at)
           select distinct on (tenant_id, env) tenant_id, env, key_id, updated_at from feather.secret
           order by tenant_id, env, updated_at desc"""
    )
    op.execute("alter table feather.webhook_event add column processed_at timestamptz")
    op.execute(
        """create index webhook_event_processed on feather.webhook_event (tenant_id, received_at)
           where processed_at is not null"""
    )


def downgrade() -> None:
    # Downgrading forgets which key is active (the next secret set makes its key active again) and which events
    # were processed, so retention keeps every event until it is upgraded again.
    op.execute("drop index feather.webhook_event_processed")
    op.execute("alter table feather.webhook_event drop column processed_at")
    op.execute("drop table feather.secret_key")

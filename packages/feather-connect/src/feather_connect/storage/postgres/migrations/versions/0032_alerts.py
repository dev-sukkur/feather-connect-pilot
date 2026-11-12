"""Alerts and the environment's alert rules (M2 plan 2 O6): an alert is one row while it fires, so it can be queried,
acknowledged, snoozed and its notice retried.

Revision ID: 0032
Revises: 0031
"""

from alembic import op

revision = "0032"
down_revision = "0031"


def upgrade() -> None:
    op.execute(
        """create table feather.alert (
            tenant_id text not null,
            alert_id text not null,
            rule text not null,
            subject text not null,
            severity text not null check (severity in ('warning', 'critical')),
            message text not null,
            state text not null check (state in ('firing', 'cleared')),
            fired_at timestamptz not null,
            last_seen_at timestamptz not null,
            cleared_at timestamptz,
            acked_by text,
            acked_at timestamptz,
            snoozed_until timestamptz,
            notified_at timestamptz,
            primary key (tenant_id, alert_id)
        )"""
    )
    op.execute(
        "create unique index alert_firing_idx on feather.alert (tenant_id, rule, subject) where state = 'firing'"
    )
    op.execute("create index alert_fired_idx on feather.alert (tenant_id, fired_at desc)")
    op.execute(
        """create table feather.alert_rule (
            tenant_id text not null,
            name text not null,
            enabled boolean not null,
            params jsonb not null default '{}',
            channels text[] not null default '{}',
            updated_at timestamptz not null,
            primary key (tenant_id, name)
        )"""
    )


def downgrade() -> None:
    op.execute("drop table feather.alert_rule")
    op.execute("drop table feather.alert")

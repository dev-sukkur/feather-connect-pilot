"""The built-in package registry and the registries an environment trusts (flow packages §4.3 and §9), and the scope
of an API token: `all`, or `publish`, which may only upload to the built-in registry.

A wheel is kept in `registry_file` with its bytes: it is opaque, never changes and never expires, unlike spool chunks,
which hold Arrow tables of a dataset version and which retention drops. A row per file keeps the index one query, and
a database backup covers the registry. One file per version: a version is never replaced, only yanked.
`version_key` is the version with trailing zeros dropped (PEP 440), so that 1.0 can't be published after 1.0.0.

Revision ID: 0016
Revises: 0015
"""

from alembic import op

revision = "0016"
down_revision = "0015"


def upgrade() -> None:
    op.execute("alter table feather.api_token add column scope text not null default 'all'")
    op.execute("alter table feather.api_token alter column scope drop default")
    op.execute(
        "alter table feather.api_token add constraint api_token_scope_check check (scope in ('all', 'publish'))"
    )
    op.execute(
        """create table feather.registry_file (
            tenant_id text not null,
            project text not null,
            version text not null,
            version_key text not null,
            filename text not null,
            sha256 text not null,
            size bigint not null,
            manifest jsonb not null,
            content bytea not null,
            uploaded_by text not null,
            uploaded_at timestamptz not null,
            yanked_at timestamptz,
            yanked_reason text,
            primary key (tenant_id, filename),
            unique (tenant_id, project, version_key)
        )"""
    )
    op.execute(
        """create table feather.registry (
            tenant_id text not null,
            name text not null,
            index_url text not null,
            token_ref text,
            added_by text not null,
            added_at timestamptz not null,
            primary key (tenant_id, name)
        )"""
    )

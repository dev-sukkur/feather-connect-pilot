"""Installed flow packages (flow packages §5 and §9): the packages, their installed versions, their wheels and their
flows' activations, each of a tenant (ADR-0013 rule 1: `tenant_id` leads every key).

A version is installed once: `version_key` is the version with trailing zeros dropped (PEP 440), unique per package,
so 1.0 can't be installed after 1.0.0 and a version always means the same code. Its wheel is kept in `package_wheel`
by sha256, once however many versions or packages share those bytes, so that a host loads this copy and a registry
that loses or yanks the release never stops an environment. A flow's activation row starts `inactive` at install:
nothing runs until a flow is activated (§6).

`package_env` (the environments built per worker, task 8) and `env_config` (the environment's own connections and
budgets, task 5a) come with the tasks that use them.

Revision ID: 0018
Revises: 0017
"""

from alembic import op

revision = "0018"
down_revision = "0017"


def upgrade() -> None:
    op.execute(
        """create table feather.package (
            tenant_id text not null,
            name text not null,
            owner text,
            created_at timestamptz not null,
            primary key (tenant_id, name)
        )"""
    )
    op.execute(
        """create table feather.package_wheel (
            tenant_id text not null,
            sha256 text not null,
            size bigint not null,
            content bytea not null,
            stored_at timestamptz not null,
            primary key (tenant_id, sha256)
        )"""
    )
    op.execute(
        """create table feather.package_version (
            tenant_id text not null,
            name text not null,
            version text not null,
            version_key text not null,
            sha256 text not null,
            registry text not null,
            filename text not null,
            size bigint not null,
            manifest jsonb not null,
            fc_versions text not null,
            lock_hash text not null,
            state text not null
                check (state in ('installed', 'preparing', 'ready', 'failed', 'uninstalled')),
            load_check jsonb not null,
            installed_by text not null,
            installed_at timestamptz not null,
            primary key (tenant_id, name, version_key),
            foreign key (tenant_id, name) references feather.package (tenant_id, name),
            foreign key (tenant_id, sha256) references feather.package_wheel (tenant_id, sha256)
        )"""
    )
    op.execute(
        """create table feather.flow_activation (
            tenant_id text not null,
            flow text not null,
            package text not null,
            version_key text not null,
            version text not null,
            state text not null
                check (state in ('inactive', 'activating', 'active', 'deactivating', 'failed')),
            changed_by text not null,
            changed_at timestamptz not null,
            reason text,
            error text,
            primary key (tenant_id, flow, package, version_key),
            foreign key (tenant_id, package, version_key)
                references feather.package_version (tenant_id, name, version_key)
        )"""
    )
    op.execute(
        "create index flow_activation_package on feather.flow_activation (tenant_id, package, version_key)"
    )


def downgrade() -> None:
    # Every installed package goes, wheels included: the registry keeps what was published, so a downgrade loses only
    # what was installed from another registry, which can be installed again from there.
    op.execute("drop table feather.flow_activation")
    op.execute("drop table feather.package_version")
    op.execute("drop table feather.package_wheel")
    op.execute("drop table feather.package")

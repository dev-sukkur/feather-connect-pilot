"""The core schema: the run journal, versions, the delivery ledger, the spool, record state and budgets.

Revision ID: 0001
Revises:
"""

from alembic import op

revision = "0001"
down_revision = None

_CATALOGUE = ("project", "flow", "dataset", "route")

# Every table carries tenant_id (FR-I1) and leads its primary key with it.
_STATEMENTS = [
    "create sequence feather.seal_seq",  # backs Version.seal_seq
    "create sequence feather.event_seq",  # orders rows that no other column orders
    """create table feather.tenant (
        tenant_id text primary key,
        name text not null default '',
        created_at timestamptz not null default now()
    )""",
    """create table feather.run (
        tenant_id text not null,
        run_id text not null,
        flow text not null,
        trigger text not null,
        actor text not null,
        params jsonb not null,
        status text not null,
        reason text,
        created_at timestamptz not null,
        updated_at timestamptz not null,
        primary key (tenant_id, run_id)
    )""",
    "create index run_flow_idx on feather.run (tenant_id, flow, run_id desc)",
    """create table feather.task (
        tenant_id text not null,
        task_id text not null,
        seq bigint not null default nextval('feather.event_seq'),
        run_id text not null,
        kind text not null,
        ref text not null,
        dataset text,
        partition text,
        route text,
        attempt int not null default 1,
        status text not null,
        worker text,
        code_sha text,
        error text,
        started_at timestamptz not null,
        finished_at timestamptz,
        primary key (tenant_id, task_id),
        unique (tenant_id, run_id, ref)
    )""",
    "create index task_run_idx on feather.task (tenant_id, run_id, seq)",
    """create table feather.task_log (
        tenant_id text not null,
        log_id bigint not null default nextval('feather.event_seq'),
        task_id text not null,
        at timestamptz not null,
        level text not null,
        message text not null,
        primary key (tenant_id, log_id, at)
    ) partition by range (at)""",
    "create index task_log_task_idx on feather.task_log (tenant_id, task_id, log_id)",
    """create table feather.version (
        tenant_id text not null,
        version_id text not null,
        stage_seq bigint not null default nextval('feather.event_seq'),
        dataset text not null,
        partition text not null,
        run_id text not null,
        complete boolean not null,
        upserts int not null,
        deletes int not null,
        content_hash text not null,
        schema_hash text,
        watermark jsonb,
        code_sha text not null,
        spool_backend text not null default 'postgres',
        status text not null default 'staged',
        seal_seq bigint,
        sealed_at timestamptz,
        primary key (tenant_id, version_id)
    )""",
    """create index version_sealed_idx on feather.version (tenant_id, dataset, partition, seal_seq desc)
        where status = 'sealed'""",
    "create index version_run_idx on feather.version (tenant_id, run_id, dataset)",
    "create index version_dataset_idx on feather.version (tenant_id, dataset, stage_seq desc)",
    """create table feather.run_slice (
        tenant_id text not null,
        run_id text not null,
        dataset text not null,
        partition text not null,
        seq bigint not null default nextval('feather.event_seq'),
        done boolean not null default false,
        primary key (tenant_id, run_id, dataset, partition)
    )""",
    # The catalogue registered from code (design 7.1).
    *[
        f"""create table feather.{table} (
            tenant_id text not null,
            name text not null,
            spec jsonb not null default '{{}}',
            code_sha text,
            registered_at timestamptz not null default now(),
            retired_at timestamptz,
            primary key (tenant_id, name)
        )"""
        for table in _CATALOGUE
    ],
    """create table feather.connection (
        tenant_id text not null,
        name text not null,
        connector text not null,
        config jsonb not null default '{}',
        secret_refs jsonb not null default '{}',
        site text,
        available boolean not null default true,
        primary key (tenant_id, name)
    )""",
    # The spool. Chunks are zstd Parquet. A chunk lives in the day partition recorded for its version in
    # spool_version; snapshots are in a plain table, so that the latest outlives every day partition.
    """create table feather.spool_version (
        tenant_id text not null,
        version_id text not null,
        day date not null,
        primary key (tenant_id, version_id)
    )""",
    "create index spool_version_day_idx on feather.spool_version (tenant_id, day)",
    """create table feather.spool_chunk (
        tenant_id text not null,
        day date not null,
        version_id text not null,
        chunk_no int not null,
        kind text not null,
        data bytea not null,
        rows int not null,
        size int not null,
        primary key (tenant_id, day, version_id, chunk_no, kind)
    ) partition by range (day)""",
    """create table feather.spool_snapshot (
        tenant_id text not null,
        version_id text not null,
        chunk_no int not null,
        data bytea not null,
        rows int not null,
        size int not null,
        primary key (tenant_id, version_id, chunk_no)
    )""",
    """create table feather.record_state (
        tenant_id text not null,
        dataset text not null,
        key text not null,
        partition text not null,
        row_hash numeric(20, 0) not null,
        payload jsonb not null,
        last_version text not null,
        primary key (tenant_id, dataset, key)
    )""",
    """create table feather.partition_status (
        tenant_id text not null,
        dataset text not null,
        partition text not null,
        route text not null,
        latest_version text,
        applied_version text,
        complete_through jsonb,
        updated_at timestamptz not null default now(),
        primary key (tenant_id, dataset, partition, route)
    )""",
    """create table feather.delivery (
        tenant_id text not null,
        route text not null,
        version_id text not null,
        dataset text not null,
        partition text not null,
        seal_seq bigint not null,
        status text not null,
        attempts int not null default 0,
        last_error text,
        rows_applied int not null default 0,
        rows_rejected int not null default 0,
        replays int not null default 0,
        retries int not null default 0,
        updated_at timestamptz not null default now(),
        primary key (tenant_id, route, version_id)
    )""",
    # Work that isn't finished (design 7.4): the ledger is mostly applied rows.
    """create index delivery_open_idx on feather.delivery (tenant_id, route, partition, seal_seq)
        where status <> 'applied'""",
    "create index delivery_seal_idx on feather.delivery (tenant_id, seal_seq, route)",
    "create index delivery_version_idx on feather.delivery (tenant_id, version_id)",
    """create table feather.delivery_attempt (
        tenant_id text not null,
        attempt_id bigint not null default nextval('feather.event_seq'),
        route text not null,
        version_id text not null,
        from_status text not null,
        to_status text not null,
        actor text not null,
        reason text,
        error text,
        rows_applied int not null,
        rows_rejected int not null,
        code_sha text,
        at timestamptz not null,
        primary key (tenant_id, attempt_id, at)
    ) partition by range (at)""",
    """create index delivery_attempt_idx on feather.delivery_attempt
        (tenant_id, route, version_id, attempt_id)""",
    """create table feather.quarantine (
        tenant_id text not null,
        quarantine_id bigint not null default nextval('feather.event_seq'),
        route text,
        version_id text,
        key text not null,
        dataset text,
        partition text,
        error text not null,
        status text not null default 'open',
        updated_at timestamptz not null default now(),
        primary key (tenant_id, quarantine_id),
        unique nulls not distinct (tenant_id, route, version_id, key, dataset)
    )""",
    "create index quarantine_open_idx on feather.quarantine (tenant_id, key) where status = 'open'",
    """create table feather.inbox (
        tenant_id text not null,
        inbox_id bigint not null default nextval('feather.event_seq'),
        day date not null,
        dataset text not null,
        received_at timestamptz not null,
        payload jsonb not null,
        signature_ok boolean not null,
        primary key (tenant_id, inbox_id, day)
    ) partition by range (day)""",
    """create table feather.override (
        tenant_id text not null,
        target text not null,
        key text not null,
        value jsonb not null,
        reason text,
        created_by text not null,
        created_at timestamptz not null default now(),
        expires_at timestamptz,
        primary key (tenant_id, target, key)
    )""",
    """create table feather.budget (
        tenant_id text not null,
        name text not null,
        reserve jsonb not null default '{}',
        primary key (tenant_id, name)
    )""",
    """create table feather.budget_window (
        tenant_id text not null,
        name text not null,
        idx int not null,
        units int not null,
        seconds int not null,
        tokens numeric not null,
        refilled_at timestamptz not null,
        primary key (tenant_id, name, idx)
    )""",
    """create table feather.worker (
        tenant_id text not null,
        worker_id text not null,
        labels jsonb not null default '{}',
        code_sha text,
        slots int not null default 1,
        last_seen timestamptz not null,
        state text not null,
        primary key (tenant_id, worker_id)
    )""",
    # Hot tables (design 7.4): small and updated constantly. Vacuum them early, and leave room for HOT updates.
    """alter table feather.delivery set (
        fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_threshold = 50,
        autovacuum_analyze_scale_factor = 0.02
    )""",
    """alter table feather.budget_window set (
        fillfactor = 50, autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 50,
        autovacuum_analyze_scale_factor = 0.05
    )""",
]


def upgrade() -> None:
    for statement in _STATEMENTS:
        op.execute(statement)

# A restore drill

Restore Feather's database to a point in time, and time it against the recovery time objective: **RTO of 1 hour or
less** (NFR-04). Run the drill before each release, and after any change to how backups are taken.

## The scripted drill

```bash
uv run python deploy/restore_drill.py
```

It runs in throwaway containers of its own and never touches a database you keep. It:

1. starts a Postgres 17 with WAL archiving on, migrates Feather into it and writes runs;
2. takes a base backup with `pg_basebackup`, then writes more runs, which only the WAL holds;
3. takes the moment to restore to, then makes the mistake: every run is deleted;
4. stops the primary, as if lost, and starts the clock;
5. starts a new Postgres from the base backup, replays the archived WAL up to that moment and promotes it;
6. stops the clock once Feather's store reads every run back, and prints the report as JSON.

It exits 1 if a run is missing, the mistake is still there, or the RTO is over an hour. The nightly deploy job runs it.

## On a real deployment

1. **Before**: Postgres runs with `wal_level=replica`, `archive_mode=on`, an `archive_command` that copies WAL
   somewhere the database's disk can't take with it, and `archive_timeout = 300` or less, so that at most five
   minutes of changes can be lost (RPO, NFR-05). Base backups are taken daily. The master key is backed up apart from
   the database.
2. **Stop Feather**: `docker compose stop feather-server feather-worker`, so nothing writes while you restore.
3. **Restore** the newest base backup from before the moment into a new data directory. Add to its
   `postgresql.auto.conf`:

    ```
    restore_command = 'cp /path/to/wal/%f %p'
    recovery_target_time = '2026-11-12 18:06:32+00'
    recovery_target_action = 'promote'
    ```

    and create an empty `recovery.signal` in it.
4. **Start Postgres** on it and wait until `select pg_is_in_recovery()` answers `f`.
5. **Migrate and start**: `feather server migrate`, then the server and the workers. They pick up from the restored
   state: runs and deliveries that were in flight are recovered as after a crash.
6. **Check**: the Overview, the newest runs, and that the secrets open (with the master key of that time). Write the
   time it took in the release checklist.

"""The restore drill (M3 plan 1 P2, NFR-04): restore Feather's database to a point in time, timed against RTO <= 1 h.

    uv run python deploy/restore_drill.py            # prints the report as JSON; exit 1 if the drill failed
    uv run python deploy/restore_drill.py --keep     # leaves the containers and volumes, to look at them

It runs everything in throwaway containers of its own (`feather-drill-*`), never on a database you keep:

1. A primary Postgres 17 with WAL archiving on, as production should run (`archive_mode`, `archive_command`).
2. Feather's schema is migrated into it, and runs are written: the data of before the backup.
3. A base backup (`pg_basebackup`), then more runs: the data of after the backup, which only the WAL holds.
4. The moment to restore to is taken, and then the mistake: every run is deleted.
5. The primary is stopped, as if lost. The clock starts.
6. A new Postgres starts from the base backup, replays the archived WAL up to that moment and is promoted.
7. The clock stops once Feather's store reads the restored database: every run of before and after the backup is
   there, and the mistake isn't. That time is the RTO measured.

The runbook (docs/06-release/runbooks/restore-drill.md) says how to do the same on a real deployment.
"""

import argparse
import json
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime
from typing import Any

PRIMARY = "feather-drill-primary"
RESTORED = "feather-drill-restored"
VOLUMES = ("feather-drill-backup", "feather-drill-wal")
IMAGE = "postgres:17"
RTO_TARGET_S = 3600.0
PASSWORD = "drill-" + uuid.uuid4().hex[:12]  # the containers are the drill's alone, and bind 127.0.0.1


def docker(*args: str, timeout: float = 300, check: bool = True) -> str:
    done = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, check=False)
    if check and done.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args[:3])}: {done.stderr.strip() or done.stdout.strip()}")
    return done.stdout.strip()


def psql(container: str, query: str) -> str:
    return docker(
        "exec", "-u", "postgres", container, "psql", "-U", "feather", "-d", "feather", "-tAc", query
    )


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(container: str, timeout: float = 300) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ready = subprocess.run(
            [
                "docker",
                "exec",
                "-u",
                "postgres",
                container,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-U",
                "feather",
                "-d",
                "feather",
            ],
            capture_output=True,
            check=False,
        )
        if ready.returncode == 0:
            return
        time.sleep(0.5)
    raise RuntimeError(
        f"{container} didn't become ready: {docker('logs', '--tail', '40', container, check=False)}"
    )


def wait_promoted(container: str, timeout: float = 3600) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if psql(container, "select pg_is_in_recovery()") == "f":
                return
        except RuntimeError:
            pass  # still starting
        time.sleep(0.5)
    raise RuntimeError(
        f"{container} wasn't promoted: {docker('logs', '--tail', '40', container, check=False)}"
    )


def clean() -> None:
    for name in (PRIMARY, RESTORED):
        docker("rm", "-f", name, check=False)
    for volume in VOLUMES:
        docker("volume", "rm", "-f", volume, check=False)


def start_primary(port: int) -> str:
    docker(
        "run", "-d", "--name", PRIMARY,
        "-p", f"127.0.0.1:{port}:5432",
        "-e", "POSTGRES_USER=feather", "-e", f"POSTGRES_PASSWORD={PASSWORD}", "-e", "POSTGRES_DB=feather",
        "-v", f"{VOLUMES[0]}:/backup", "-v", f"{VOLUMES[1]}:/wal",
        IMAGE,
        "postgres", "-c", "wal_level=replica", "-c", "archive_mode=on", "-c", "archive_timeout=5",
        "-c", "archive_command=test ! -f /wal/%f && cp %p /wal/%f",
    )  # fmt: skip
    docker("exec", PRIMARY, "chown", "postgres:postgres", "/backup", "/wal")
    wait_ready(PRIMARY)  # on TCP: the image's first start runs a server on the socket only, then this one
    return f"postgresql://feather:{PASSWORD}@127.0.0.1:{port}/feather"


def write_runs(url: str, label: str, count: int) -> set[str]:
    from feather_connect.storage.postgres.connect import connect

    store, _ = connect(url)  # the spool shares the store's pool, which closes with it
    try:
        return {store.runs.create_run(f"drill-{label}", "manual", "drill", {}) for _ in range(count)}
    finally:
        store.close()


def archived_up_to_now(container: str, timeout: float = 120) -> None:
    """Switch to a new WAL file and wait until the one holding what was just written is archived."""
    wal = psql(container, "select pg_walfile_name(pg_switch_wal())")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        last = psql(container, "select coalesce(last_archived_wal, '') from pg_stat_archiver")
        if last and last >= wal:
            return
        time.sleep(0.5)
    raise RuntimeError(f"WAL {wal} wasn't archived")


def drill(keep: bool = False) -> dict[str, Any]:
    from feather_connect.storage.postgres.connect import connect
    from feather_connect.storage.postgres.migrate import migrate

    clean()
    report: dict[str, Any] = {"started_at": datetime.now().astimezone().isoformat(), "image": IMAGE}
    try:
        url = start_primary(free_port())
        for _ in range(60):
            try:
                migrate(url)
                break
            except Exception:  # noqa: BLE001  # the image's first-start server goes away once; try again
                time.sleep(1)
        else:
            migrate(url)
        before = write_runs(url, "before-backup", 50)

        started = time.monotonic()
        docker(
            "exec",
            "-u",
            "postgres",
            PRIMARY,
            "pg_basebackup",
            "-U",
            "feather",
            "-D",
            "/backup/base",
            "-X",
            "fetch",
            "-c",
            "fast",
        )
        report["backup_s"] = round(time.monotonic() - started, 1)

        after = write_runs(url, "after-backup", 50)
        archived_up_to_now(PRIMARY)
        target = psql(PRIMARY, "select now()")
        time.sleep(1.5)  # so the mistake is clearly after the target
        psql(PRIMARY, "truncate feather.run cascade")  # the mistake
        archived_up_to_now(PRIMARY)
        report["target_time"] = target

        # Recovery settings in the backup: replay the archive up to the target, then promote.
        settings = (
            "restore_command = 'cp /wal/%f %p'\n"
            f"recovery_target_time = '{target}'\n"
            "recovery_target_action = 'promote'\n"
        )
        docker("exec", "-u", "postgres", PRIMARY, "sh", "-c",
               f"printf %s \"{settings}\" >> /backup/base/postgresql.auto.conf && touch /backup/base/recovery.signal")  # fmt: skip
        docker("stop", PRIMARY)  # lost

        clock = time.monotonic()
        port = free_port()
        docker(
            "run", "-d", "--name", RESTORED,
            "-p", f"127.0.0.1:{port}:5432",
            "-e", "PGDATA=/backup/base", "-e", f"POSTGRES_PASSWORD={PASSWORD}",
            "-v", f"{VOLUMES[0]}:/backup", "-v", f"{VOLUMES[1]}:/wal",
            IMAGE,
        )  # fmt: skip
        wait_promoted(RESTORED)
        restored = f"postgresql://feather:{PASSWORD}@127.0.0.1:{port}/feather"
        store, _ = connect(restored)
        try:
            found = {run.run_id for run in store.runs.list_runs(limit=1000)}
        finally:
            store.close()
        rto = time.monotonic() - clock

        report.update(
            rto_s=round(rto, 1),
            rto_target_s=RTO_TARGET_S,
            runs_before_backup=len(before & found),
            runs_after_backup=len(after & found),
            runs_expected=len(before) + len(after),
            mistake_undone=found >= before | after,
        )
        report["ok"] = bool(report["mistake_undone"]) and rto <= RTO_TARGET_S
        return report
    except Exception as error:  # noqa: BLE001  # any failure is the drill's finding, reported
        report.update(ok=False, error=str(error))
        return report
    finally:
        if not keep:
            clean()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--keep", action="store_true", help="leave the drill's containers and volumes")
    args = parser.parse_args(argv)
    report = drill(keep=args.keep)
    print(json.dumps(report, indent=2))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())

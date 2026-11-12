# Chaos and recovery results

M3 plan 1 P2: NFR-04 (durability) and NFR-05 (recovery). The chaos suite runs nightly and on every pull request that
touches the product (CI's `chaos` job); the restore drill runs nightly in the `deploy` job. This page records a run of
each, and the release records its own in the release checklist.

## The chaos suite

Each test acts when a connector says it is at a known line, never after a sleep, and checks that every route holds
every key once (`max_writes_per_key == 1`) and that the chunk it interrupted was really attempted again.

| Test | What happens | Holds |
|---|---|---|
| `test_kill_mid_apply` (5 tests) | kill -9 mid-apply, with and without a restart (the sweeper hands the task on); a worker wrongly declared dead; a kill during extract; **Postgres restarted mid-apply** | Nothing lost or duplicated; one clean version |
| `test_kill_mid_push` (2 tests, the seal at two points) | kill -9 mid-seal and mid-apply of webhook versions | Nothing lost or duplicated |
| `test_overlap_and_budgets` (3 tests) | a recovered extract waits for the stopped one; reads that wait for a budget | The run succeeds; a read too big for its budget fails naming it |
| `test_package_switch` (2 tests) | kill -9 of a package host mid-switch, and of the supervisor | No host left behind; each task runs once |
| `test_server_restart_and_slow_network` (new) | **the server killed and restarted mid-task**; **a slow network to Postgres** (a TCP proxy delaying every chunk 2 ms each way) through a kill -9 mid-apply | NFR-05: at most one retry; nothing lost or duplicated on the slow network |

Run on 13 Nov 2026 on the development machine (Windows 11, Postgres 17 in Docker): the two new tests and the Postgres
restart passed, in 109 s together. The rest of the suite passed on CI for PR 30, and runs again for this release.

## The restore drill

`deploy/restore_drill.py`: a primary with WAL archiving, Feather migrated into it, 50 runs written, a base backup, 50
more runs that only the WAL holds, the moment to restore to, then the mistake (every run deleted); the primary
stopped; a new Postgres from the backup, replaying the archive to that moment and promoted; done when Feather's store
reads every run back.

| When | Machine | Base backup | Restored and readable (RTO) | Runs back | Mistake undone |
|---|---|---|---|---|---|
| 12 Nov 2026 | Windows 11 laptop, Docker Desktop, `postgres:17` | 1.6 s | **2.4 s** (target ≤ 3,600 s) | 100 of 100 | Yes |

The drill's database is small, so its RTO is mostly the time Postgres takes to start and replay. On a real
deployment, RTO grows with the base backup's size and the WAL since it; a daily base backup keeps the replay to a day
at most. The release checklist records the drill on the release commit.

RPO: with `archive_timeout = 300`, at most five minutes of changes are lost (NFR-05's RPO); the restore runbook says
so.

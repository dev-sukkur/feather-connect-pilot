# Release checklist: v1.0.0

M3 plan 1 L2. One line per NFR (discovery §8) with its evidence, then the release's own steps. A line is ticked when its
evidence is in hand for the commit being released. Updated 13 Nov 2026, on branch `feat/m3-release`.

**State**: **on hold** (sponsor, 13 Nov 2026). v1.0.0 is not released yet: Feather stays private and is deployed and used by its own team first. The build is done and merged (PRs 32 and 33). release-please runs only when started by hand, and its 1.0.0 pull request (PR 34) was closed unmerged. Neither PyPI nor GitHub Pages is needed for private use: flow packages go through the server's own registry, and the docs can be read in the repository or built with `uv run mkdocs build`. When the release is back on: put release-please's push trigger back, settle the items marked **open**, and rename `feather-http`, a name already taken on PyPI.

## The NFRs

| NFR | Evidence | Where | State |
|---|---|---|---|
| 01 Latency | Webhook to destination p95 ≤ 5 s at 50 events/s; a 10× burst loses nothing and recovers | CI `perf` job (nightly and on PRs), `docs/06-release/perf-results.md` | Measured on CI each night; the release's numbers go in perf-results |
| 02 Throughput | ≥ 20,000 rows/s per worker from source batch to spool, after a warm-up | CI `perf` job; perf-results | Measured; see perf-results |
| 03 Capacity | 5 million records a day on the *small* deployment, as a sustained run that must finish in a twelfth of a day | CI `perf` job, nightly (`FEATHER_PERF_CAPACITY=1`); perf-results | Measured; see perf-results |
| 04 Durability | The chaos suite: kill -9 mid-apply, mid-extract, mid-seal and mid-push, a wrongly declared dead worker, a host killed mid-switch, Postgres restarted mid-apply, and a slow network to Postgres through a kill | CI `chaos` job, nightly and on PRs; `docs/06-release/chaos-results.md` | Passes; see chaos-results |
| 05 Recovery | A server restart costs a running task at most one retry; the restore drill brings the database back to a point in time within RTO 1 h; WAL archived every ≤ 5 min for RPO 5 min | `tests/chaos/test_server_restart_and_slow_network.py`; `deploy/restore_drill.py` (nightly in the deploy job); the restore runbook | Passes; the drill's RTO is in chaos-results. **At release**: run the drill once more and record it below |
| 06 Runnability | A clean install to the demo in the dashboard in ≤ 5 min; Compose ≤ 4 containers | `tests/deploy` checks the containers; the install is timed by hand | **At release**: time it by hand and record it below |
| 07 Footprint | Postgres ≥ 15 without extensions is the only state | Architecture: no extension is created by any migration (checked: none) | Met |
| 08 Maintainability | pyright, ruff, coverage ≥ 85% over the SDK, engine, server and CLI | CI `check` and `postgres` jobs | Met on every PR |
| 09 Compatibility | Semver from 1.0 by release-please; migrations forward-only (Alembic, expand then contract) | `release-please-config.json`, `.github/workflows/release-please.yml` | Ready; release-please opens the 1.0.0 pull request once this is on main |
| 10 Security | No known critical or high vulnerability; SBOM and signed images; the security review | CI `security` job and Trivy in `deploy`; `release.yml`; `docs/06-release/security-review.md` | Scans clean. **Open**: the sponsor's written acceptance of review findings X2-3 and X2-5 to X2-11 |
| 11 Usability | An operator finds, understands and retries or replays a failed delivery in ≤ 5 clicks | `tests/dashboard/test_operator_journeys.py` walks it; a timed walk-through by hand | Journeys pass in CI. **At release**: the walk-through by hand |
| 12 Portability | linux/amd64 and arm64 images; CI on Linux and Windows | `release.yml`; CI `check` matrix | Met |
| 13 Accessibility | axe's WCAG 2.1 AA checks on the core screens and the M2 pages | `tests/dashboard/test_accessibility.py` | Met for 27 pages, in light and dark. The M3 reason page and the Holds page's requests table have no axe run yet, as the dashboard tests' stack isn't protected |
| 14 Documentation | The docs site: guides, runbooks, reference pages from the code, examples run in CI | `site-docs/`, `.github/workflows/docs.yml`, `tests/docs` | Builds strictly, examples pass. **Open**: GitHub Pages for the repository (`FEATHER_DOCS_PAGES`) |
| 15 Cost | One VM, no paid licence | Architecture: Compose on one machine; every dependency is open source | Met |

## The release

| Step | State |
|---|---|
| The PyPI names and the GitHub organisation for the images (discovery Q9) | **Open** (sponsor) |
| A trusted publisher on PyPI for each of the 12 packages: this repository, `publish.yml`, environment `pypi`; then the repository variable `FEATHER_PYPI=true` | **Open**, after Q9 |
| The repository variable `FEATHER_IMAGE_ORG`, once Q9 names the organisation | **Open**, after Q9 |
| GitHub Pages set to deploy from Actions, and `FEATHER_DOCS_PAGES=true` | **Open** (sponsor) |
| The restore drill run on the release commit, its RTO recorded | At release |
| The clean install timed (NFR-06) and the operator walk-through (NFR-11) | At release |
| The upgrade from the last release: v0.1.0's database migrated to the release, its flows running on | At release: `feather server migrate` on a copy of qa's database |
| All CI green on the release commit: check, postgres, engine conformance, chaos, perf, dashboard, deploy, security, docs | At release |
| Merge release-please's 1.0.0 pull request: it tags v1.0.0, makes the GitHub release, and calls the image and PyPI workflows | At release, with the sponsor's go-ahead |
| Remove `release-as: 1.0.0` from `release-please-config.json` afterwards, so later versions follow the commits | After the release |
| The M3 exit report, `docs/06-release/m3-exit-report.md`, after Q1 and Q2 | After Q1 and Q2 |

## Recorded at release

| What | Number | When, on what |
|---|---|---|
| Restore drill RTO | | |
| Clean install to the demo in the dashboard | | |
| Operator walk-through, clicks to retry a failed delivery | | |

# Flow packages: install, activate and deactivate flows without a restart

- **Status:** Accepted on 2026-11-08 (ADR-0011)
- **Date:** 2026-11-08
- **Decision record:** [ADR-0011](adr/0011-flow-packages-hot-install.md)
- **Milestone:** M1.1, brought forward from M2 together with readiness ([milestones](../04-plan/milestones-and-scope.md)). The two roles it needs are pulled forward from M2's RBAC (§11)
- **Sponsor's answers (8 Nov 2026):** brought forward; a package registry from the start, built into `feather-server`; running runs finish on deactivate; names always carry the package prefix; packages are the only way to deploy flows, and the M1 project mode is retired

## 1. The problem

In M1 an environment runs **one project**, loaded when a process starts:

- `feather-server` calls `load_project` at start-up. Its scheduler, webhook ingress and dashboard know only the flows loaded then.
- Each `feather-worker` loads the same project at start-up and registers its flows in the catalogue (design §6.11).
- In Compose the project is one folder mounted at `/project`, or baked into a flow image.

So adding, changing or removing a flow means changing that folder and restarting the server and every worker. Three things follow:

1. **A restart for every change.** One new flow restarts everything, including flows that didn't change.
2. **One unit of release.** All flows ship together. A team can't release its own flow on its own schedule, and a bad flow blocks the others.
3. **No on and off switch.** A flow can be paused, which records skipped runs, but it can't be taken out of service without removing it from the code and restarting.

The customer's code is already separate from Feather Connect's source: Feather Connect installs as Python packages and images, and a project is the customer's own repository. What's missing is a way to **release flows on their own, into a running environment, and switch each one on and off.**

## 2. What the customer gets

| Need | How it works |
|---|---|
| Keep flows apart from Feather Connect | A flow package is a Python wheel built from the customer's own repository with `feather package build` and published to Feather's built-in registry. Feather Connect upgrades on its own schedule. A manifest states which Feather Connect versions a package works with |
| Install a flow without a restart | `feather package install acme-invoices==1.4.0 --env prod` from the built-in registry, or **Packages → Install** on the dashboard. Nothing restarts |
| Choose what runs | Installed flows start **inactive**. Each flow has **Activate** and **Deactivate** on the dashboard, the CLI and the API |
| Upgrade one flow | Install version 1.5.0 and activate it. Version 1.4.0 finishes its running tasks and stops. Other packages aren't touched |
| Roll back | Activate 1.4.0 again. Old versions stay installed until removed |
| Separate teams | Each package has its own version, dependencies and owner. A package that fails to load doesn't affect the others |

## 3. The design in one picture

```text
 customer repo                 Feather Connect environment (no restart)
 ─────────────                 ─────────────────────────────────────────────────────────────
 feather.yaml       build      ┌───────────────┐  install   ┌──────────────────────────────┐
 flows/*.py   ───────────────▶ │ wheel in a    │ ─────────▶ │ Postgres: package, version,  │
 tests/, env/                  │ registry      │  (CLI/API/ │ wheel, activation, config    │
                               └───────────────┘   UI)      └──────────────┬───────────────┘
                                                                           │ NOTIFY / 2 s poll
                     ┌─────────────────────────────────────────────────────┼─────────────────────┐
                     ▼                                                     ▼                     ▼
          feather-server                                   feather-worker (supervisor)    feather-worker …
          scheduler + ingress + dashboard                  ├─ host: acme-invoices 1.4.0   (same, on each
          read flow specs from the catalogue,              │   own venv, own process      replica)
          never import package code                        └─ host: partner-orders 2.0.1
```

Three changes make it work:

1. **The package is the unit of release.** An environment holds many installed packages instead of one project folder.
2. **Workers become supervisors.** A worker container runs one child process, a *package host*, per active package version. Activating a version starts its host; deactivating drains it. The container never restarts.
3. **The server stops importing flow code.** The scheduler, ingress and dashboard work from the flow specs that hosts register in the catalogue, which they already do in M1, so a new package is picked up without restarting the server.

## 4. The flow package

### 4.1 A flow package is a Python wheel

A package is a standard **Python wheel** with a Feather manifest inside, so every existing Python registry can host it and `uv` can install it with its dependencies. The customer's repository keeps the M1 project layout, plus a `pyproject.toml`. Building and publishing it takes three commands:

```bash
uv run feather test
uv run feather validate --env prod
uv run feather package build                       # → dist/acme_invoices-1.4.0-py3-none-any.whl
uv run feather package publish --url https://feather-qa.acme.com   # uploads to the built-in registry
```

### 4.2 What is inside

| Part | What |
|---|---|
| `acme_invoices/feather-package.json` | The manifest: package name, version, the flows, datasets and routes it defines, the connections it needs with their connector types, the Feather Connect versions it supports, and the build time and commit |
| `acme_invoices/feather.yaml`, `env/*.yaml` | The project as in M1. Env files hold only addresses and secret *references* |
| `acme_invoices/flows/`, `connectors/` | The code: flows, and connectors the package brings with it |
| `acme_invoices/fixtures/`, `tests/` | Kept so `--sim` runs and the install check can use them |
| Wheel metadata | Name, version and **dependencies**, including `feather-connect>=1.0,<2` and any extra libraries. `feather package build` locks them with hashes, so an install resolves exactly what was tested |
| Entry point `feather_connect.packages` | Points Feather at the manifest, as connectors already register through `feather_connect.connectors` (design §8.2) |

The name and version come from `pyproject.toml`. A version installed once can't be replaced: a change needs a new version number, so a version number always means the same code.

### 4.3 The built-in registry

Packages are installed **from a registry**, and Feather Connect has its own. `feather-server` serves it, so there is nothing extra to run, host or pay for:

- **Where.** `https://<feather-server>/registry/simple/`, a standard Python package index (the "simple" index of PEP 503 and the upload API that `uv publish` and `twine` use). Any Python tool can read from it or publish to it.
- **Storage.** Wheels are stored in Feather's Postgres, in the spool's chunk storage, or in object storage when one is configured (ADR-0005). Backups of the database cover the registry too.
- **On by default.** Every environment has the registry `feather`, trusted from the start. `FEATHER_REGISTRY=0` turns it off for a site that uses only its own company registry.
- **Who can publish.** An API token with the `publish` scope, created by an admin, typically one per customer CI pipeline. It can upload new versions and nothing else: it can't install, activate or overwrite a version. A version once published is never replaced, only yanked.
- **Who can read.** Feather itself, and anyone with an API token, for example a developer running `uv pip download` to inspect a release.
- **Across environments.** A customer publishes to qa's registry. `feather package promote` (§6) copies the tested wheel, matched by sha256, into prod's registry and installs it there, so prod never needs a token for qa or a network path to it.

From the customer's CI and an admin's terminal:

```bash
uv run feather package publish --url https://feather-qa.acme.com --token "$FEATHER_PUBLISH_TOKEN"
feather package search invoices --env qa
feather package install acme-invoices==1.4.0 --env qa
```

The dashboard's **Packages → Install** page lists the packages in the environment's registries and each one's versions, so an operator picks one instead of uploading a file. Installing a wheel from a file is still allowed, for an air-gapped site, behind the same checks.

**Other registries, optional.** A customer that already runs a company registry can add it alongside the built-in one. Any registry that speaks the Python package index protocol works: GitLab Package Registry, Azure Artifacts, AWS CodeArtifact, Google Artifact Registry, JFrog Artifactory, Sonatype Nexus, `devpi` or `pypiserver`. Credentials are secret references, never values:

```bash
feather registry add acme https://pkgs.example.com/simple/ --token '${secret:env:ACME_REGISTRY_TOKEN}' --env prod
```

## 5. Installing

`feather package install acme-invoices==1.4.0 --env prod`, `POST /api/v1/packages` or **Packages → Install** runs these steps and stops at the first one that fails:

1. **Fetch and check the wheel** from a trusted registry: size (50 MiB by default), the registry's sha256, the manifest schema, a name and version not installed before, and Feather Connect version compatibility.
2. **Store it** in Postgres: the wheel goes in the spool's chunk storage, or object storage when configured (ADR-0005), and its rows go in the catalogue tables (§9). Hosts later load this stored copy, so a registry outage or a deleted release never stops a running environment.
3. **Prepare its environment.** Resolve its locked dependencies with `uv` into a virtual environment keyed by the lock's hash, on each worker's cache volume. Two packages with the same lock share one. Built in the background, it shows on the package's page as *preparing*, then *ready*.
4. **Load-check it** in a throwaway child process: import the flows, and validate them against the environment's configuration and secrets, as `feather validate --env prod` does.
5. **Record it** as *installed*, with every flow *inactive*, and write an audit row.

Nothing runs until a flow is activated.

## 6. Activating and deactivating

Each flow in an installed package version has a state in this environment:

```text
            install                activate                deactivate
 (none) ──────────────▶ inactive ─────────────▶ active ───────────────▶ inactive
                           │  ▲                    │
                           │  └──── load failed ◀──┘ (stays on the old version, shows the error)
                           └─ uninstall (only when no version of it is active)
```

| Action | What happens | Within |
|---|---|---|
| **Activate** a flow | The state row changes and a NOTIFY goes out. Each worker supervisor makes sure a host is running for that package version, and that host registers its flows. The scheduler starts firing the flow's triggers, and ingress starts accepting its webhooks | 30 s with a ready environment (test target) |
| **Deactivate** a flow | Its triggers stop, and its webhooks answer `404`, as for an unknown dataset. **Runs in progress finish**, including their deliveries and retries. Its history, versions, ledger and failures stay, and remain visible. To stop a run now, cancel it on its own page, as today | 5 s for triggers |
| **Switch version** (activate 1.5.0 while 1.4.0 is active) | Hosts for 1.5.0 start and register. Then the flow's triggers move to 1.5.0 in one transaction, and the 1.4.0 hosts drain: they stop taking work and finish their current tasks within the 30 s grace (design §6.11). Tasks are tagged with their version, so a 1.4.0 task never runs on 1.5.0 code | No run lost or doubled (chaos test) |
| **Uninstall** a version | Allowed only when none of its flows is active. Its stored wheel and environment are deleted; its history stays | — |

**Deactivate isn't Pause.** Pause keeps a flow in service but skips its runs and records each skipped run, for maintenance. Deactivate takes the flow out of service. Both stay available.

A flow is activated **per environment**. The same package version can be active in qa and inactive in prod. `feather package promote acme-invoices 1.4.0 --from qa --to prod` installs in prod exactly the wheel that passed in qa, matched by sha256, even if the registry has since been changed. This is the "promotion" that design §5.8 plans for M2.

## 7. How code loads without a restart

### 7.1 Package hosts

A `feather-worker` container runs a **supervisor** that doesn't import any flow code. It:

- watches the activation table (LISTEN/NOTIFY, plus a poll every 2 seconds as a fallback);
- starts one **package host** per active package version: a child process running the existing worker loop (`run_worker`) in that package's environment, on its stored wheel;
- labels each host with its package and version, so tasks reach only the host whose code they were started with;
- stops a host with SIGTERM, which drains it as any worker drains today, when its version is no longer active;
- restarts a host that crashes, with back-off, and reports a crash loop on the package's page;
- heartbeats as a worker of its own, labelled `role: supervisor` with no slots, so the fleet view shows it before any package is active; its hosts register as `<worker>/<package>@<version>`.

**As built (P4–P6):**

- A version is **prepared** before it can be activated: the supervisor builds its environment and load-checks it there, and records it `ready` or `failed`. An install leaves it `preparing`; activating a version that isn't ready is refused (409).
- **Switching** follows §6: the new version's host starts and says `ready` on its pipe, and only then are the flows' triggers moved to it. The old host keeps running while runs tagged with its version are queued or running, so they finish on the code they started with.
- **Tasks follow their run's version.** Every task carries the pool of the version its run is tagged with, whichever process submits it: a host, or the server when an operator retries, replays or skips a delivery. A plan or extract also holds an advisory lock on its concurrency key, so two versions never read one partition at once.
- **Push datasets are sealed on their host.** The server's ingress verifies and keeps an event; the host of the active version seals the dataset's inbox, since only its code can parse the events.
- **Environments** are built with `uv` from the package's lock, binary wheels only, one per lock hash, shared between versions with the same lock. Unused ones are removed every 10 minutes.
- Under `feather dev`, the supervisor gets a working folder (`--workdir`, the project folder): hosts run in it, and a relative `${secret:file:…}` is read against it, so a project's relative paths keep their meaning.

Each host is today's worker, so retries, checkpoints, heartbeats and the sweeper work unchanged. A host that dies hands its tasks on, exactly as a killed worker does in M1.

**Why a process per package, not a reload inside one process.** Python can't reliably unload a module: old classes, globals and threads stay alive, and two packages can need different versions of one library. A child process gives a clean load, its own dependencies and crash isolation, and its end frees everything.

**Cost.** About 60–90 MB of memory per active package version on each worker replica. An environment with 20 active packages needs about 2 GB per replica. Hosts are started only for active versions, and a worker can be limited to some packages with labels (`--packages acme-*`) to spread them out.

### 7.2 The server works from specs

`feather-server` stops calling `load_project`. Instead:

| Part | Reads | Notes |
|---|---|---|
| Scheduler | The triggers of active flows, from the catalogue | It refreshes on NOTIFY. A flow activated at 10:00:05 fires at its next tick |
| Webhook ingress | Webhooks a package declares in `feather.yaml`, and push datasets of active flows whose source is Feather Connect's own | It verifies the signature with a built-in scheme (HMAC, Shopify, Stripe, Zoho, bearer), keeps each event once by its key, and puts it in the push dataset's inbox. The package's own code parses it, on the active version's host. A package that brings its own webhook source as code is refused at install (§12) |
| Dashboard and API | The catalogue, as today | Adds the Packages pages (§10). A packaged dataset or route is described from its flow's catalogue spec, which names things but holds no code: its partitions show by key, not by label, and its write cost is unknown |
| Budgets | Defined at environment level (§8) | As today |

## 8. Connections, budgets and names across packages

Several packages share one environment, so three things are settled at environment level:

- **Shared connections and budgets.** The environment keeps a configuration of its own, outside any package: connections that many packages use, such as `zakya` or `erp`, and budgets such as `zoho-api` (`90/minute`, `1400/day`). The same budget then limits every package's calls, as it does across flows today. It is edited on **Settings → Environment** or with `feather env set`. Secrets stay references (`${secret:env:…}`, `${secret:file:…}`), never values.
- **A flow's schedule here.** An environment may also keep a `flow` entry, named `<package>/<flow>`, that replaces the flow's clock triggers (`cron` with `tz`, `every`, `manual`) or turns it off (`enabled: false`), with no new package version (added in M3.1, FR-G4). The server's scheduler applies it to the catalogue's spec within seconds; a package host applies it as the `flows:` overlay when it starts, so a poller's interval changes then. An entry for a flow the version no longer has, or one that isn't valid, is left out with a warning.
- **A package's own connections.** A package may also define connections of its own in its `feather.yaml` and `env/<env>.yaml`. An environment-level setting with the same name overrides the package's, and the dashboard shows which layer set each value, as **effective config** does today.
- **Names carry the package prefix.** Every flow, dataset and route a package defines is named `<package>/<name>` in the environment: `acme-invoices/invoices`, `acme-invoices/erp`. Two packages can then use the same short names without a clash, and every run, delivery, failure and audit row says which package it belongs to. The prefix appears everywhere a name does:

  | Where | Example |
  |---|---|
  | Dashboard | **Flows** groups flows by package; a flow's page is titled `acme-invoices/invoices` |
  | CLI | `feather runs --flow acme-invoices/invoices`, `feather replay acme-invoices/erp --partition …` |
  | API | `POST /api/v1/flows/acme-invoices%2Finvoices:run` |
  | Webhooks | `POST /ingress/default/acme-invoices/partner-orders` |

  Inside its own code a package keeps using short names (`fc.to(http.Call("erp", …))`): Feather adds the prefix when the package is loaded. Environment-level connections and budgets (above) have no prefix, because they are shared on purpose.

## 9. Storage

New tables in the `feather` schema. Every table carries `tenant_id`, as all tables do from M1.

| Table | Holds | Key columns |
|---|---|---|
| `package` | One row per package name | name, owner, created_at |
| `registry` | The registries this environment trusts | name, index_url, token_ref (a secret reference), added_by, added_at |
| `package_version` | One row per installed version | name, version, sha256, registry, manifest (jsonb), wheel location, size, fc_versions, lock_hash, state (installed, preparing, ready, failed, uninstalled), installed_by, installed_at |
| `package_env` | Per worker and lock hash, whether the environment is built | worker_id, lock_hash, state, error, built_at |
| `flow_activation` | Which version of each flow is active in this environment | flow, package, version, state (inactive, activating, active, deactivating, failed), changed_by, changed_at, reason, error |
| `env_config` | Environment-level connections and budgets | key, value (jsonb), layer, changed_by, changed_at |

The catalogue tables from M1 (`project`, `flow`, `dataset`, `route`) gain a `package_version` column. Each install, activation, deactivation, promotion and uninstall writes an audit row with who, what and why.

## 10. Dashboard, CLI and API

**Dashboard.** A new **Packages** section in the sidebar:

- **Packages:** each package, its versions, which flows are active in which version, and its state (preparing, ready, failed, with the error).
- **Install:** search the environment's registries, pick a package and version, and install. The page shows the install checks as they pass. Uploading a wheel file is the fallback for air-gapped sites.
- **Registries:** add, test and remove the registries this environment trusts.
- **A package version:** its manifest, flows, connections needed and whether each is configured, its hosts on each worker, and Activate and Deactivate per flow with a preview of the impact.
- **A flow's page** shows its package and version, with Activate and Deactivate next to Pause.

**CLI.**

```bash
feather package build
feather package publish --url https://feather-qa.acme.com --token "$FEATHER_PUBLISH_TOKEN"
feather registry add acme https://pkgs.example.com/simple/ --token '${secret:env:ACME_REGISTRY_TOKEN}' --env prod
feather package search invoices --env prod
feather package install acme-invoices==1.4.0 --env prod
feather package list --env prod
feather flow activate acme-invoices/invoices --version 1.4.0 --env prod --reason "go live"
feather flow deactivate acme-invoices/invoices --env prod --reason "partner paused"
feather package promote acme-invoices 1.4.0 --from qa --to prod
feather package uninstall acme-invoices 1.3.0 --env prod
```

**API.** `GET /api/v1/registries`, `POST /registries`, `GET /registries/{r}/packages?q=`, `POST /packages` (name and version from a registry, or a wheel upload), `GET /packages`, `GET /packages/{name}/versions/{v}`, `POST /flows/{flow}:activate` (with `version`), `POST /flows/{flow}:deactivate`, `DELETE /packages/{name}/versions/{v}`. As with every M1 action, each one takes a reason and an `Idempotency-Key`, and has a preview where it affects running work.

## 11. Security

Installing a package runs its code on the workers. So:

- **Who may install.** Only the *admin* role may add registries, install, promote or uninstall. Activating and deactivating need the *operator* role. Because this work comes before the rest of M2, these two roles are pulled forward from M2's RBAC (design §9): users and API tokens get a role, `admin` or `operator`, and the rest of RBAC follows in M2. Existing users become `admin`, so nothing changes for them.
- **What is installed.** Only from registries the environment trusts. The sha256 is recorded and checked on every load. Optionally, a signing key per environment: a package must be signed with it (minisign or Sigstore), checked at install.
- **Where it runs.** Package hosts run as the worker's unprivileged user, in the worker container. There is no sandbox inside it. Packages from parties you don't trust belong in a separate environment.
- **What a host can reach** (amended 11 Nov 2026, as built in P4). A host is a process tree of its own (a Job Object on Windows), killed with its supervisor, and it gets less than the worker has:
  - **No secret of the worker's.** Its environment holds only what Python needs and the variables named in `FEATHER_HOST_ENV_PASS`; never the master key, the session key or the worker's database URL.
  - **Its own connections' credentials only.** It asks the supervisor for each over a pipe; the supervisor resolves the reference with the environment's vault and checks it against the version's manifest and configuration, so a host gets only the secrets of connections its package declares.
  - **A database role of its own**, `feather_host` (`FEATHER_HOST_DATABASE_URL`): the run, task, version and delivery tables it needs, and not the secrets, users or tokens tables. Without it, hosts connect as the worker, which can read every table, and the supervisor warns.
  - **The network**, as the worker has. Limiting it is left to the deployment (§12).
- **The install-time load check** runs the package in a child process with an empty environment and a temporary home folder, so code that reads a variable or a file when imported must have a default.
- **Secrets.** As today, packages hold only references. Values come from the environment or, encrypted, from the database, and logs mask them.

## 12. Limits of this design

- A package can't bring its own **webhook source connector**: the server, which doesn't load package code, verifies every event. It declares its webhooks instead (§7.2), and its own code parses them on its host. Its own polling sources and sinks are fine.
- **What the server knows of a packaged flow** is its catalogue spec: no partition labels and no write cost. Work that needs the package's code goes to the active version's host as a run: a **backfill** is queued by its days (`params.backfill`), and the host names their partitions as it plans; a **partition check** is a `verify` run, which the host answers on the run (`feather.run.result`, migration 0027) while the server waits up to 60 seconds. A backfill's preview counts days, not partitions.
- **Hosts have the worker's network.** Egress rules belong to the deployment.
- **A tick of a packaged flow is a run of its own**, queued for the version's host, where an M1 poller attached its ticks to one stream run a day. A frequent poller writes many runs; bringing back the stream run is an M2 item.
- The **dashboard can't show a package's transforms** as code. It shows the spec, as today.
- **Old Python versions:** every package runs on the worker image's Python (3.13 today).
- `feather dev` installs the project folder as the next dev version on each save (§13).

## 13. Compatibility with M1

Packages are the **only** way to deploy flows. The M1 project mode, where a deployment mounts `/project` or bakes it into a flow image, is retired, so there is one way to release, one code path in the server and workers, and nothing to choose per environment.

- **Deployments.** `deploy/compose.yaml` drops the `/project` mount and `FEATHER_PROJECT_DIR`. The server and workers start with no flows and run what is installed and active.
- **Existing projects.** A project becomes a package with `feather package build` once `feather.yaml` has a `version:`, and it gets a package name.
- **Upgrading a running environment.** The upgrade builds the mounted project into a package, installs it and activates the flows that were running, before the mount is removed. Names gain the package prefix, and the migration renames the existing datasets, routes and watermarks to match, so no history, watermark or delivery is lost.
- **Local work.** `feather dev` installs the project folder as `<version>.dev<N>` on start and on each save, activates its flows, and keeps the last three dev versions. A save that doesn't load leaves the running version alone.

**As built (P6):** `feather server upgrade --project <dir>` does the upgrade in one transaction. It installs the project as a package, renames runs, schedules, datasets and deliveries to `<package>/<name>`, records each old name in `feather.renamed`, and refuses while an M1 worker is alive. Running it twice changes nothing. Old names keep working: API calls redirect with 308, and dashboard pages with 302 or 307. `FEATHER_PROJECT` is ignored with a warning.

## 14. Plan

| # | Task | Proves |
|---|---|---|
| 0 | Roles `admin` and `operator` on users and tokens, pulled forward from M2 | An operator can't install; existing users are admins |
| 1 | Manifest schema, the wheel layout and entry point, `feather package build` with locked dependencies, reproducible wheels | Building twice gives the same sha256 |
| 2 | The built-in registry: the simple index and upload API at `/registry/simple/`, wheels in Postgres, `publish`-scoped tokens, yanking; `feather package publish`, search, fetch; then `feather registry add` for other indexes | `uv publish` and `feather package publish` upload to it, `uv pip install` reads from it, a version can't be overwritten, and an install from a company index works with a token from a secret reference |
| 3 | Migration for §9's tables; store and read wheels | Round trip; refusing a version installed before |
| 4 | Install pipeline (§5) with the load check in a child process; the CLI and API | Each check's failure is named; nothing runs after install |
| 5a | Environment-level config (§8); the package prefix on every name, applied at load | Two packages with the same short flow name run side by side; a shared budget limits both together |
| 5 | Server from specs: scheduler, ingress and dashboard read the catalogue, refresh on NOTIFY; the server and workers no longer call `load_project` | A flow activated while the server runs fires on time, with no restart |
| 6 | Worker supervisor and package hosts; labels per package version; drain on deactivate | Activate in ≤ 30 s, deactivate stops triggers in ≤ 5 s |
| 7 | Version switch and rollback | Chaos test: `kill -9` of a host during a switch loses and duplicates nothing |
| 8 | Dependency environments with `uv`, cached by lock hash | Two packages needing different versions of one library run side by side |
| 9 | Dashboard Packages pages, Activate and Deactivate on the flow page; accessibility checks | axe at 0 violations, light and dark |
| 10 | Promotion qa → prod by sha256; uninstall | The wheel in prod is byte for byte the one tested in qa |
| 11 | Optional signing; Operating Guide section | An unsigned package is refused where signing is required |
| 12 | Retire the project mode: the upgrade that turns a mounted project into its first package and renames to prefixed names; drop the `/project` mount from Compose | An M1 environment upgrades with its history, watermarks and deliveries intact, and no flow misses a run |

**Exit criteria.** In a running Compose environment, with no container restarted:

- a customer installs a package built in another repository, activates one of its flows, and sees it run;
- they upgrade it to a new version, roll it back, and deactivate it;
- another package keeps running throughout, with no run lost or doubled.

## 15. Decisions

| # | Question | Decision (8 Nov 2026) |
|---|---|---|
| 1 | Milestone | Brought forward, ahead of the rest of M2. Two roles come forward with it (§11) |
| 2 | Package source | The registry built into `feather-server`, on by default. Company registries can be added beside it (§4.3) |
| 3 | Running runs on deactivate | They finish (§6) |
| 4 | Names | Always prefixed with the package name (§8) |
| 5 | Keep the M1 project mode for small deployments, or move every environment to packages | Packages only. The project mode is retired, and running environments upgrade in place (§13) |

## 16. Settled while building (P1–P6, 9–11 Nov 2026)

| Question | As built |
|---|---|
| Connection settings and secrets | Settings that aren't secret are plain values in the database, visible and editable, each change audited. Secrets are encrypted with a master key per environment, kept outside the database, and never shown again once set |
| Webhooks in packages | Declared in `feather.yaml` and verified by the server's built-in schemes; the package's code parses (§7.2) |
| Install states | `preparing`, then `ready` or `failed` once a supervisor has built and load-checked the version (§7.1) |
| Promotion | `feather package promote` exports a wheel from one environment and imports it into another by sha256; there is no server-to-server path |
| Uninstall | Keeps the version's row as `uninstalled`; deletes the wheel unless another version shares it. An uninstalled version can't be installed again |
| Signing | ed25519 over the wheel's `RECORD`, with the signature in `.dist-info/feather-signature.json`. The settings `trusted_package_keys` and `require_signed_packages`; the `signature` check comes after `not_yanked` |
| Packaged flow pages | `/packages/<package>/flows/<flow>` has Activate, Switch, Rollback and Deactivate; the Flows list shows packaged flows beside any others |
| Hosts | No master key; credentials over a pipe; a `feather_host` database role (§11) |

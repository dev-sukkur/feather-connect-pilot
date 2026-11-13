# Feather Connect on Railway

A private deployment for the team's own use (sponsor, 13 Nov 2026: v1.0.0 waits; Feather runs privately first). It
replaces `compose.yaml` with three Railway services in one project, built from this repository on every change to
`main`:

| Service | What it is | Built from |
|---|---|---|
| `Postgres` | Railway's PostgreSQL, with its volume. The only state (NFR-07). | Railway's database template |
| `feather-server` | Dashboard, JSON API, webhook ingress and scheduler, on a public `https://` domain. Runs the migrations before each deploy. | `deploy/Dockerfile.server`, configured by `deploy/railway/server.json` |
| `feather-worker` | Runs the flows' tasks. No port. One or more replicas. | `deploy/Dockerfile.worker`, configured by `deploy/railway/worker.json` |

The two JSON files are Railway's config-as-code: the builder, the Dockerfile, which paths start a new deploy, the
pre-deploy migration, the health check and the restart policy. Everything else (variables, domain, replicas,
backups) is set in Railway, as below. No secret is in the repository.

## Before you start

- A Railway account on a paid plan (services that run all the time, and volume backups), with Railway's GitHub app
  allowed to read this repository. The repository can be private.
- On your machine: the Feather CLI (`uv sync` in this repository) and Railway's CLI (`npm i -g @railway/cli`, then
  `railway login`), for the first admin user and for checking.
- Two keys, made once and kept in your password manager, **apart from the database's backups**:

  ```bash
  uv run python -c "import secrets; print(secrets.token_urlsafe(48))"
  ```

  ```bash
  uv run feather secret new-key
  ```

  The first is `FEATHER_SECRET_KEY` (signs sessions and confirmations). The second is `FEATHER_MASTER_KEY` (seals
  the connections' secrets in the database). Losing the master key loses every stored secret; a restored database
  is no use without it.

## Set it up

1. **Project and database.** New project → *Database* → *PostgreSQL*. Keep the service's name `Postgres`: the
   variables below refer to it by name.

2. **Shared variables.** Project → *Settings* → *Shared Variables*, in the `production` environment. Seal the two
   keys (*Seal* on the variable's menu), so no one can read them back from the dashboard:

   | Variable | Value |
   |---|---|
   | `FEATHER_SECRET_KEY` | the first key |
   | `FEATHER_MASTER_KEY` | the second key |
   | `FEATHER_ENV` | `prod` |
   | `FEATHER_LOG_LEVEL` | `INFO` |

3. **The server.** *New* → *GitHub Repo* → this repository. Name the service `feather-server`. In its *Settings*:
   - *Config-as-code* → *Railway Config File*: `/deploy/railway/server.json` (a path from the repository's root,
     with the leading slash). Leave *Root Directory* empty: the images are built from the whole repository.
   - *Source* → branch `main`, and turn on *Wait for CI*, so a commit deploys only once its GitHub checks pass.
   - *Networking* → *Generate Domain*, port `8080`. Or add your own domain there and its DNS record.

   Its *Variables*:

   | Variable | Value | Why |
   |---|---|---|
   | `FEATHER_DATABASE_URL` | `${{Postgres.DATABASE_URL}}` | Postgres over the private network |
   | `FEATHER_SECRET_KEY`, `FEATHER_MASTER_KEY`, `FEATHER_ENV`, `FEATHER_LOG_LEVEL` | `${{shared.FEATHER_SECRET_KEY}}` and so on | The shared ones |
   | `FEATHER_BASE_URL` | `https://${{RAILWAY_PUBLIC_DOMAIN}}`, or your own domain | Links, and a `Secure` session cookie |
   | `PORT` | `8080` | The port Railway's health check and domain use |
   | `FORWARDED_ALLOW_IPS` | `*` | Uvicorn takes the client's address from Railway's proxy. Only the proxy reaches the server |

4. **The worker.** *New* → *GitHub Repo* → this repository again. Name it `feather-worker`. *Railway Config File*:
   `/deploy/railway/worker.json`; branch `main` with *Wait for CI*; **no** domain. Its variables are
   `FEATHER_DATABASE_URL` (`${{Postgres.DATABASE_URL}}`) and the four shared ones. *Settings* → *Deploy* → *Replicas*
   sets how many run (start with 1). Every replica shares the same variables.

5. **Deploy.** *Deploy* the staged changes. The server's pre-deploy step runs `feather server migrate`; the server
   goes live once `/readyz` answers 200 (the database is reachable and its schema is current). A worker migrates
   too when it starts, and an advisory lock lets only one migration run at a time, so the order doesn't matter.

6. **The first admin.** With the CLI, from this repository:

   ```bash
   railway link
   ```

   ```bash
   railway ssh --service feather-server
   ```

   Then, inside the server's container, run `feather server create-user admin` and type the password at the prompt.
   The first user is an admin. Then `exit`.

7. **A role for package hosts.** A flow package's code runs in a package host, which by default connects to
   Postgres as the worker does, and could read every table, the sealed secrets, users and tokens included (the
   worker logs a warning saying so). Give hosts a role of their own. On the `Postgres` service → *Data* → *Query*
   (or `railway connect Postgres`), with a new random password:

   ```sql
   create role feather_host login password '<a new password>';
   ```

   Then add to the **worker**'s variables, and let it redeploy:

   | Variable | Value |
   |---|---|
   | `FEATHER_HOST_DATABASE_URL` | `postgresql://feather_host:<that password>@${{Postgres.PGHOST}}:${{Postgres.PGPORT}}/${{Postgres.PGDATABASE}}` |

   Each time the worker starts, after the migrations, it grants this role what running flows need and nothing that
   holds a credential: no `secret`, `secret_key`, `app_user`, `api_token` or `registry`, and only `insert` on `audit`.
   Seal the variable.

8. **Check it.** Open the domain and sign in. `https://<domain>/readyz` shows the database and schema as ok; the
   overview shows the worker's heartbeat.

## Your flows

Flows reach the deployment as packages, kept in its Postgres, as with Compose: build and publish a flow package to the
server's own registry, then install it (`feather package publish`, `feather package install`, with
`--server https://<domain>` and a token from `feather server create-token`). Connections and their secrets are set
with `feather config import` and `feather secret set` against the same server. See the docs site's *Write a flow*
and *Deploy* pages (`site-docs/`).

The images also hold the demo project (`examples/demo`), as on Compose. It has no schedule, so it does nothing in
`prod` unless someone runs it.

## Backups and restore

- **Railway's backups.** On the `Postgres` service, *Backups* → set a schedule (daily, and keep weekly ones). These
  are volume snapshots: restoring one brings the whole database back to that moment, from the same tab.
- **What that gives.** RPO is the time since the last snapshot, up to a day with daily backups, not the 5 minutes of
  NFR-05, which needs WAL archiving (`site-docs/runbooks/restore-drill.md`). That is accepted for private use; add WAL
  archiving to object storage before production work depends on it.
- **The master key** is not in any backup. Keep it in the password manager. A restore needs the same
  `FEATHER_MASTER_KEY`.
- **Try a restore** once, before relying on it: restore a snapshot into a second environment (below) and sign in there.

## Updates and rollback

- **Every merge to `main` deploys**, once CI passes, as long as it touches the paths in the JSON files'
  `watchPatterns`. A docs-only change doesn't deploy.
- **Migrations are forward-only** (expand, then contract), so the version before is meant to run on the new schema.
  To roll back, *Redeploy* an earlier deployment from the service's *Deployments* list. Its pre-deploy step runs
  `migrate`, which finds nothing to do.
- **Change a key**: the secret key by setting a new value (everyone signs in again); the master key by the rotation
  in the Operating Guide (`FEATHER_MASTER_KEY_PREVIOUS` first, on both services).

## A test environment

When you want qa: project → *Environments* → *New environment*, duplicating `production`. It gets its own Postgres,
its own private network and its own variables. Give it **its own** `FEATHER_SECRET_KEY` and `FEATHER_MASTER_KEY`, set
`FEATHER_ENV=qa`, and point it at a branch of its own rather than `main`, so it can run a change before production
does.

## Checked before the first deploy

On 13 Nov 2026, both images were built from this branch and run as Railway runs them, against a scratch Postgres 17:
the pre-deploy command with the image's entrypoint replaced (`feather server migrate`, every migration to 0033); the
server with `FEATHER_ENV=prod` and `PORT=8080`, answering `/readyz` with 200 to the health check's `Host:
healthcheck.railway.app`; a worker on an empty database (it migrates and starts); a worker with
`FEATHER_HOST_DATABASE_URL` (the host role could read `run` and not `secret`); and `feather server create-user admin`
inside the server's container. Railway itself (the config files, *Wait for CI*, the domain, backups) is checked at
the first deploy.

Railway's `DATABASE_URL` connects as its `postgres` superuser. Feather needs only to own its schema; for private use
that is accepted, and the host role above keeps flow code away from credentials.

## How it differs from Compose

| | Compose (`compose.yaml`) | Railway |
|---|---|---|
| Migrations | the one-shot `migrate` container | the server's pre-deploy command |
| TLS | a proxy you put in front | Railway's domain |
| Workers | `FEATHER_WORKERS` replicas | the worker service's replicas |
| Backups | your own, with WAL archiving for 5-minute RPO | Railway's snapshots; RPO up to a day |
| Images | built on the machine, or from ghcr.io | built by Railway from this repository |

# Deploying Feather Connect

| File | What it is |
|---|---|
| `Dockerfile.server` | The server image: dashboard, JSON API, ingress and scheduler. `feather server`. |
| `Dockerfile.worker` | The worker image, which a flow image is built `FROM`. `feather worker`. |
| `compose.yaml` | Postgres, a one-shot `migrate`, the server and the workers, on one machine. |
| `.env.example` | The settings `compose.yaml` reads. Copy it to `.env`. |
| `compose.nonprod.yaml` | The test environments qa and uat on one machine: one Postgres, a database each (layout B). |
| `nonprod-init.sh`, `nonprod.env.example`, `qa.env.example`, `uat.env.example` | What `compose.nonprod.yaml` needs. |
| `compose.test.yaml` | The throwaway Postgres of the test suite (not for data you keep). |

## Run it

```bash
cp deploy/.env.example deploy/.env       # set FEATHER_SECRET_KEY and POSTGRES_PASSWORD
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml exec feather-server feather server create-user admin
```

Then open `http://127.0.0.1:8080`. Compose refuses to start without `FEATHER_SECRET_KEY` and `POSTGRES_PASSWORD`.

- **Containers.** `postgres`, `feather-server` and `feather-worker` run; `migrate` runs once and exits. That is four
  containers in all (NFR-06), and Postgres is the only state, in the `pgdata` volume (NFR-07).
- **More workers.** `FEATHER_WORKERS=3 docker compose -f deploy/compose.yaml up -d` runs three replicas.
- **Your flows.** Set `FEATHER_PROJECT_DIR` to your project, or build a flow image:

  ```dockerfile
  FROM ghcr.io/feather-connect/feather-worker:dev
  COPY --chown=feather:feather . /project
  ```

- **Ports** bind to `127.0.0.1`. Put a TLS proxy in front to reach the dashboard from elsewhere, and set
  `FEATHER_BASE_URL` to its `https://` address, so the session cookie is `Secure`.
- **Images** are `ghcr.io/<org>/feather-server` and `feather-worker`, with `<org>` from `FEATHER_IMAGE_ORG` until
  discovery Q9 names it. Releases build them for linux/amd64 and linux/arm64. They hold no secrets.
- **Profiles** `objects`, `sso` and `observability` are kept for M2 and M3, and add nothing yet.

## Test environments: qa and uat on one machine

`compose.nonprod.yaml` runs two test environments side by side (layout B, ADR-0013 decision 3): one Postgres server
with a database each, `feather_qa` and `feather_uat`, and a migrate, a server and a worker for each environment.
There is no dev environment: developers use `feather dev` on their own machine. The test environments are used now
and then, to try a change before production, and are not watched all the time.

```bash
cp deploy/nonprod.env.example deploy/nonprod.env   # the Postgres password and the two database users' passwords
cp deploy/qa.env.example deploy/qa.env             # qa's own FEATHER_SECRET_KEY, and its user's password in the URL
cp deploy/uat.env.example deploy/uat.env           # the same for uat, with a different key
docker compose -f deploy/compose.nonprod.yaml --env-file deploy/nonprod.env up -d --build
docker compose -f deploy/compose.nonprod.yaml --env-file deploy/nonprod.env exec qa-server feather server create-user admin
docker compose -f deploy/compose.nonprod.yaml --env-file deploy/nonprod.env exec uat-server feather server create-user admin
```

qa is then at `http://127.0.0.1:8180` and uat at `http://127.0.0.1:8280` (`QA_PORT`, `UAT_PORT`).

- **Kept apart.** Each environment has its own database, owned by its own user, and `nonprod-init.sh` takes `connect`
  away from everyone else, so qa's user can't open uat's database or the other way round. Each has its own `.env`, so
  its own secret key: a secret set in qa can't be read in uat. Users, tokens, packages and audit are each
  environment's own.
- **Shared.** The Postgres server, its memory and disk, and its backups. A heavy run in qa slows uat. That is the
  trade for one machine; give uat its own deployment with `compose.yaml` when it must not be.
- **Protected.** Make uat protected, so every change needs a reason and compensating or skipping deliveries waits for
  a second person: put `settings: {protected: true}` in an environment file and
  `feather config import uat.yaml --env uat --server http://127.0.0.1:8280 --reason "uat is protected"`.
- **Package host roles** are the server's, not a database's. If you give a package host its own role
  (`FEATHER_HOST_DATABASE_URL`), name it per environment, `feather_qa_host` and `feather_uat_host`, never one for both.
- **Seven containers**: Postgres, two migrates that run once, two servers and two workers.
- **The init script runs once**, on an empty `pgdata` volume. To change a password later, use `alter role` in
  Postgres and the `.env` files together.

### Production stays apart

Production never runs here. It has its own deployment with `compose.yaml`, on its own machine or at least its own
Postgres server, with its own secret key, backups and people. No test environment holds a production secret or
reaches production's database: test environments use a Zakya test organisation and a SAP test client.

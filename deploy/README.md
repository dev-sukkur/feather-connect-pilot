# Deploying Feather Connect

| File | What it is |
|---|---|
| `Dockerfile.server` | The server image: dashboard, JSON API, ingress and scheduler. `feather server`. |
| `Dockerfile.worker` | The worker image, which a flow image is built `FROM`. `feather worker`. |
| `compose.yaml` | Postgres, a one-shot `migrate`, the server and the workers, on one machine. |
| `.env.example` | The settings `compose.yaml` reads. Copy it to `.env`. |
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

# Deploy

Feather runs as a server and workers on Postgres 17, its only database. The repository's `deploy/` has what you need:

| File | What it is |
|---|---|
| `compose.yaml` | One environment on one machine: Postgres, a one-shot migrate, the server and the workers |
| `compose.nonprod.yaml` | The test environments qa and uat on one machine: one Postgres, with a database each |
| `railway/` | Railway instead of Compose: the server and the worker as two services on Railway's Postgres, set up step by step in `deploy/railway/README.md` |
| `Dockerfile.server`, `Dockerfile.worker` | The images, also published, signed, as `ghcr.io/<org>/feather-server` and `feather-worker` |

```bash
cp deploy/.env.example deploy/.env       # set FEATHER_SECRET_KEY and POSTGRES_PASSWORD
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml exec feather-server feather server create-user admin
```

- **Ports** bind to 127.0.0.1. Put a TLS proxy in front, and set `FEATHER_BASE_URL` to its `https://` address so the
  session cookie is `Secure`.
- **Production** runs on its own deployment, with its own Postgres, key and backups, and is marked protected. Test
  environments never hold a production secret.
- **Backups**: run Postgres with WAL archiving, and rehearse a restore, as the [restore drill](runbooks/restore-drill.md)
  does.
- **Verify an image** before you run it, with `cosign verify` as the [security page](security.md) shows.

The repository's `deploy/README.md` has the details.

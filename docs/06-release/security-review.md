# Security review for v1.0.0

M3 plan 1 X2. A review of the control plane against design §13 and OWASP ASVS 4.0 level 2, on branch
`feat/m3-release`, 13 Nov 2026. It was done by reading the code, by Bandit, pip-audit and Trivy (X1, now in CI), and
by a test that walks every route (`packages/feather-server/tests/test_every_route.py`).

**Verdict.** No finding is high or critical. Four were fixed here. Seven are accepted, each with its reason below, and
**each accepted finding needs the sponsor's written acceptance** before v1.0.0 (plan §2, X2's exit). That is in the
ledger.

## Findings

| # | Finding | Severity | State |
|---|---|---|---|
| X2-1 | `feather server` listened on 0.0.0.0 unless told otherwise (an M1 open item) | Medium | **Fixed**: 127.0.0.1 by default. The image says `--host 0.0.0.0` itself and binds to 127.0.0.1 on the host through Compose |
| X2-2 | The worker image held the demo's secret file, `examples/demo/secrets/erp-token.txt` (an M1 open item). Its value is fake | Low | **Fixed**: `.dockerignore` keeps every `examples/*/secrets` out of the images; `tests/deploy` checks no secret is in an image |
| X2-3 | A viewer's change to an unknown object, or with a body that fails validation, is refused 404, 422 or 400 before the capability check is reached, so the refusal names the problem rather than the role | Low | **Accepted**: nothing is done either way, and a viewer can already read what a 404 tells them. A viewer never gets a mass action's confirmation token, as issuing one checks the capability. The every-route test holds this |
| X2-4 | Bandit B324: SHA-1 in `feather dev` | Low | **Fixed**: it names the files on disk to notice an edit, and now says `usedforsecurity=False` |
| X2-5 | Bandit B608 (×40, medium): SQL built with f-strings | Low | **Accepted**: every case puts in Feather's own table, column or schema names, from constants or names validated against `[a-z0-9_]` (`system_schema`); values always go as parameters. In the DuckDB connector, the SQL is the flow author's own query, from their own code |
| X2-6 | Bandit B301: pickle in the local engine | Low | **Accepted**: the local engine is for `feather run --local` and the tests, and only unpickles what it pickled itself, in the same process. Production runs DBOS |
| X2-7 | Bandit B314: `xml.etree` parses `feather test`'s JUnit report | Low | **Accepted**: the file is the one pytest just wrote in the same command, on the developer's machine |
| X2-8 | The server speaks HTTP and sets no HSTS | Low | **Accepted**: TLS ends at the proxy in front, as the Deploying README says; HSTS is that proxy's header. The session cookie is `Secure` once `FEATHER_BASE_URL` is `https://` |
| X2-9 | The sign-in throttle is per process | Low | **Accepted**: with N server replicas, an attacker gets N times the tries. Argon2id makes each try slow, and the small deployment runs one server |
| X2-10 | Sessions last 12 hours with no idle timeout | Low | **Accepted** for v1: a sign-out or a new password ends every session of the user at once (the session epoch), and a role change counts from the next request |
| X2-11 | No multi-factor sign-in | Info | **Accepted**: SSO, which brings the identity provider's MFA, is in M9. Until then, keep the dashboard behind the organisation's VPN or proxy auth |

## ASVS level 2, by chapter

| Chapter | What was checked | Result |
|---|---|---|
| V1 Architecture | Design §13; the trust boundaries: browser, API client, ingress sender, package host, Postgres | Holds. A package host connects as `feather_host`, which can't read secrets, users, tokens or registries, and can only insert into audit |
| V2 Authentication | Passwords hashed with Argon2id and a new salt; failed sign-ins throttled per user; no default user or password; the first user made on the command line | Pass, with X2-9 and X2-11 |
| V3 Sessions | A signed cookie, `HttpOnly`, `SameSite=Lax`, `Secure` behind https; 12 h; an epoch in the cookie ends old sessions on sign-out or a new password; a role is read again on every request | Pass, with X2-10 |
| V4 Access control | A capability per action, checked in the service the API, the dashboard and the CLI share; four roles plus the grants `approver` and `pii-reader`; protected environments (reasons, a second person, viewer tokens); every route walked by a test, anonymous, as a viewer and as an operator | Pass, with X2-3 |
| V5 Input | Pydantic models on every API body; forms checked in the service; CSRF tokens on every change of the dashboard and the cookie-carrying API (a test lists the routes without, which are ingress only); ingress bodies capped (413); Jinja autoescaping; a CSP of `'self'` with no inline script | Pass |
| V6 Cryptography | Secrets sealed with AES-256-GCM and the tenant, environment and name as associated data, under a key that rotates (`feather secret rotate-key`); API tokens kept only as SHA-256 of a 256-bit random value; webhook signatures checked with HMAC and `compare_digest` | Pass |
| V7 Errors and logging | Errors as RFC 9457 problems with no stack trace; secret values registered and redacted from logs; every change and every refusal audited with actor, target and reason; the host role can't change the audit | Pass |
| V8 Data | PII fields masked unless the `pii-reader` grant; secrets never shown again once set; the audit and packages tables out of the host role's reach | Pass |
| V9 Communications | TLS at the proxy; outbound connectors verify TLS by default | Pass, with X2-8 |
| V10 Malicious code | Packages signed and verified on install when the registry says so; a package runs in its own host process as `feather_host`; CI pins every action by SHA | Pass |
| V12 Files | Uploads only to the built-in registry, as wheels of a size cap, by a publish token | Pass |
| V13 API | Bearer tokens with a role and a scope; idempotency keys on changes; the OpenAPI document is the contract | Pass |
| V14 Configuration | Bandit, pip-audit and Trivy on every PR; SBOM and keyless signing on release; CODEOWNERS on auth, access, secrets, migrations, CI and deploy; the images hold no secret; the server binds 127.0.0.1 by default | Pass |

## The scans on this branch

- **Bandit 1.8.6** over `packages/`: one high (X2-4, fixed), 54 medium (X2-5 to X2-7, accepted). CI fails on high.
- **pip-audit 2.9.0** over `uv.lock`, every extra: no known vulnerability.
- **Trivy** over both images: runs in the deploy job, failing on a critical or high vulnerability that has a fix.
  `.trivyignore` is empty.

## How to repeat it

```bash
uvx --from "bandit[toml]==1.8.6" bandit -c pyproject.toml -r packages -ll
uv export --frozen --all-extras --no-emit-workspace --no-hashes -o requirements-audit.txt
uvx pip-audit==2.9.0 -r requirements-audit.txt --strict
uv run pytest packages/feather-server/tests/test_every_route.py packages/feather-server/tests/test_protected.py -q
```

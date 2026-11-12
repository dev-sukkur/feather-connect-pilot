# Contributing to Feather Connect

Thank you for helping. Feather Connect is MIT licensed, and contributions come in under the same licence.

## Sign off your commits (DCO)

Every commit says that you wrote it, or have the right to contribute it, under the
[Developer Certificate of Origin 1.1](https://developercertificate.org/). Say so with a `Signed-off-by` line, which
`git commit -s` adds:

```
Signed-off-by: Your Name <you@example.com>
```

CI refuses a pull request from a fork whose commits aren't all signed off. To sign off the commits already on your branch:
`git rebase --signoff main`.

## Before you start

- **A bug**: open an issue with what you did, what happened and what you expected, and the version
  (`feather --version`). A security issue goes privately through the Security tab instead: see `SECURITY.md`.
- **A feature or a connector**: open an issue first and say what it is for. A connector is a package of its own under
  `packages/connectors/`, with a README whose example the tests run.

## Make the change

```bash
uv sync --all-extras
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest packages/feather-connect/tests -m "not postgres"
docker compose -f deploy/compose.test.yaml up -d         # the test Postgres, on 127.0.0.1:25433
uv run pytest packages tests/templates tests/demo tests/e2e tests/dev
```

- **Tests first**: a bug fix comes with the test that failed before it; a feature with tests of what it promises.
- **Commit messages** are [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`,
  `test:`, `chore:`), since release-please writes the changelog and the next version from them. A breaking change
  says `!` (`feat!:`) and has a `BREAKING CHANGE:` footer.
- **Docs** change with the code: the Operating Guide for what an operator sees, the site's pages under `site-docs/`
  (`uv run pytest tests/docs` builds them and runs their examples), and a connector's or template's README.
- **One pull request** per change, from a branch. CI runs the checks, the tests on Postgres, the dashboard in
  Chromium with axe's accessibility checks, the security scans, and, when they apply, the chaos, performance and
  deploy checks.

## Review

Changes to authentication, access control, secrets, migrations, CI and deployment need the owner's review
(`.github/CODEOWNERS`). Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).

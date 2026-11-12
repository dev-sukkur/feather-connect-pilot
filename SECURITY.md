# Security

## Reporting a vulnerability

Please report a vulnerability privately, through GitHub's **Report a vulnerability** button on this repository's
Security tab, not in a public issue. Say what you found, how to reproduce it and which version it affects. We answer
within five working days, and agree a date to publish the fix and the advisory with you.

## Supported versions

| Version | Security fixes |
|---|---|
| 1.x | Yes, the latest minor release |
| Before 1.0 | No |

## What we do

- **Every pull request** is scanned: Bandit over the code (high severity fails), pip-audit over the locked
  dependencies, and Trivy over both images (any critical or high vulnerability with a fix fails). Accepted findings are
  in `.trivyignore` and `docs/06-release/security-review.md`, each with why.
- **Releases** sign the images keyless with cosign and attach a CycloneDX SBOM to each, as an attestation and on the
  GitHub release. Verify an image as `.github/workflows/release.yml` shows.
- **Changes** to authentication, access control, secrets, migrations, CI and deployment need the owner's review
  (`.github/CODEOWNERS`).
- **The design**: the control plane's security is design §13; the v1 review against OWASP ASVS level 2 is
  `docs/06-release/security-review.md`.

## Running it safely

- The server listens on 127.0.0.1 unless told otherwise. Put a TLS proxy in front of it, and set `FEATHER_BASE_URL`
  to its `https://` address so the session cookie is `Secure`.
- Keep `FEATHER_SECRET_KEY` out of images and source control; each environment has its own.
- Mark production protected (`settings: {protected: true}`): every change then needs a reason, and compensating or
  skipping deliveries needs a second person.

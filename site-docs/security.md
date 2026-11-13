# Security

- **Report a vulnerability** privately, through the repository's Security tab (**Report a vulnerability**), not in a
  public issue.
- **The v1 review** against OWASP ASVS level 2, with every finding and its state, is in the repository at
  `docs/06-release/security-review.md`.
- **Scans**: Bandit, pip-audit and Trivy run on every pull request.

## Verify an image

Each released image is signed keyless with cosign and carries a CycloneDX SBOM as an attestation:

```bash
cosign verify ghcr.io/<org>/feather-server:v1.0.0 \
  --certificate-identity-regexp 'https://github.com/.+/.github/workflows/release.yml@refs/tags/v.+' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
cosign verify-attestation --type cyclonedx ghcr.io/<org>/feather-server:v1.0.0 \
  --certificate-identity-regexp 'https://github.com/.+/.github/workflows/release.yml@refs/tags/v.+' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Run it safely

- Keep the server on 127.0.0.1 behind a TLS proxy. Each environment has its own `FEATHER_SECRET_KEY`.
- Mark production protected (`settings: {protected: true}`). Every change then needs a reason, tokens are viewers
  unless a role is asked for, and compensating or skipping deliveries waits for a second person.
- Give package hosts their own database role (`feather_host`), which can't read secrets, users or tokens.

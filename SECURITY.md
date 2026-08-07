# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| 0.3.x | Yes — current Production Snapshot |
| 0.2.x | Best-effort until 0.3.x is widely adopted |
| < 0.2 | No |

## Threat model (honest)

MPREG 0.3.0 targets **crash-fault tolerant (CFT)** clusters operated by
**trusted operators** who control peer membership and secrets.

| In scope | Out of scope (non-claims) |
| --- | --- |
| Accidental misconfig footguns (open metrics, placeholder secrets) | Byzantine / malicious peers (BFT) |
| Optional TLS on data plane (wss/tcps) | Guaranteed residual-free cache after lost ABORT |
| Monitoring bearer token when mon is exposed | WAN multi-region linearizability / Jepsen-class proof |
| Federated route signatures + gossip HMAC | OAuth2/OIDC IdP product (roadmap) |
| Dependency vulnerability scanning in CI | Formal pen-test certification |
| `mpreg config-check --strict` production gate | SIEM / infinite audit retention |

Raft is **CFT, not BFT**. Cache `STRONG` put is flag-gated majority-commit;
get/delete stay refuse (`1012`). Shared audit is a bounded G-Set epidemic, not SIEM.
See `tests/invariants/claims.yaml` `non_claims` and
`docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md`.

## Reporting a vulnerability

Please report security issues **privately**:

1. Email the maintainer listed in `pyproject.toml` (`authors`), or
2. Use GitHub **Private vulnerability reporting** on the repository if enabled.

Include:

- MPREG version (`import mpreg; mpreg.__version__`)
- Deployment shape (single-node / cluster / federated)
- Profile or relevant settings (redact secrets)
- Reproduction steps and impact assessment

Do **not** open a public issue with exploit details until a fix or advisory is ready.

We aim to acknowledge reports within **7 days** and to ship fixes on a timeline
commensurate with severity for the 0.3.x line.

## Production hardening checklist

Before exposing a node beyond a lab trust domain:

1. Copy a profile from `mpreg/profiles/`; **rotate every `change-me` secret**.
2. Run:
   ```bash
   uv run mpreg config-check path/to/profile.toml --strict --format json
   ```
   Exit code `2` means warnings remain — fix them for production.
3. Set `monitoring_auth_token`; keep `monitoring_enable_cors=false`.
4. Bind monitoring to loopback or a private network (`monitoring_host=127.0.0.1`)
   unless the token is set and the network is trusted.
5. For multi-cluster / untrusted links use `federated.toml` defaults:
   route signatures required, gossip HMAC on, discovery policy as needed.
6. Prefer `wss://` / `tcps://` when the network is not fully private.
7. Scrape `/metrics/prometheus` only with the bearer token.
8. Do not treat residual doctor/monitor fields as automatic heal.

Critical warnings surfaced in `config-check` JSON as `critical_warnings` include
placeholder secrets, CORS enabled on monitoring, non-loopback mon without token,
and `fabric_route_allow_unsigned=true`.

## Dependency security

CI runs `scripts/ci_security_deps.sh` (pip-audit via uvx when available).
Operators should re-run that script in their own supply-chain pipeline.

## Scope disclaimer

This document is **operational guidance**, not a warranty, insurance policy,
or third-party security audit. CI green does not mean “unhackable.”

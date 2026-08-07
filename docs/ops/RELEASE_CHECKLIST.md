# MPREG 0.3.0 / 0.3.1 Release Checklist

| Field | Value |
| --- | --- |
| **Milestone** | Production Snapshot `v0.3.0` |
| **Architecture** | `docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md` |
| **Master plan** | `docs/plans/RELEASE_0_3_PRODUCTION_SNAPSHOT_MASTER_PLAN.md` |
| **Gate** | `bash scripts/release_gate.sh` |

## Pre-tag (automated)

- [ ] `bash scripts/release_gate.sh` exits 0  
- [ ] `uv run pytest tests/release/ -q` green  
- [ ] CI workflow jobs green on the release commit (lint, typecheck, unit-fast,
      invariants, distlab-core, security-deps, demo-smoke, package-smoke)  
- [ ] `pyproject.toml` / `mpreg.__version__` == `0.3.0`  
- [ ] CHANGELOG has `## [0.3.0]` with user-facing notes  
- [ ] `SECURITY.md` present  
- [ ] claims.yaml includes R0.3 release proofs + non_claims  

## Pre-tag (human)

- [ ] README honesty matches claims (no “we ship OAuth2” drift)  
- [ ] Working tree clean except intentional release commits  
- [ ] No secrets in tree (`change-me` only as documented placeholders)  
- [ ] Decide PyPI upload yes/no; if yes, credentials ready  
- [ ] GitHub Release notes drafted from CHANGELOG 0.3.0 section  

## Operator golden path (post-install verification)

```bash
# 1. Install (wheel or editable)
uv pip install mpreg==0.3.0   # or: uv sync

# 2. Copy profile and rotate secrets
cp mpreg/profiles/federated.toml ./my-node.toml
# edit: name, peers, monitoring_auth_token, replace change-me-*

# 3. Strict config gate
uv run mpreg config-check ./my-node.toml --strict --format json

# 4. Start
uv run mpreg server start-config ./my-node.toml

# 5. Scrape metrics (Bearer)
curl -sS -H "Authorization: Bearer $MPREG_MONITORING_TOKEN" \
  "http://127.0.0.1:${MON_PORT}/metrics/prometheus" | head

# 6. Doctor
uv run mpreg doctor --url "http://127.0.0.1:${MON_PORT}"
# If STRONG enabled:
uv run mpreg doctor --strong --format json --url "http://127.0.0.1:${MON_PORT}"

# 7. Alerts
# Load mpreg/ops/prometheus_alerts.yml into your rules controller
```

Residual doctor fields (`abort_fail_peer_count`, `residual_ops_hint`, …) are
**ops visibility**, not automatic heal — see residual honesty docs.

## Tag procedure (human; do not automate force-push)

```bash
git status   # clean
bash scripts/release_gate.sh
git tag -a v0.3.0 -m "MPREG 0.3.0 Production Snapshot"
# git push origin main
# git push origin v0.3.0
```

Optional publish:

```bash
uv build
# uvx twine upload dist/*
```

## Post-tag

- [ ] GitHub Release published  
- [ ] PyPI (if applicable) install-smoke from clean venv  
- [ ] Announce with honesty banner (CFT, flag-gated STRONG/audit)  
- [ ] Open post-0.3.0 backlog (residual T140+ only if product bugs; roadmap OAuth2 etc.)  

## Non-goals frozen at this tag

BFT, WAN SLA, Jepsen/Elle, automatic residual heal, OAuth2/OIDC product,
full self-healing control plane, STRONG get/delete quorum.

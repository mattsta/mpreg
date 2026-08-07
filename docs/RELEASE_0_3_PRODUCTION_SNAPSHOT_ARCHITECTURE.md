# MPREG 0.3.0 — Production Snapshot Architecture

| Field | Value |
| --- | --- |
| **Status** | **Complete** — validated 2026-08-07 |
| **Milestone** | `v0.3.0` Production Snapshot |
| **Authority** | Official release architecture for public upload |
| **Master plan** | `docs/plans/RELEASE_0_3_PRODUCTION_SNAPSHOT_MASTER_PLAN.md` |
| **Burndown** | `docs/plans/RELEASE_0_3_BURNDOWN.md` |
| **Proof ledger** | `docs/plans/RELEASE_0_3_PROOF_LEDGER.md` |
| **Related** | `tests/invariants/claims.yaml`, `docs/PRODUCTION_DEPLOYMENT.md`, residual honesty through T139 |

## 1. Purpose

Ship a **clean, upload-ready public snapshot** that is:

- **Reasonably safe** — default-off dangerous flags; prod profiles warn/fail on placeholders
- **Secure enough for trusted-operator CFT clusters** — monitoring auth, TLS path, dep audit
- **High performance with evidence** — documented lab baselines, not marketing numbers
- **Reliable** — CI gates that actually exercise product code, not only demos
- **Honest** — README / CHANGELOG / claims.yaml tell the same story

This is **not** BFT, WAN multi-region SLA, Jepsen/Elle, automatic residual heal, OAuth2/OIDC productization, or infinite residual polish (T140+ is out of scope for this tag).

## 2. What already ships (do not re-litigate)

| Layer | Reality |
| --- | --- |
| Fabric routing | Path-vector / LS control plane; gossip catalog |
| CFT Raft | Production Raft modules; not BFT |
| Queues / pubsub / cache | Real; EXACTLY_ONCE refuse-by-default `1011` |
| STRONG put MVP | Flag-gated majority-commit put; get/delete `1012` |
| Shared audit | Flag-gated G-Set epidemic |
| Residual ops | doctor/monitor residual fields — **visibility, not heal** |
| DistLab | First-party platform-tests-platform lab |
| Profiles | `mpreg/profiles/*` + `config-check` |
| Ops | Prometheus alerts, SLO doc, PRODUCTION_DEPLOYMENT |

Residual honesty T66–T139 is **complete enough**. This milestone is **release engineering + claim discipline**.

## 3. Target system shape at 0.3.0

```
                    ┌─────────────────────────────────────┐
                    │         Public surfaces              │
                    │  PyPI/wheel · GitHub Release · docs  │
                    └─────────────────┬───────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          │                           │                           │
          ▼                           ▼                           ▼
   Honesty plane              Quality plane                 Safety plane
   README/CHANGELOG           CI matrix                     SECURITY.md
   claims.yaml                ruff/mypy/pytest              config-check prod
   non_claims freeze          distlab-core + demo           mon token / TLS
          │                           │                           │
          └───────────────────────────┼───────────────────────────┘
                                      ▼
                         Operability plane
              profiles · doctor · metrics · PERF_BASELINE
              golden-path runbook · release checklist
```

### 3.1 Trust model (explicit)

| Assumption | In scope |
| --- | --- |
| Operators control peers and secrets | Yes |
| Crash-fault peers (CFT) | Yes |
| Same trust domain or federated.toml hardening | Yes |
| Byzantine / malicious peers | **No** (non_claim) |
| WAN partitions as product SLA | **No** |
| Residual-free after lost ABORT without ops | **No** |

### 3.2 Default posture

| Knob | Default | Prod expectation |
| --- | --- | --- |
| `cache_strong_enabled` | off | opt-in + monitoring |
| `mgmt_audit_shared_enabled` | off | opt-in + path + monitoring |
| `monitoring_auth_token` | unset / placeholder | **required** for exposed mon |
| `monitoring_enable_cors` | false | stay false |
| Federated HMAC / route sigs | profile-dependent | required on untrusted links |
| Placeholder `change-me` secrets | lab | **warn** always; **error** under `--strict` / prod-mode |

## 4. Release tracks (architecture → plan mapping)

| Track | Name | Architectural outcome |
| --- | --- | --- |
| **R1** | CI quality matrix | Every push proves lint, types, fast tests, invariants, DistLab core, dep audit |
| **R2** | Honesty surface | Public docs match claims; no aspirational “we have OAuth2” drift |
| **R3** | Security snapshot | SECURITY.md; config-check prod guards; TLS/mon auth documented + tested |
| **R4** | Packaging | `0.3.0` metadata; wheel install smoke; classifiers/URLs |
| **R5** | Performance evidence | PERF_BASELINE.md + one reproducible smoke (not heroics) |
| **R6** | Ops golden path | Single checklist: config-check → start → scrape → doctor |
| **R7** | Gate + freeze | Full validation; claims/ledger; tag-ready tree |

## 5. CI architecture (R1)

```
push/PR
  ├─ lint          ruff check mpreg tests
  ├─ typecheck     mypy mpreg (scoped)
  ├─ unit-fast     pytest curated paths -m "not slow"
  ├─ invariants    claims-related residual + strong/audit core
  ├─ distlab-core  registry smoke + happy_3 + ci-core preset subset
  ├─ demo-smoke    existing scripts/run_demo_smoke.sh
  ├─ security-deps pip-audit / uv export audit
  └─ (optional nightly) perf-smoke · demo-suite
```

**Principle:** tiered gates. Demo-suite alone is insufficient for a production snapshot.

## 6. Honesty architecture (R2)

Single source of truth hierarchy:

1. `tests/invariants/claims.yaml` — machine-oriented claims + **non_claims**
2. Honesty banners in GETTING_STARTED / PRODUCTION / README
3. CHANGELOG 0.3.0 user-facing notes
4. Roadmap section — **future only**, never mixed into “features we have”

Drift rule: if README asserts a capability not in claims or code → move to roadmap or delete.

## 7. Security architecture (R3)

| Surface | Control |
| --- | --- |
| Metrics / monitoring HTTP | Bearer `monitoring_auth_token` when set |
| Data plane | Optional TLS (wss/tcps); lab may be plain ws |
| Control plane federated | Route signatures + gossip HMAC + discovery policy |
| Config footguns | `config-check` warnings; `--strict` elevates critical to exit≠0 |
| Dependencies | CI audit on lock/export |
| Disclosure | `SECURITY.md` |

Out of scope for 0.3.0: OAuth2/OIDC IdP product, default mTLS mesh, SIEM product.

## 8. Packaging architecture (R4)

- Version: **0.3.0** in `pyproject.toml`
- Build: hatchling wheel + sdist
- Entry points: `mpreg`, `mpreg-example` only (`uv run …`)
- Install smoke: clean venv → `mpreg --help` / `config-check` dev profile
- Python: declare `>=3.11,<4`; CI documents tested version (3.14)

## 9. Performance architecture (R5)

- **Evidence, not slogans.** Remove or rephrase unsubstantiated “Million+ msg/s” as lab-order-of-magnitude only if measured.
- `docs/ops/PERF_BASELINE.md`: hardware class, topology, commands, p50/p95 bands, non-claims (not WAN SLA).
- Optional CI job: short smoke that fails only on catastrophic regression (orders of magnitude), not flaky thresholds.

## 10. Ops golden path (R6)

```
1. Copy profile → rotate secrets
2. uv run mpreg config-check <profile> --strict
3. uv run mpreg server start-config <profile>
4. Scrape GET /metrics/prometheus (Bearer)
5. uv run mpreg doctor --url $MON [--strong|--audit]
6. Alerts: mpreg/ops/prometheus_alerts.yml
```

Document in PRODUCTION_DEPLOYMENT + short RELEASE_CHECKLIST.

## 11. Non-goals freeze (tag surface)

**Do not block 0.3.0 on:**

- Residual honesty T140+
- STRONG get/delete quorum
- Automatic residual heal
- Jepsen/Elle/WAN
- BFT
- OAuth2/OIDC
- Full self-healing control plane
- SWIM/MembershipProtocol as default production membership

## 12. Definition of done (milestone)

1. CI matrix green on main for all R1 jobs  
2. README / CHANGELOG / claims.yaml consistent  
3. SECURITY.md + config-check prod guards tested  
4. Version 0.3.0; wheel install smoke green  
5. PERF_BASELINE.md + one reproducible command  
6. Ops golden path documented and cross-linked  
7. R7 gate script/tests green; tree ready to tag  
8. Working tree clean after commit(s)

## 13. Validation philosophy

| Kind | Tool |
| --- | --- |
| Unit residual / release closeout | `tests/chaos/test_r0_3_*.py` or `tests/release/` |
| Config safety | `tests/test_config_check_cli.py` extensions |
| CI scripts | `scripts/ci_*.sh` invoked by workflow and locally |
| Claims | `claims.yaml` proof list + non_claims for release snapshot |
| Manual | `scripts/release_gate.sh` one-shot |

When a gate fails → **fix product or docs**, then re-run. Do not weaken non_claims to pass.

## 14. Success metric

A stranger can:

1. Install from wheel/docs  
2. Start a profile without silent insecure federated defaults  
3. Understand what is and is not guaranteed  
4. Scrape metrics and run doctor  
5. Trust that CI would have caught obvious breakage  

That is the Production Snapshot.

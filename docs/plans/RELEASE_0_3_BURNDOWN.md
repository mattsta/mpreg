# MPREG 0.3.0 — Unified Sequential Burndown

| Field            | Value                                                       |
| ---------------- | ----------------------------------------------------------- |
| **Status**       | **Complete**                                                |
| **Master**       | `docs/plans/RELEASE_0_3_PRODUCTION_SNAPSHOT_MASTER_PLAN.md` |
| **Architecture** | `docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md`      |
| **Ledger**       | `docs/plans/RELEASE_0_3_PROOF_LEDGER.md`                    |

Linear order. Check off only when validated (test or script green).

## R0 — Planning artifacts

- [x] R0.1 Architecture doc
- [x] R0.2 Master plan
- [x] R0.3 This burndown
- [x] R0.4 Proof ledger skeleton
- [x] R0.5 Index link from docs/ARCHITECTURE or DISTLAB hub

## R1 — CI quality matrix

- [x] R1.01 `scripts/ci_lint.sh`
- [x] R1.02 `scripts/ci_typecheck.sh`
- [x] R1.03 `scripts/ci_unit_fast.sh`
- [x] R1.04 `scripts/ci_invariants.sh`
- [x] R1.05 `scripts/ci_distlab_core.sh`
- [x] R1.06 `scripts/ci_security_deps.sh`
- [x] R1.07 Workflow: lint job
- [x] R1.08 Workflow: typecheck job
- [x] R1.09 Workflow: unit-fast job
- [x] R1.10 Workflow: invariants job
- [x] R1.11 Workflow: distlab-core job
- [x] R1.12 Workflow: security-deps job
- [x] R1.13 Workflow: demo-smoke retained
- [x] R1.14 `tests/release/test_r1_ci_scripts_exist.py` green
- [x] R1.15 Local lint+typecheck+unit-fast smoke

## R2 — Honesty surface

- [x] R2.01 README drift inventory applied
- [x] R2.02 Roadmap-only for OAuth2/OIDC / unshipped items
- [x] R2.03 Throughput claims caveated or removed
- [x] R2.04 CHANGELOG `## [0.3.0]` section
- [x] R2.05 claims.yaml release proofs + non_claims
- [x] R2.06 ARCHITECTURE / GETTING_STARTED links
- [x] R2.07 `tests/release/test_r2_*.py` green
- [x] R2.08 README → SECURITY.md + release architecture

## R3 — Security snapshot

- [x] R3.01 `SECURITY.md`
- [x] R3.02 config-check mon/cors/strict guards
- [x] R3.03 tests for strict + cors + federated placeholder
- [x] R3.04 PRODUCTION + profiles security notes
- [x] R3.05 claims non_claim security doc scope
- [x] R3.06 `tests/release/test_r3_*.py` green

## R4 — Packaging

- [x] R4.01 version 0.3.0
- [x] R4.02 project.urls + classifiers
- [x] R4.03 `scripts/ci_package_smoke.sh`
- [x] R4.04 version test
- [x] R4.05 package smoke green

## R5 — Performance evidence

- [x] R5.01 `docs/ops/PERF_BASELINE.md`
- [x] R5.02 links from PRODUCTION + README
- [x] R5.03 `scripts/ci_perf_smoke.sh` (optional short)
- [x] R5.04 doc existence test

## R6 — Ops golden path

- [x] R6.01 `docs/ops/RELEASE_CHECKLIST.md`
- [x] R6.02 PRODUCTION + mpreg/ops README links
- [x] R6.03 checklist test

## R7 — Gate & freeze

- [x] R7.01 `scripts/release_gate.sh`
- [x] R7.02 ledger complete
- [x] R7.03 burndown all checked
- [x] R7.04 master + architecture status Complete
- [x] R7.05 `uv run pytest tests/release/ -q` green
- [x] R7.06 `bash scripts/release_gate.sh` green
- [x] R7.07 milestone commit(s)

---

## Progress log

| Date       | Note                                                         |
| ---------- | ------------------------------------------------------------ |
| 2026-08-07 | Burndown created; execution started                          |
| 2026-08-07 | All tracks R1–R7 validated; `scripts/release_gate.sh` exit 0 |

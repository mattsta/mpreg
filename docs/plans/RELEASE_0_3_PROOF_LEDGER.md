# MPREG 0.3.0 Production Snapshot — Proof Ledger

Point → artifact → claim mapping for the public release milestone.
Scope is **release engineering + honesty + safety gates**, not WAN/BFT/Jepsen.

| Point                  | Proof path                                                           | Claim                 |
| ---------------------- | -------------------------------------------------------------------- | --------------------- |
| R0 architecture        | `docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md`               | support planning      |
| R0 master plan         | `docs/plans/RELEASE_0_3_PRODUCTION_SNAPSHOT_MASTER_PLAN.md`          | support planning      |
| R0 burndown            | `docs/plans/RELEASE_0_3_BURNDOWN.md`                                 | support planning      |
| R1 ci scripts          | `scripts/ci_*.sh`, `tests/release/test_r1_ci_scripts_exist.py`       | support CI            |
| R1 workflow            | `.github/workflows/ci.yml`                                           | support CI            |
| R2 README honesty      | `tests/release/test_r2_readme_honesty.py`                            | honesty               |
| R2 CHANGELOG           | `tests/release/test_r2_changelog_030.py`                             | support release       |
| R2 claims              | `tests/invariants/claims.yaml` R0.3 entries                          | honesty               |
| R3 SECURITY.md         | `SECURITY.md`, `tests/release/test_r3_security_md.py`                | support security      |
| R3 config-check strict | `tests/test_config_check_cli.py`, release tests                      | support security      |
| R4 version 0.3.0       | `pyproject.toml`, `tests/release/test_r4_version.py`                 | support package       |
| R4 wheel smoke         | `scripts/ci_package_smoke.sh`                                        | support package       |
| R5 PERF_BASELINE       | `docs/ops/PERF_BASELINE.md`, release test                            | support perf evidence |
| R6 checklist           | `docs/ops/RELEASE_CHECKLIST.md`, release test                        | support ops           |
| R7 gate                | `scripts/release_gate.sh`, `tests/release/test_r7_gate_artifacts.py` | support release       |
| Prior product          | DistLab T1–T7, residual T17–T139                                     | INV-\* unchanged      |

## Non-claims (release snapshot)

- CI green ≠ pen-test or formal verification
- PERF_BASELINE lab numbers ≠ WAN multi-region SLA
- SECURITY.md threat model ≠ BFT or residual-free guarantee
- version 0.3.0 ≠ feature-complete enterprise suite
- demo-smoke alone was insufficient; matrix is the bar
- This ledger ≠ Jepsen/Elle proof

## Validated

`bash scripts/release_gate.sh` → exit 0 on 2026-08-07 (lint, typecheck/import, unit-fast, invariants, distlab-core, demo-smoke, security-deps, package-smoke, perf-smoke, `tests/release/`).

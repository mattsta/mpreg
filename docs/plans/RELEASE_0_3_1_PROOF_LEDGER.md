# MPREG 0.3.1 Production Hardening — Proof Ledger

| Point | Proof path | Claim |
| --- | --- | --- |
| H0 planning | `docs/RELEASE_0_3_1_*`, `docs/plans/RELEASE_0_3_1_*` | support |
| H1 unit-fast | `scripts/ci_unit_fast.sh`, `tests/release/test_h1_ci_surface.py` | support CI |
| H1 typecheck | `scripts/ci_typecheck.sh` | support CI |
| H2 lint | `scripts/ci_lint.sh`, ruff autofix, `test_h2_lint_bar.py` | support quality |
| H3 support | `SUPPORT.md` | support ops |
| H4 version | `pyproject.toml`, `test_h4_version_031.py` | support package |
| H4 claims | `tests/invariants/claims.yaml` `release_0_3_1` | honesty |
| H5 gate | `scripts/release_gate.sh` | support release |
| Prior | 0.3.0 snapshot + DistLab/residual | unchanged INV-* |

## Non-claims

- 0.3.1 does not claim full-tree ruff/mypy zero
- SUPPORT.md is not a commercial SLA
- Hardening patch ≠ new distributed guarantees (still CFT, not BFT/WAN/Jepsen)

## Validated

`bash scripts/release_gate.sh` → exit 0 on 2026-08-07 (26 release tests; version 0.3.1).

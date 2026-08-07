# MPREG 0.3.1 Production Hardening — Master Plan (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-07 |
| **Authority** | Official planning for post-0.3.0 hardening patch |
| **Architecture** | `docs/RELEASE_0_3_1_PRODUCTION_HARDENING_ARCHITECTURE.md` |
| **Burndown** | `docs/plans/RELEASE_0_3_1_BURNDOWN.md` |
| **Proof ledger** | `docs/plans/RELEASE_0_3_1_PROOF_LEDGER.md` |
| **Version target** | `0.3.1` |
| **Point budget** | 5 tracks × ~15–25 pts = **~100 items** |

## Global rules

1. Do **not** open residual honesty T140+ unless P0 product bug.
2. Prefer safe autofix over mass style churn that risks behavior.
3. `uv run` entry points only in docs/scripts.
4. Gate: `bash scripts/release_gate.sh` must stay green after each track.
5. Track complete only with burndown check + tests/scripts green.

## Gate command

```bash
bash scripts/release_gate.sh
uv run pytest tests/release/ -q
```

---

# TRACK H1 — CI surface expansion (~20 pts)

1. Expand `scripts/ci_unit_fast.sh` with stable high-signal paths
2. Keep `-m "not slow and not chaos…"`
3. Expand typecheck import smoke (client, fabric light, ops)
4. Dynamic version assert (not hardcoded 0.3.0 only)
5. mypy additional modules with `--follow-imports=skip` if clean
6. `tests/release/test_h1_ci_surface.py`
7–20. Buffer / path tuning if flaky

# TRACK H2 — Lint bar raise (~20 pts)

21. `ruff check mpreg --select I,F401,UP --fix`
22. Re-run unit-fast after autofix
23. Raise `ci_lint.sh`: E9 full + I,F401,UP035 on mpreg + full rules release/
24. Document remaining BLE001 debt as post-0.3.1
25. `tests/release/test_h2_lint_bar.py`
26–40. Buffer

# TRACK H3 — Container & support (~20 pts)

41. `SUPPORT.md` (how to get help, links to SECURITY/claims)
42. Explicit non-support of process/process-mesh in ops docs
43. RELEASE_CHECKLIST publish path without support docss
44. RELEASE_CHECKLIST support docs + PyPI section refresh
45. PRODUCTION_DEPLOYMENT support docs pointer
46. `tests/release/test_h3_support docs_support.py`
47–60. Buffer

# TRACK H4 — Version & honesty (~15 pts)

61. version 0.3.1 pyproject + __init__ fallback
62. CHANGELOG `## [0.3.1]`
63. claims.yaml `release_0_3_1` + non_claims
64. README/GETTING_STARTED 0.3.1 pointer if needed
65. `tests/release/test_h4_version_031.py`
66–75. Buffer

# TRACK H5 — Gate & freeze (~25 pts)

76. Update package smoke assert 0.3.1
77. release_gate still orchestrates all
78. All burndown checked
79. Master/arch Complete
80. Full release_gate green
81. Commit
82–100. Buffer

## Status dashboard

| Track | Status |
| --- | --- |
| H1 CI surface | **complete** |
| H2 Lint | **complete** |
| H3 Support/docs | **complete** |
| H4 Version/honesty | **complete** |
| H5 Gate | **complete** |

## Order

```
H1 → H2 → H3 → H4 → H5
```

## Relationship

| Milestone | Status |
| --- | --- |
| 0.3.0 Production Snapshot | Complete |
| **0.3.1 Production Hardening** | **This plan** |
| Residual T140+ | Deferred |
| Full ruff/mypy zero | Future major cleanup |

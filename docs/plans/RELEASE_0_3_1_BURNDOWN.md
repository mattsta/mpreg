# MPREG 0.3.1 — Unified Sequential Burndown

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Master** | `docs/plans/RELEASE_0_3_1_PRODUCTION_HARDENING_MASTER_PLAN.md` |
| **Architecture** | `docs/RELEASE_0_3_1_PRODUCTION_HARDENING_ARCHITECTURE.md` |
| **Ledger** | `docs/plans/RELEASE_0_3_1_PROOF_LEDGER.md` |

## H0 — Planning

- [x] H0.1 Architecture
- [x] H0.2 Master plan
- [x] H0.3 Burndown
- [x] H0.4 Proof ledger
- [x] H0.5 Link from 0.3.0 architecture / ARCHITECTURE.md

## H1 — CI surface

- [x] H1.01 Expand unit-fast paths
- [x] H1.02 Expand typecheck imports + dynamic version
- [x] H1.03 Wider mypy surface if clean
- [x] H1.04 `test_h1_ci_surface.py`
- [x] H1.05 unit-fast + typecheck green

## H2 — Lint

- [x] H2.01 Safe ruff autofix I/F401/UP
- [x] H2.02 Raise ci_lint bar
- [x] H2.03 `test_h2_lint_bar.py`
- [x] H2.04 lint green

## H3 — Support & publish docs

- [x] H3.01 SUPPORT.md
- [x] H3.02 PRODUCTION / checklist links to SUPPORT.md
- [x] H3.03 Checklist / PRODUCTION links
- [x] H3.04 `test_h3_support_md.py`

## H4 — Version & honesty

- [x] H4.01 version 0.3.1
- [x] H4.02 CHANGELOG 0.3.1
- [x] H4.03 claims release_0_3_1
- [x] H4.04 `test_h4_version_031.py`

## H5 — Gate

- [x] H5.01 package smoke 0.3.1
- [x] H5.02 release tests all green
- [x] H5.03 `release_gate.sh` green
- [x] H5.04 plans Complete
- [x] H5.05 commit

## Progress log

| Date | Note |
| --- | --- |
| 2026-08-07 | 0.3.1 hardening track opened after 0.3.0 gate green |
| 2026-08-07 | All H0–H5 validated; release_gate exit 0 |

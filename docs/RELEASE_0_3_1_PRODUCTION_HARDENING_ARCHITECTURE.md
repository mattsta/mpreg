# MPREG 0.3.1 — Production Hardening Architecture

| Field | Value |
| --- | --- |
| **Status** | **Complete** — validated 2026-08-07 |
| **Milestone** | `v0.3.1` Production Hardening (patch after 0.3.0 Snapshot) |
| **Authority** | Official release architecture for post-snapshot quality bar raise |
| **Master plan** | `docs/plans/RELEASE_0_3_1_PRODUCTION_HARDENING_MASTER_PLAN.md` |
| **Burndown** | `docs/plans/RELEASE_0_3_1_BURNDOWN.md` |
| **Proof ledger** | `docs/plans/RELEASE_0_3_1_PROOF_LEDGER.md` |
| **Predecessor** | `docs/RELEASE_0_3_PRODUCTION_SNAPSHOT_ARCHITECTURE.md` (Complete) |

## 1. Purpose

0.3.0 established the **Production Snapshot** gate (CI matrix, honesty, SECURITY,
packaging, lab perf evidence). 0.3.1 **raises the quality floor** without opening
residual-honesty micro-bands or enterprise roadmap features:

- Broader **unit-fast** and **import/typecheck** surfaces
- Safe **lint autofix** + slightly stricter lint bar
- **Container + support/publish** artifacts for “upload everywhere”
- Patch version, CHANGELOG, claims, full `release_gate` green

## 2. Non-goals (still frozen)

| Out of scope | Why |
| --- | --- |
| Residual honesty T140+ | Diminishing returns; ops visibility done |
| Full-tree ruff zero (600+ BLE/SIM) | Multi-release cleanup; not a patch |
| Full-tree strict mypy zero (1000+) | Multi-release cleanup |
| OAuth2/OIDC, BFT, WAN SLA, Jepsen | Roadmap / non_claims |
| STRONG get/delete quorum | Product v1.1, not hardening patch |

## 3. Tracks

| Track | Outcome |
| --- | --- |
| **H1** | CI unit-fast + typecheck expansion |
| **H2** | Safe ruff fix + lint bar raise |
| **H4** | Version 0.3.1 + CHANGELOG + claims |
| **H5** | Gate tests + `release_gate` + freeze |

## 4. Quality bar vs 0.3.0

| Gate | 0.3.0 | 0.3.1 |
| --- | --- | --- |
| Lint | E9 full tree + full rules on `tests/release` | + safe autofix; + I/F401/UP on `mpreg/` where clean |
| Typecheck | import smoke + tiny mypy surface | + more core modules; version dynamic |
| Unit-fast | ~5 product paths + release | + client/core/integration smoke paths |
| Docs | SECURITY + checklist | + SUPPORT + container section |

## 5. Definition of done

1. `pyproject.toml` / `mpreg.__version__` == `0.3.1`
2. `bash scripts/release_gate.sh` exit 0
3. `uv run pytest tests/release/ -q` green (0.3.0 + 0.3.1 tests)
4. CHANGELOG `## [0.3.1]`; claims `release_0_3_1`
6. Master/burndown/architecture status **Complete**

## 6. Validation

Same philosophy as 0.3.0: scripts + `tests/release/` + claims. Prefer product
fixes over weakening gates. Residual T140+ remains deferred.

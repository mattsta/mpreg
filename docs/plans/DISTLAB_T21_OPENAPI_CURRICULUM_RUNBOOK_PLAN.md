# DistLab T21 — OpenAPI Honesty, Curriculum Refuse, Runbook Polish (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete (gated 228)** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T20 complete (`f20a321`) |
| **Scope** | STRONG + shared audit ops contract + curriculum teaching |
| **Point budget** | **~85 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …` |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. STRONG get/delete quorum remains v1.1 non-goal.
4. Never `python -m`.

## Stages

| Stage | Exit |
| --- | --- |
| T21-S0 | Official plan |
| T21-S1 | OpenAPI response schemas for `/metrics/strong` + `/metrics/shared-audit` with capability honesty |
| T21-S2 | Curriculum `cache_strong_quorum`: GCM STRONG get/delete 1012 + capabilities + RYW |
| T21-S3 | Runbook + SLO + OPERATE polish (presets, refuse counters, config-check) |
| T21-S4 | Tests + full related gate + docs Phase 9 + commit |

## Non-claims

Unchanged: not WAN / Elle / Jepsen / BFT / fsync / STRONG quorum get-delete.

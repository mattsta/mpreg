# DistLab T60 — Live enriched residual_ops_hint e2e (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T57/T59 |
| **Scope** | Live mesh: seed CFT residual + abort-fail diagnostics → non-empty enriched hint on scrape |
| **Point budget** | **~15 pts** |
| **Entry points only** | `uv run pytest …` |

## Goals

1. `test_distlab_live_residual_ops_hint_enriched_e2e`
2. Seed peer L1 residual (prepare+commit) + origin coordinator abort-fail fields
3. Assert `/metrics/strong` and `/mgmt/v1/strong` residual_ops_hint has
   ns/key/op_id/peer + not auto-heal
4. Doctor payload evaluation consumes the same string
5. Residuals + Phase 48

## Non-claims

Seeding abort-fail diagnostics on the coordinator mirrors exhausted-ABORT
shape — not kernel drop injectors, not WAN, not automatic heal. Peer residual
is prepare+commit seed (same honesty as T44 live client RPC e2e).

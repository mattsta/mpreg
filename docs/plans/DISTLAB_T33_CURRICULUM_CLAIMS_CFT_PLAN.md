# DistLab T33 — Curriculum + Claims CFT Honesty (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T32 (`902d0a2`) |
| **Scope** | Teach CFT residual limit in curriculum; claims.yaml non_claims; doctor ttl_gc |
| **Point budget** | **~55 pts** |
| **Entry points only** | `uv run mpreg-example …` / `uv run pytest …` |

## Stages

| Stage | Exit | Status |
| --- | --- | --- |
| T33-S0 | Plan | done |
| T33-S1 | `cache_strong_quorum` CFT residual + LWW heal scenario | done |
| T33-S2 | README + non-claims banners | done |
| T33-S3 | claims.yaml INV-CACHE-STRONG-01 + non_claims | done |
| T33-S4 | doctor detail `ttl_gc=` / counts | done |
| T33-S5 | Gate + Phase 21 + commit | done |

## Delivered

| Surface | What |
| --- | --- |
| Curriculum | CFT residual + LWW heal + caps assert |
| README | CFT non-claims |
| claims.yaml | claim text + non_claims + proof tests |
| Doctor | ttl_gc / visible / backups / pruned |

## Non-claims

Unchanged. Curriculum documents CFT limit; does not claim residual-free under lost ABORT.

## Gate

```bash
uv run pytest tests/chaos/test_t33_residuals.py -q
uv run mpreg-example run cache_strong_quorum
```

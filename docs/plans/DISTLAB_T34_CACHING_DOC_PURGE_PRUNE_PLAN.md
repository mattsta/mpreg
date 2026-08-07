# DistLab T34 — CACHING_SYSTEM CFT Honesty + Purge Prune (Official)

| Field                 | Value                                                                          |
| --------------------- | ------------------------------------------------------------------------------ |
| **Status**            | **Complete**                                                                   |
| **Date**              | 2026-08-06                                                                     |
| **Authority**         | Continuation after T33 (`f48e8fd`)                                             |
| **Scope**             | Product docs CFT honesty; purge path also prunes orphan backups; Hypothesis GC |
| **Point budget**      | **~40 pts**                                                                    |
| **Entry points only** | `uv run pytest …`                                                              |

## Stages

| Stage  | Exit                                                  | Status |
| ------ | ----------------------------------------------------- | ------ |
| T34-S0 | Plan                                                  | done   |
| T34-S1 | `docs/CACHING_SYSTEM.md` CFT honesty                  | done   |
| T34-S2 | `purge_expired_pending` also runs orphan backup prune | done   |
| T34-S3 | Hypothesis orphan backup GC property                  | done   |
| T34-S4 | Gate + Phase 22 + commit                              | done   |

## Non-claims

Unchanged CFT limits.

## Gate

```bash
uv run pytest tests/chaos/test_t34_residuals.py \
  tests/invariants/test_cache_strong_properties.py -k "orphan or cft" -q
```

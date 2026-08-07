# DistLab T38 — GCM retry_abort Surface + Product Docs (Official)

| Field                 | Value                                                                          |
| --------------------- | ------------------------------------------------------------------------------ |
| **Status**            | **Complete**                                                                   |
| **Date**              | 2026-08-06                                                                     |
| **Authority**         | Continuation after T37 (`23acb40`)                                             |
| **Scope**             | GlobalCacheManager.strong_retry_abort; CACHING_SYSTEM + client/curriculum docs |
| **Point budget**      | **~30 pts**                                                                    |
| **Entry points only** | `uv run pytest …`                                                              |

## Goals

1. `GlobalCacheManager.strong_retry_abort(key, op_id, peers=…)` wraps coordinator.
2. Sync abort counters into GCM metrics on retry.
3. `docs/CACHING_SYSTEM.md` documents abort_fail_peers + retry_abort.
4. Curriculum README + client guide cross-links.
5. Residuals + ledger + commit.

## Non-claims

Unchanged CFT limits. No HTTP mgmt mutation for retry in this track (library API).
Not automatic background heal.

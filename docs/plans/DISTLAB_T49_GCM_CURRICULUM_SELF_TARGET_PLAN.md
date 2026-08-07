# DistLab T49 — GCM curriculum + DistLab self-target (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T48 |
| **Scope** | Curriculum GCM.strong_retry_abort residual→clear; DistLab self-target scenario; ops_surfaces meta |
| **Point budget** | **~20 pts** |
| **Entry points only** | `uv run mpreg-example …` / `uv run pytest …` |

## Goals

1. `cache_strong_quorum` teaches GCM.strong_retry_abort after CFT residual
2. DistLab `strong.cft_retry_abort_self_target` (peers=[self] local.abort)
3. Scenario registered + in `strong-core` / `ci-core`
4. `ops_surfaces` meta on clears-residual scenario lists full ops stack
5. Residuals + Phase 37

## Non-claims

Self-target is RPC fan-in correctness — not automatic heal, not BFT, not WAN.
Curriculum GCM path is teachable library surface, not SIEM orchestration.

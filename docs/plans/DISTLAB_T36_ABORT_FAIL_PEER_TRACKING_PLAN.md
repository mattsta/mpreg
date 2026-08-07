# DistLab T36 — Abort-Fail Peer Tracking + Client Honesty (Official)

| Field                 | Value                                                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **Status**            | **Complete**                                                                                                       |
| **Date**              | 2026-08-06                                                                                                         |
| **Authority**         | Continuation after T35 (`235ca58`)                                                                                 |
| **Scope**             | Surface which peers exhausted ABORT retries (CFT residual candidates); fix client/catalog residual-free overclaims |
| **Point budget**      | **~40 pts**                                                                                                        |
| **Entry points only** | `uv run pytest …` / `uv run mpreg …`                                                                               |

## Problem

1. Operators see `aborts_peer_fail` counters but not **which peers** failed ABORT —
   the useful CFT residual signal for repair/LWW targeting.
2. Client-facing copy still says unqualified "residual-free failures"
   (`MPREG_CLIENT_GUIDE`, APP_CATALOG, feature registry, design alternatives table).

## Goals

1. `_abort_all` records last-fail peer set + bounded recent ring on coordinator.
2. Failed put `quorum_info` includes `abort_fail_peers` when ABORT exhausted.
3. `strong_status` / metrics snapshot / doctor / monitor expose last fail peers.
4. DistLab CFT scenario asserts `abort_fail_peers` contains residual peer.
5. Client guide + catalogs + design alt table use CFT-qualified residual wording.
6. Do **not** claim automatic residual heal or residual-free under lost ABORT.

## Stages

| Stage  | Work                                               | Status |
| ------ | -------------------------------------------------- | ------ |
| T36-S1 | Plan                                               | done   |
| T36-S2 | Coordinator abort_fail peer tracking + quorum_info | done   |
| T36-S3 | GCM status/metrics + doctor/monitor                | done   |
| T36-S4 | DistLab CFT assert + docs honesty                  | done   |
| T36-S5 | Residuals + gate + ledger + commit                 | done   |

## Non-claims

Unchanged: WAN/Elle/Jepsen/BFT/fsync/STRONG quorum get-delete/SIEM/
residual-free under partial-commit+lost-abort / pending TTL as residual GC /
automatic ABORT / LWW ≠ ABORT. Peer list is ops diagnostics only.

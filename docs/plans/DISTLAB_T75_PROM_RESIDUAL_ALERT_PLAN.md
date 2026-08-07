# DistLab T75 — Prometheus residual-candidate info alert (Official)

| Field                 | Value                                                           |
| --------------------- | --------------------------------------------------------------- |
| **Status**            | **Complete**                                                    |
| **Date**              | 2026-08-06                                                      |
| **Authority**         | Continuation after T73                                          |
| **Scope**             | MPREGStrongAbortFailPeersPresent info alert (lab_process_local) |
| **Point budget**      | **~8 pts**                                                      |
| **Entry points only** | slo helper + packaged prometheus_alerts.yml                     |

## Goals

1. Info-severity alert when abort_fail_peers > 0 for 5m
2. Annotations point at residual_ops_hint / retry-abort; not auto-heal
3. Residuals + Phase 63

## Non-claims

Info alert is ops guidance — not automatic heal, not residual-free proof, not WAN/BFT/SIEM.

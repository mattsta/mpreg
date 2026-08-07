# DistLab T24 — Prometheus Capability Gauges + Ops CLI Honesty (Official)

| Field                 | Value                                                           |
| --------------------- | --------------------------------------------------------------- |
| **Status**            | **Complete (gated 245)**                                        |
| **Date**              | 2026-08-06                                                      |
| **Authority**         | Continuation after T23 complete (`2ee42ae`)                     |
| **Scope**             | Scrapable honesty + operator CLI teaching for STRONG/audit      |
| **Point budget**      | **~85 pts**                                                     |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …` |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA — alert rules must say so.
3. Shared audit is not SIEM / BFT / infinite retention.
4. STRONG get/delete quorum remains v1.1 non-goal.
5. Never `python -m`.

## Stages

| Stage  | Exit                                                                        |
| ------ | --------------------------------------------------------------------------- |
| T24-S0 | Official plan                                                               |
| T24-S1 | Prom gauges: `mpreg_strong_cap_*`, `mpreg_shared_audit_cap_*` (0/1 honesty) |
| T24-S2 | Alert rules: pending / audit drops with non-WAN annotations                 |
| T24-S3 | `ops_cli_tour`: monitor strong/audit table + doctor --strong/--audit        |
| T24-S4 | Tests + full related gate + docs Phase 12 + commit                          |

## Non-claims

Unchanged: not WAN / Elle / Jepsen / BFT / fsync / STRONG quorum get-delete / SIEM.

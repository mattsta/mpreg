# DistLab T26 — Coexistence Live Prom Caps + OPERATE Polish (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete (gated 254)** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T25 complete (`6160c88`) |
| **Scope** | Same-process STRONG+audit prom honesty + operator docs |
| **Point budget** | **~55 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …` |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. Never `python -m`.

## Stages

| Stage | Exit |
| --- | --- |
| T26-S0 | Official plan |
| T26-S1 | Live doctor e2e scrapes prom with **both** strong + audit cap gauges honest |
| T26-S2 | OPERATE + runbook: ci-core, prom cap series, monitor table |
| T26-S3 | Tests + full related gate + docs Phase 14 + commit |

## Non-claims

Unchanged: not WAN / Elle / Jepsen / BFT / fsync / STRONG quorum get-delete / SIEM.

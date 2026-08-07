# DistLab T22 — Shared-Audit Capability Honesty + Curriculum Parity (Official)

| Field | Value |
| --- | --- |
| **Status** | **Complete (gated 235)** |
| **Date** | 2026-08-06 |
| **Authority** | Continuation after T21 complete (`af05563`) |
| **Scope** | Shared audit ops contract parity with STRONG honesty |
| **Point budget** | **~90 pts** |
| **Entry points only** | `uv run mpreg …` / `uv run mpreg-example …` / `uv run pytest …` |

## Global rules

1. Prefer production correctness fixes discovered by tests.
2. Lab SLI / suite runners are not WAN SLA.
3. Shared audit is not SIEM / BFT / infinite retention / linearizable cluster ops.
4. STRONG get/delete quorum remains v1.1 non-goal.
5. Never `python -m`.

## Stages

| Stage | Exit |
| --- | --- |
| T22-S0 | Official plan |
| T22-S1 | `build_shared_audit_metrics` always exposes honest `capabilities` |
| T22-S2 | `evaluate_shared_audit_doctor_payload` + doctor / monitor table wire-up |
| T22-S3 | OpenAPI `SharedAuditMetricsResponse.capabilities` enums |
| T22-S4 | Curriculum `shared_audit_mesh` teaches capabilities + metrics shape |
| T22-S5 | Expand `audit-core` preset; tests + full related gate + docs Phase 10 + commit |

## Capability contract (v1)

| Flag | v1 value | Notes |
| --- | --- | --- |
| `gset_epidemic` | true when store present + flag on | Product path |
| `siem` | **false** | Not a SIEM |
| `bft` | **false** | CFT gossip only |
| `infinite_retention` | **false** | Bounded watermark window |
| `linearizable_cluster_ops` | **false** | Audit visibility ≠ mutation linearizability |
| `multi_tenant_beyond_cluster_id` | **false** | cluster_id reject only |

Doctor fails closed if metrics claim `siem`, `bft`, `infinite_retention`,
`linearizable_cluster_ops`, or `multi_tenant_beyond_cluster_id`.

## Non-claims

Unchanged: not WAN / Elle / Jepsen / BFT / fsync / STRONG quorum get-delete / SIEM.

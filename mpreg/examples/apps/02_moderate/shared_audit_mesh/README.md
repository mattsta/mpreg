# shared_audit_mesh (L2 · product)

## Story

Operators mutate one node (drain) and need the **forensic audit** visible on
peers without standing up an external SIEM. Shared audit is a bounded cluster
G-Set with per-origin watermarks, replicated over fabric gossip.

## Lesson

- `SharedAuditStore` merge is commutative/idempotent; watermarks are monotonic.
- Epidemic DELTA + digest/PULL anti-entropy converge the window.
- `build_audit_response(..., scope="cluster")` is the HTTP/CLI contract shape.
- Production binds `FabricSharedAuditTransport` when
  `mgmt_audit_shared_enabled=true`; this app uses `InProcessSharedAuditTransport`
  (same replicator core).

## Run

```bash
uv run mpreg-example run shared_audit_mesh
```

## What it proves

- Drain-shaped record on A appears in B and C stores after DELTA flush
- `scope=cluster` response includes foreign origin rows + `shared_audit` metadata
- Digest/PULL repairs dropped DELTA
- `build_shared_audit_metrics` capabilities: `gset_epidemic` when on; never
  SIEM / BFT / infinite retention / linearizable cluster ops
- Feature tags: `ops.shared_audit`, `ops.mgmt_drain`, `ops.mgmt_audit`

## API drill-down

| Surface    | API                                                             |
| ---------- | --------------------------------------------------------------- |
| Store      | `SharedAuditStore.insert_and_persist`, `get`, `snapshot`        |
| Replicator | `SharedAuditReplicator.publish`, epidemic flush                 |
| Response   | `build_audit_response(entries, scope=..., shared_enabled=...)`  |
| Settings   | `mgmt_audit_shared_enabled`, `mgmt_audit_shared_max_entries`, … |
| HTTP       | `GET /mgmt/v1/audit?scope=cluster` (production)                 |
| Claim      | `INV-SHARED-AUDIT-01`                                           |

## Non-claims

- Not a SIEM / infinite retention store (bounded per-origin watermark window).
- Not BFT; not multi-tenant isolation beyond `cluster_id` reject.
- Does **not** make drain/detach apply linearly consistent cluster-wide — only
  the audit log becomes eventually visible.

## Production exit ramp

- Enable `mgmt_audit_shared_enabled` on mesh nodes; optional `mgmt_audit_path`
  for local JSONL.
- `mpreg admin audit` / `GET /mgmt/v1/audit?scope=cluster`
- Design: `docs/SHARED_AUDIT_AND_STRONG_CACHE_DESIGN.md`
- Next ops: `live_partition_chaos`, `ops_cli_tour`

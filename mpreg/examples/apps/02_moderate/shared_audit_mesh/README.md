# shared_audit_mesh (L2)

Three-node mesh with `mgmt_audit_shared_enabled`. Drain on A; prove cluster
audit visibility on B via G-Set + digest/PULL.

**Flags:** `mgmt_audit_shared_enabled=true`, optional `mgmt_audit_path`.

**Non-claims:** not SIEM, not BFT, bounded per-origin watermark window.

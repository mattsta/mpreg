# namespace_policy_gate (L1 · plane)

## Story

Multi-tenant clusters need a control plane for namespace visibility: validate
rules, apply them, inspect status for a viewer, export the live set, and read
the audit trail.

## Lesson

Namespace policy is a first-class RPC surface on `MPREGClientAPI`, not only CLI.

## Run

```bash
uv run mpreg-example run namespace_policy_gate
```

## What it proves

- `namespace_policy_validate` accepts well-formed rules
- `namespace_policy_apply(enabled=True)` installs rules
- `namespace_status` deny for non-matching viewer cluster
- `namespace_policy_export` returns applied rules
- `namespace_policy_audit` records apply activity

## Architecture

- One local `MPREGServer` (cluster_id=`market`)
- Client admin path over WebSocket RPC

## API drill-down

| Call | Feature ID |
|------|------------|
| `namespace_policy_validate` | `ns.validate` |
| `namespace_policy_apply` | `ns.apply` |
| `namespace_status` | `ns.status` |
| `namespace_policy_export` | `ns.export` |
| `namespace_policy_audit` | `ns.audit` |

## Non-claims

- Not a full multi-tenant isolation proof under load.
- Does not demo data-plane denial on every plane in this app (see platform tests).
- Not a substitute for network ACLs / mTLS.

## Production exit ramp

- CLI: `mpreg namespace-policy validate|apply|export|audit`
- Compose with `discovery_join` and fabric route policy apps
- `docs/examples-curriculum/FEATURE_CATALOG.md` → `ns.*`

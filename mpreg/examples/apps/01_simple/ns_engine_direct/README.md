# ns_engine_direct (L1 · plane)

In-process `NamespacePolicyEngine` owner/viewer/data decisions (not only RPC apply).

```bash
uv run mpreg-example run ns_engine_direct
```

## Proves

- `ns.engine` — construct rules, `allows_source` / `allows_viewer` / `allows_data_access`
- Disabled short-circuit and `default_allow` behavior

## Non-claims

- Live multi-tenant cutover windows under production gossip lag

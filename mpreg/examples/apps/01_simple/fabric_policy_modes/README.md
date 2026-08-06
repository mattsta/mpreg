# fabric_policy_modes (L1 · plane)

Strict isolation, explicit bridging, routing catalogs, link-state table.

```bash
uv run mpreg-example run fabric_policy_modes
```

## Proves

- `fabric.strict` — `create_strict_isolation_config`
- `fabric.explicit` — `create_explicit_bridging_config`
- `fabric.catalog` — `FunctionCatalog` / `TopicCatalog`
- `fabric.link_state` — `LinkStateTable.apply_update` + sequence guard

## Non-claims

- Live multi-area OSPF-style flooding across production continents

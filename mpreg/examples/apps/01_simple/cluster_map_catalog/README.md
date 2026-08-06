# cluster_map_catalog (L1 · plane)

Live cluster map refresh, catalog query, summary query surfaces.

```bash
uv run mpreg-example run cluster_map_catalog
```

## Proves

- `client.cluster_map` — `cluster_map` + `refresh_cluster_map`
- `disco.catalog_query` — `MPREGClientAPI.catalog_query`
- `disco.summary_query` when discovery export is available

## Non-claims

- `call_with_summary` multi-region ingress redirect under production DNS

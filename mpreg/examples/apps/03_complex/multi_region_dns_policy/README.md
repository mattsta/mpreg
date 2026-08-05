# multi_region_dns_policy (L3 · product)

## Story

US and EU edges register the same logical `shop` service under regional
namespaces and serve local RPC; DNS list/describe separates namespaces.

## Lesson

Compose multi-node peers + DNS gateway namespaces. Cross-cluster RPC depends on
fabric mode — app is honest when isolation blocks EU from US client.

## Run

```bash
uv run mpreg-example run multi_region_dns_policy
```

## What it proves

- Dual-node peer mesh boot
- `shop` in `us` and `eu` DNS namespaces
- Local region RPC via locs
- Describe regional registration

## Non-claims

- Not global anycast traffic management.
- Cross-cluster RPC may be isolated without fabric bridge config.

## Production exit ramp

- `multi_region_shop`, `global_edge_control_plane`, `namespace_policy_gate`

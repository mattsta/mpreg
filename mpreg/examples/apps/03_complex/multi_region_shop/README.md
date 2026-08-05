# multi_region_shop (L3)

## Story

A two-region shop: **west** owns inventory lookup; **east** owns checkout
quotes. Permissive fabric bridging lets one client drive a cross-region DAG.

## Lesson

`cluster_id` + `federation_config` + resource `locs` are the multi-cluster
control knobs. Teach fabric routing before adding Raft or DNS.

## Run

```bash
uv run mpreg-example run multi_region_shop
```

## What it proves

- `inventory_lookup` returns west inventory.
- `checkout_quote` returns east quote totaling 998 cents for qty=2.

## Architecture

```text
Client ──► region-west (inventory) ══fabric══► region-east (checkout)
```

## Non-claims

- **Not** global linearizability or multi-region ACID.
- Permissive bridging is for demos — production needs route policies/security
  (`docs/FABRIC_ROUTE_POLICIES.md`, `FABRIC_ROUTE_SECURITY.md`).

## Production exit ramp

- Lock down bridging; add signed routes (`signed_route_border` planned).
- HA seeds per region (`ha_client_failover`).
- Operator loop: `mpreg doctor`, `monitor decisions` (OPERATE.md).
- World reference (planned): `global_edge_control_plane`.

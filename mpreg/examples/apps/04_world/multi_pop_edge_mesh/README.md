# multi_pop_edge_mesh (L4 · product)

## Story

A second world-tour: one control hub plus three edge POPs (US / EU / AP). The
hub geo-routes; edges serve health and cart paths; monitoring correlates the
tour with a W3C traceparent stamp.

## Lesson

Four-cluster permissive fabric + multi-POP RPC locs + unified monitoring timeline.

## Run

```bash
uv run mpreg-example run multi_pop_edge_mesh
```

## Proves

- Hub `route_plan` for us/eu/ap
- Tri-edge health pings over fabric
- Edge echo payload round-trip
- Path matrix (3 regions × 2 paths)
- Correlation timeline + traceparent shape

## Non-claims

- Lab topology on loopback — not multi-continent latency or SLA proof.
- Complements `global_edge_control_plane` (hub+2) with a third POP.

# fabric_graph_resilience (L3 · plane)

## Story

Federation planners need lab-scale graph routing (Dijkstra) and circuit-breaker
resilience patterns before operators wire real probes.

## Lesson

`FederationGraph` + `DijkstraRouter` pick lower-weight paths; `CircuitBreaker`
opens/half-opens/closes honestly.

## Run

```bash
uv run mpreg-example run fabric_graph_resilience
```

## What it proves

- Three-node topology with weighted edges
- Direct path preferred over multi-hop when cheaper
- Edge removal forces alternate path
- Circuit breaker opens at threshold and recovers after timeout + successes

## Non-claims

- Not a production federation control loop (see module docstring / claims.yaml).
- Not OSPF/BGP planet-scale SLA.
- Health probes remain UNKNOWN until real monitoring is attached.

## Production exit ramp

- `docs/FABRIC_LINK_STATE_ROUTING.md`
- Compose with `global_edge_control_plane` and `multi_region_shop`

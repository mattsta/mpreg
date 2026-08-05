# fabric_hub_hierarchy (L3 · plane)

## Story

Large multi-region lab topologies model Global → Regional → Local hubs so
clusters attach near geography and messages climb only when needed.

## Lesson

`HubTopology` + `GlobalHub` / `RegionalHub` / `LocalHub` + cluster registration
and `find_best_local_hub`.

## Run

```bash
uv run mpreg-example run fabric_hub_hierarchy
```

## Proves

- Three-tier parent/child wiring
- Cluster register / duplicate reject / load metrics
- Geographic local-hub selection
- Interest-based local route
- Topology statistics
- start_all / stop_all lifecycle

## Non-claims

- **Library / research module** — not the production `MPREGServer` control plane.
  Live peer routing uses fabric route tables, gossip, and dial.

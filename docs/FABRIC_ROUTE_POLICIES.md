# Fabric Route Policies (Optional)

## Overview

Route policies control which announcements are accepted and how routes are
scored. Policies are optional and default to permissive behavior.

Use this when:

- You want to restrict which clusters can advertise routes.
- You need deterministic selection based on latency, hops, cost, or tags.
- You want per-neighbor import filtering.

## Global Policy

```python
from mpreg.core.config import MPREGSettings
from mpreg.fabric.route_control import RoutePolicy

settings = MPREGSettings(
    fabric_route_policy=RoutePolicy(
        max_hops=5,
        allowed_tags={"gold", "low-latency"},
        deny_tags={"blocked"},
    ),
)
```

## Export Policy

Export policy filters _local_ announcements before they enter gossip:

```python
settings = MPREGSettings(
    fabric_route_export_policy=RoutePolicy(allowed_tags={"gold"}),
)
```

Because gossip is broadcast, export policy applies globally rather than
per neighbor. Use export neighbor policies for targeted exports.

### Export Targeting (Per-Neighbor)

Per-neighbor export filtering is supported through the fabric gossip target
filter. Use a policy directory to restrict which neighbors receive specific
announcements:

```python
from mpreg.fabric.route_policy_directory import RouteNeighborPolicy, RoutePolicyDirectory

export_directory = RoutePolicyDirectory(
    default_policy=RoutePolicy(allowed_destinations=set())
)
export_directory.register(
    RouteNeighborPolicy(
        cluster_id="cluster-b",
        policy=RoutePolicy(allowed_tags={"gold"}),
    )
)

settings = MPREGSettings(
    fabric_route_export_neighbor_policies=export_directory,
)
```

Export targeting is optional. If unset, all neighbors receive announcements and
apply their import policies locally.

## Neighbor Import Policies

Override imports for specific neighbors with a policy directory:

```python
from mpreg.fabric.route_policy_directory import RouteNeighborPolicy, RoutePolicyDirectory

directory = RoutePolicyDirectory(default_policy=RoutePolicy(max_hops=4))
directory.register(
    RouteNeighborPolicy(
        cluster_id="cluster-b",
        policy=RoutePolicy(allowed_tags={"gold"}),
    )
)

settings = MPREGSettings(
    fabric_route_neighbor_policies=directory,
)
```

## Route Selection Trace

Use `RouteTable.explain_selection()` to understand why a route was chosen:

```python
trace = route_table.explain_selection(destination)
print(trace.to_dict())
```

The trace captures scores, tiebreakers, and filtered candidates.

## Live Policy Reloads

Route policies can be updated without dropping existing routes:

```python
server.reload_fabric_route_config(
    route_policy=RoutePolicy(allowed_tags={"gold"}),
)
```

The new policy applies to future announcements; existing routes remain until
they expire or are withdrawn.

Per-neighbor export filters can be reloaded the same way:

```python
server.reload_fabric_route_config(
    export_neighbor_policies=export_directory,
)
```

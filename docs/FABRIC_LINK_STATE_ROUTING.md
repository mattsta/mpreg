# Fabric Link-State Routing (Optional)

## Decision Summary

Path-vector remains the default routing control plane. Link-state is added as an
optional feature for clusters that need a global topology view and predictable
shortest-path routing. This keeps the default fast and simple while enabling
more deterministic routing when topology is stable and scale is large.

## What Link-State Enables

- Global adjacency awareness instead of local path-vector hints.
- Deterministic shortest-path routing across multi-hop topologies.
- Faster convergence for stable meshes (at the cost of extra control-plane data).
- Optional ECMP selection across equal-cost paths.
- Optional area scoping to keep link-state graphs bounded.

## When to Use It

Use link-state mode when:

- The cluster graph is relatively stable (few churn events).
- You need deterministic, repeatable routing decisions across nodes.
- Multi-hop routing quality is critical and path-vector announcements are too sparse.

Avoid link-state when:

- Topology churn is high and you want minimal control-plane overhead.
- Path-vector withdrawals/hold-down already meet convergence goals.

## Configuration

```python
from mpreg.core.config import MPREGSettings
from mpreg.fabric.link_state import LinkStateMode

settings = MPREGSettings(
    fabric_link_state_mode=LinkStateMode.PREFER,  # DISABLED | PREFER | ONLY
    fabric_link_state_ttl_seconds=30.0,
    fabric_link_state_announce_interval_seconds=10.0,
    fabric_link_state_ecmp_paths=1,
    fabric_link_state_area=None,
)
```

Mode behavior:

- `DISABLED`: Path-vector only (default).
- `PREFER`: Use link-state routes first, then fall back to path-vector.
- `ONLY`: Use link-state routes only (direct peer fallback still allowed).

### ECMP and Area Scoping

- `fabric_link_state_ecmp_paths`: when >1, rotates across multiple equal-cost
  paths returned by the link-state graph.
- `fabric_link_state_area`: when set, only link-state updates for the same
  area are accepted. Use path-vector for cross-area routing.

### Multi-Area / ABR Support

Use `LinkStateAreaPolicy` to announce per-area neighbor sets and control which
neighbors participate in each area. This is the recommended way to model
multi-area clusters or ABR-style boundaries.

```python
from mpreg.fabric.link_state import LinkStateAreaPolicy

area_policy = LinkStateAreaPolicy(
    local_areas=("area-a", "area-b"),
    neighbor_areas={
        "cluster-b": ("area-a",),
        "cluster-c": ("area-b",),
        # ABR-style sharing: advertise cluster-d into both areas
        "cluster-d": ("area-a", "area-b"),
    },
    allow_unmapped_neighbors=False,
)

settings = MPREGSettings(
    fabric_link_state_area_policy=area_policy,
)
```

Note: when `fabric_link_state_area_policy` is set, it drives link-state
announcements and area acceptance. The simple `fabric_link_state_area` setting
is still used as the default area when the policy does not provide one.

### ABR-Style Summary Exports

Use summary filters + area hierarchy to control which neighbors from a child
area are exported into a parent area. Summary export rules are explicit, so
nothing is exported unless a filter is configured.

```python
from mpreg.fabric.link_state import LinkStateAreaPair, LinkStateSummaryFilter

area_policy = LinkStateAreaPolicy(
    local_areas=("area-a", "area-b"),
    neighbor_areas={
        "cluster-b": ("area-a",),
        "cluster-c": ("area-b",),
    },
    area_hierarchy={"area-b": "area-a"},
    summary_filters={
        LinkStateAreaPair(source_area="area-b", target_area="area-a"): (
            LinkStateSummaryFilter(allowed_neighbors=frozenset({"cluster-c"}))
        )
    },
)
```

## Integration Details

- Link-state updates are gossiped as `link_state_update` messages.
- Each update advertises a cluster's direct neighbor set + metrics.
- Updates feed a local graph router (`GraphBasedFederationRouter`) to compute
  shortest paths.
- Planner order (when enabled): link-state → path-vector → graph fallback.
- Peer connection management prioritizes link-state neighbors for dialing when
  link-state mode is enabled (falling back to the default peer ordering).

## Tests

- Unit: `tests/test_fabric_link_state.py`
- Integration: `tests/integration/test_fabric_link_state_gossip.py`
- Scale validation: `tests/integration/test_fabric_route_scale_validation.py`
- ECMP mesh validation: `tests/integration/test_fabric_route_scale_validation.py::test_link_state_mesh_supports_ecmp_paths`

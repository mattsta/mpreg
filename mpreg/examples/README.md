# MPREG Examples

This directory is organized into three tiers of examples:

## Tier 1: Single-System Full Capability

These show the complete feature set of one system at a time.

Run a specific system demo:

```
uv run python mpreg/examples/tier1_single_system_full.py --system rpc
uv run python mpreg/examples/tier1_single_system_full.py --system cache
uv run python mpreg/examples/tier1_single_system_full.py --system pubsub
uv run python mpreg/examples/tier1_single_system_full.py --system queue
uv run python mpreg/examples/tier1_single_system_full.py --system fabric  # Fabric federation
uv run python mpreg/examples/tier1_single_system_full.py --system monitoring
uv run python mpreg/examples/fabric_route_security_demo.py  # Route security + policy
```

## Tier 2: Two-System Integrations

These show how two systems work together in realistic workflows.

```
uv run python mpreg/examples/tier2_integrations.py
```

Includes:

- RPC + Cache
- Pub/Sub + Queue
- Cache + Fabric Federation

## Tier 3: Full System Expansion

This is the full-system demo that combines RPC, caching (L1-L4), pub/sub,
queues, fabric federation, and monitoring into one end-to-end workflow.

Note: Federation demos are powered by the unified fabric control plane (no
deprecated federation modules remain).

```
uv run python mpreg/examples/tier3_full_system_expansion.py
```

## Quick Entry Points

```
uv run python mpreg/examples/quick_demo.py
uv run python mpreg/examples/simple_working_demo.py
uv run python mpreg/examples/real_world_examples.py
uv run python mpreg/examples/fabric_route_security_demo.py
uv run python mpreg/examples/auto_port_cluster_bootstrap.py
uv run python mpreg/examples/persistence_restart_demo.py
uv run python mpreg/examples/fabric_snapshot_restart_demo.py
```

## Demo Launcher (CLI)

Use the unified CLI to run demos:

```
uv run mpreg demo tier1 rpc
uv run mpreg demo tier2
uv run mpreg demo tier3
uv run mpreg demo all
```

## Settings File Example

Start a server from the bundled settings file:

```
uv run mpreg server start-config mpreg/examples/persistence_settings.toml
```

## Demo Suite (CI Friendly)

Run the full demo suite with fail-fast validation:

```
scripts/run_demo_suite.sh
```

Run the smoke suite for faster CI checks:

```
scripts/run_demo_smoke.sh
```

## Demo-as-Test Expectation

These demos are meant to run successfully in CI or local validation. If a demo
fails, treat it as a functional regression and fix it before publishing.

## Performance + Correctness Tips

- Prefer running tiered demos over older filenames to keep behavior consistent.
- Example suites allocate ports dynamically to avoid collisions in CI and parallel runs.
- Use `CacheOptions(cache_levels=...)` in custom scripts to target L3/L4.
- `DeliveryGuarantee.QUORUM` requires multiple subscribers and acknowledgments.
- Keep logging at INFO for stable latency measurements.

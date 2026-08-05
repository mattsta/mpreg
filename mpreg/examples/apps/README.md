# Curriculum Example Apps

**70 shipped apps** — product verticals, capability plane tours, integrations,
and unified legacy demos. One runner:

```bash
uv run mpreg-example list
uv run mpreg-example run <id>
uv run mpreg-example smoke          # 8 apps
uv run mpreg-example suite          # all 70
uv run mpreg-example demo tier1|tier2|tier3|quick|product_vertical|all_planes

uv run mpreg examples list          # same runner
uv run mpreg demo tier1             # delegates here

uv run pytest tests/examples_apps -m example_smoke
uv run pytest tests/examples_apps -m example_suite
```

**Never** `python -m` or `uv run python`.

Every successful run emits **`◆ obs:`** latency/throughput lines (`app_run`
probe default-on). RPC bare names qualify under `app.*`; platform builtins are
`mpreg.*` (namespace deny for user registration).

Docs: [`docs/examples-curriculum/`](../../../docs/examples-curriculum/).

## Levels

| Dir | Level | Role |
|-----|-------|------|
| `00_getting_started/` | L0 | Hellos (rpc, cluster, trace, pubsub, cache, ports) |
| `01_simple/` | L1 | Product apps + `plane_*` capability tours |
| `02_moderate/` | L2 | Multi-plane composition + tier2 integrations |
| `03_complex/` | L3 | Fabric, chaos, join, snapshots |
| `04_world/` | L4 | `global_edge_control_plane` |
| `_shared/` | — | Registry, runner, runtime |

## Kinds

| kind | Meaning |
|------|---------|
| `product` | Narrative teaching app |
| `plane` | Single-plane depth tour (was tier1) |
| `integration` | Two-plane drill (was tier2) |
| `legacy` | Former standalone demo, now first-class id |

## Operate

[OPERATE.md](../../../docs/examples-curriculum/OPERATE.md)

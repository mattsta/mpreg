# MPREG Examples

## Unified curriculum (only supported user path)

All learning apps, capability plane tours, and former “legacy demos” run through
one entrypoint:

```bash
uv run mpreg-example list
uv run mpreg-example run hello_rpc
uv run mpreg-example smoke          # fast CI path (8 apps)
uv run mpreg-example suite          # all 35 shipped apps
uv run mpreg-example demo tier1     # all plane_* tours
uv run mpreg-example demo tier2
uv run mpreg-example demo tier3
uv run mpreg-example demo quick
uv run mpreg-example demo product_vertical

# Same runner via main CLI
uv run mpreg examples list
uv run mpreg demo tier1

# Pytest (live app mains)
uv run pytest tests/examples_apps -m example_smoke
uv run pytest tests/examples_apps -m example_suite
```

**Never** `python -m` or `uv run python` for examples.

Docs + tracker: [`docs/examples-curriculum/`](../../docs/examples-curriculum/).  
Code: [`mpreg/examples/apps/`](apps/).  
Operate: [`docs/examples-curriculum/OPERATE.md`](../../docs/examples-curriculum/OPERATE.md).

### Levels

| Dir                        | Level | Role                      |
| -------------------------- | ----- | ------------------------- |
| `apps/00_getting_started/` | L0    | Hellos                    |
| `apps/01_simple/`          | L1    | Product + plane tours     |
| `apps/02_moderate/`        | L2    | Multi-plane composition   |
| `apps/03_complex/`         | L3    | Fabric / chaos / join     |
| `apps/04_world/`           | L4    | World reference           |
| `apps/_shared/`            | —     | Registry, runner, runtime |

### Legacy module files

Files like `tier1_single_system_full.py`, `tier2_integrations.py`,
`fabric_route_security_demo.py`, etc. remain as **implementation backends**
imported by curriculum apps (`plane_*`, integrations, wrappers). Prefer:

| Instead of                                       | Use                                            |
| ------------------------------------------------ | ---------------------------------------------- |
| `uv run python mpreg/examples/tier1_….py`        | `uv run mpreg-example run plane_rpc`           |
| `uv run python …/tier2_….py`                     | `uv run mpreg-example demo tier2`              |
| `uv run python …/tier3_….py`                     | `uv run mpreg-example run tier3_expansion`     |
| `uv run python …/quick_demo.py`                  | `uv run mpreg-example demo quick`              |
| `uv run python …/fabric_route_security_demo.py`  | `uv run mpreg-example run signed_route_border` |
| `uv run python …/persistence_restart_demo.py`    | `uv run mpreg-example run config_reload_live`  |
| `uv run python …/auto_port_cluster_bootstrap.py` | `uv run mpreg-example run auto_port_bootstrap` |

Scripts:

```bash
scripts/run_example_apps_smoke.sh
scripts/run_example_apps_suite.sh
scripts/run_demo_smoke.sh    # → mpreg-example smoke
scripts/run_demo_suite.sh    # → mpreg-example suite
```

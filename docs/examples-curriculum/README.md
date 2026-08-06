# MPREG Example Apps Curriculum

> **Honesty banner:** Examples are **runnable, assertable teaching apps**.
> “Production-ready pattern” means the structure and APIs match how you would
> grow a real service — **not** BFT, exactly-once, or multi-region strong
> consistency unless an app explicitly proves that contract. See repo
> `claims.yaml` / non-claims where present.

This directory is the **project tracker and design home** for the example-app
growth cycle: from getting-started through world-spanning fabric architectures.

| Doc | Purpose |
|-----|---------|
| [VISION.md](VISION.md) | Goals, non-goals, learning principles |
| [STAGES.md](STAGES.md) | Phases A–D with exit criteria and POC apps |
| [APP_CATALOG.md](APP_CATALOG.md) | Full matrix of shipped apps (+ closed historical planned rows) |
| [APP_CONVENTIONS.md](APP_CONVENTIONS.md) | Packaging, ports, asserts, honesty |
| [OPERATE.md](OPERATE.md) | Configure / start / run / manage / operate |
| [TRACKER.md](TRACKER.md) | Checklist status for this growth cycle |
| [POC_NOTES.md](POC_NOTES.md) | Implementation notes per vertical slice |

**Code lives under** `mpreg/examples/apps/`.  
**Central runner (entrypoints only):** `uv run mpreg-example …`  
(also `uv run mpreg examples …` / `uv run mpreg demo …` — same runner).  
**Never** `python -m` / `uv run python`. **35 shipped apps** in one registry.

## Quick start (users)

```bash
uv sync

uv run mpreg-example list
uv run mpreg-example run hello_rpc
uv run mpreg-example smoke          # 8 apps, CI-friendly
uv run mpreg-example suite          # all 35
uv run mpreg-example demo tier1     # plane tours (was tier1)
uv run mpreg-example demo product_vertical

uv run pytest tests/examples_apps -m example_smoke
uv run pytest tests/examples_apps -m example_suite
```

## Learning path

```text
hello_rpc → hello_cluster → hello_trace → hello_pubsub → hello_cache → hello_ports
        → ha_client_failover → job_queue_worker → url_shortener_rpc
        → sensor_ingest_pubsub → session_cache
        → order_intake → media_pipeline → feature_flag_mesh → webhook_dispatcher
        → multi_region_shop → signed_route_border → discovery_join → chaos_checkout
        → global_edge_control_plane
```

Depth drills: `plane_*`, `rpc_plus_cache`, `tier3_expansion`.

## Unified legacy

| Old | Now |
|-----|-----|
| `tier1_*.py` / `mpreg demo tier1` | `plane_*` / `mpreg-example demo tier1` |
| `tier2_*.py` | integration apps / `demo tier2` |
| `tier3_*.py` | `tier3_expansion` |
| standalone `*_demo.py` | canonical curriculum ids (see APP_CATALOG) |

See also: [docs/BOOK.md](../BOOK.md), [docs/EXAMPLES.md](../EXAMPLES.md),
[mpreg/examples/README.md](../../mpreg/examples/README.md).

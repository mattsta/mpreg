# POC Notes (Vertical Slice)

## Slice path

```text
hello_rpc → hello_cluster → hello_trace
  → ha_client_failover → job_queue_worker
  → order_intake → multi_region_shop
```

## Implementation choices

### Shared lifecycle

Reuse `showcase_utils.run_with_servers` patterns inside `_shared.runtime` so
apps do not reinvent start/stop races. Brief stagger + settle sleep matches
existing demos that already pass CI.

### hello_trace without external HTTP

Unified monitoring timeline is in-process and always works offline. HTTP
`/routing/decisions` requires a live monitoring port; we document that as the
production exit ramp rather than blocking L0 on aiohttp scrape races.

### ha_client_failover

Proves multi-seed **connectivity and call path** with two live seeds. Full
“kill primary mid-call” is a stronger chaos scenario deferred to
`chaos_checkout` (Phase C) so L1 stays fast and deterministic.

### order_intake composition

Uses:

- RPC for `create_order`
- Global cache for idempotency key
- TopicExchange for notification pattern match
- Queue manager for fulfill worker

In-process planes (cache/queue/pubsub) mirror tier2 style so the app stays
single-process and CI-friendly while teaching composition.

### multi_region_shop

Uses `create_permissive_bridging_config` like tier1 fabric demo. Asserts both
regional functions resolve in one client DAG. Does **not** claim linearizability
across regions.

## Follow-ups (closed / honest residual)

| Item | Status |
|------|--------|
| Split DataPipeline → `media_pipeline` | **Done** — curriculum app `media_pipeline` shipped |
| multi_region `--chaos` + FaultInjector | **Superseded** — dedicated chaos apps (`chaos_checkout`, `packet_loss_chaos`, `live_partition_chaos`, `chaos_crash_recover`) teach the surface without bloating the multi-region happy path |
| Long-lived mode (print endpoints, Ctrl+C) | **Optional / residual** — apps are ephemeral by design; use `mpreg server start-config` + profiles for long-lived (see OPERATE.md) |
| CI examples smoke | **Done** — `.github/workflows/ci.yml` runs `scripts/run_demo_smoke.sh` + `run_demo_suite.sh` |

Honest non-claims for this vertical slice remain: multi-region linearizability,
kernel TCP loss, multi-hub DAO treasury ops (see FEATURE_CATALOG residuals).

# ha_client_failover (L1)

## Story

Two identical API nodes; the cluster client is seeded with both URLs and
successfully calls `ping` using HA defaults.

## Lesson

`MPREGClusterClient(seed_urls=…)` is the production-shaped entry for multi-hub
RPC. Default call policy is HA-oriented.

## Run

```bash
uv run mpreg-example run ha_client_failover
```

## What it proves

- Call returns `ok:ha` with two live seeds.

## Non-claims

- Does not forcibly kill a seed mid-request in this L1 app (see planned
  `chaos_checkout`).

## Production exit ramp

- Tune `default_ha_policy()` / timeouts for your SLO.
- Prefer dual seeds in every region edge.
- Next: `job_queue_worker`, then `order_intake`.

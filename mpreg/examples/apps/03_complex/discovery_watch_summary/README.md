# discovery_watch_summary (L3 · product)

## Story

Operators need live catalog deltas and summarized service inventories — not only
static peer lists — when onboarding clients to a discovery-aware mesh.

## Lesson

`catalog_watch` + pubsub deltas and `summary_query` / `summary_watch` are first-class
client discovery APIs.

## Run

```bash
uv run mpreg-example run discovery_watch_summary
```

## What it proves

- `catalog_watch` returns a topic
- Registering a function emits a delta containing the new name
- `summary_query(namespace=svc.market)` lists quote + indicator
- `summary_watch` topic shapes for global/ns scopes

## Non-claims

- Not multi-region summary federation under partition.
- Not access-audit / signed summary proof (see FEATURE_CATALOG gaps).

## Production exit ramp

- `docs/DISCOVERY_RUNBOOKS.md`
- Compose with `namespace_policy_gate` and `plane_dns`

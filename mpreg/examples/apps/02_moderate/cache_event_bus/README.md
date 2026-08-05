# cache_event_bus (L2 · integration)

## Story

Cache mutations should fan out as events for invalidation, monitoring, and
cross-node coordination without polling.

## Lesson

`CachePubSubIntegration` bridges `GlobalCacheManager` ops to topic notifications
and local event listeners.

## Run

```bash
uv run mpreg-example run cache_event_bus
```

## What it proves

- Namespace notification config is stored
- `notify_cache_event(CACHE_PUT)` increments notifications_sent
- Event listeners observe put events
- Invalidation broadcast path is invocable

## Non-claims

- Not multi-cluster consistency protocol proof.
- Not a substitute for queue-backed durable audit.

## Production exit ramp

- Attach integration on server with default cache + topic exchange
- Next: `feature_flag_mesh`, `session_cache`

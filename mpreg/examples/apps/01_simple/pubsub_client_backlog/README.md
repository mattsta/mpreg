# pubsub_client_backlog (L1 · plane)

Dedicated `MPREGPubSubClient` wire path plus exchange backlog API.

```bash
uv run mpreg-example run pubsub_client_backlog
```

## Proves

- `client.pubsub` — `MPREGPubSubClient` start/subscribe/publish/stop
- `pubsub.backlog` — `get_backlog` flag + `TopicExchange.backlog.get_backlog`
- `pubsub.headers` on published messages

## Non-claims

- Cross-cluster fabric topic forward durability (`pubsub.fabric_forward`)

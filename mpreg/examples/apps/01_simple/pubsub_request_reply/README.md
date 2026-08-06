# pubsub_request_reply (L1 · product)

## Story

Some workflows need RPC-shaped request/response without registering a command —
publish on a work topic and wait for a reply on a private `reply_to` topic.

## Lesson

`MPREGPubSubClient.publish_with_reply` allocates a reply topic, sets headers,
and waits for the service worker’s response publish.

## Run

```bash
uv run mpreg-example run pubsub_request_reply
```

## What it proves

- Service worker subscribes to `rpc.echo`
- Request/reply round-trip returns `status=ok`
- Second independent request also completes

## Architecture

- One server, two `MPREGClientAPI` sessions (requester + service)
- Each wrapped in `MPREGPubSubClient`

## API drill-down

| Call                                 | Feature ID                               |
| ------------------------------------ | ---------------------------------------- |
| `subscribe(patterns, handler)`       | `pubsub.client_wire`, `pubsub.exchange`  |
| `publish_with_reply(topic, payload)` | `pubsub.publish_reply`, `pubsub.headers` |

## Non-claims

- Not durable work-queue semantics (use queue plane).
- Not exactly-once; retries are caller responsibility.
- Cross-cluster topic forward is out of scope here.

## Production exit ramp

- Prefer queue ALO for durable jobs (`job_queue_worker`)
- Wire RPC when schema stability matters (`plane_rpc`)
- Next: `webhook_dispatcher`, `sensor_ingest_pubsub`

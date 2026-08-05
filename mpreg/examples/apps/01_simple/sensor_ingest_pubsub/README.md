# sensor_ingest_pubsub (L1)

## Story

Sensor bus: temperature and pressure streams fan out to specialized + catch-all subscribers.

## Lesson

Topic taxonomy with `*` and `#` for multi-consumer ingest.

## Run

```bash
uv run mpreg-example run sensor_ingest_pubsub
```

## What it proves

- 7 total pattern matches across 4 publishes
- Specialized buckets receive only their plane

## Architecture

```text
sensors → TopicExchange → temp-svc / pressure-svc / all-svc
```

## Non-claims

- Not durable; not geo-replicated.

## Production exit ramp

- Next: webhook_dispatcher, media_pipeline

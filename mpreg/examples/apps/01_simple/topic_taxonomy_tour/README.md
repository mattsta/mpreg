# topic_taxonomy_tour (L1 · plane)

## Story

MPREG organizes control-plane topics under a shared taxonomy so RPC, queue,
cache, and federation events do not collide with user topics.

## Lesson

`TopicValidator` + `TopicTemplateEngine` + `TopicTaxonomy` constants.

## Run

```bash
uv run mpreg-example run topic_taxonomy_tour
```

## Non-claims

- `{param}` format templates are not AMQP wildcards for `matches_topic` (see API_FRICTION F17).

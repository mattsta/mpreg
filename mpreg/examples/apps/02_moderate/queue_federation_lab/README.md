# queue_federation_lab (L2 · plane)

## Story

Queue federation is a **library wire protocol** (`QueueFederationRequest/Ack/
Subscription`) plus `FabricQueueFederationManager`. This lab proves codecs and
type surfaces without spinning a full multi-cluster queue mesh.

## Run

```bash
uv run mpreg-example run queue_federation_lab
```

## Non-claims

- Not a full live cross-cluster queue SLA proof (see product multi-region apps).

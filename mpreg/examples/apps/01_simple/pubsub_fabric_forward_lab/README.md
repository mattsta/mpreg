# pubsub_fabric_forward_lab (L1 · plane)

Fabric pub/sub forwarding metadata: hop path, max_hops, header round-trip.

```bash
uv run mpreg-example run pubsub_fabric_forward_lab
```

## Proves

- `pubsub.fabric_forward` — `PubSubForwardingMetadata` + `FABRIC_PUBSUB_FORWARDING_KEY`
- Hop counting, max_hops fail-closed, header encode/decode

## Non-claims

- Live multi-cluster topic flood under production WAN loss

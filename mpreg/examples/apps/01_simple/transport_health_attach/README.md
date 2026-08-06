# transport_health_attach (L1 · plane)

## Story

Operators attach transport health aggregators to the unified monitor so
connection scores feed SLO / golden-signal views.

## Lesson

`create_transport_health_aggregator` + `TransportHealthSnapshot` +
`UnifiedSystemMonitor.attach_transport_adapter`.

## Run

```bash
uv run mpreg-example run transport_health_attach
```

## What it proves

- Aggregator construction and snapshots
- attach_transport_adapter accepts an adapter with health maps
- Snapshot fields (score, error rate, connections)

## Non-claims

- Does not open a live multi-protocol listener mesh.

## Production exit ramp

- Attach real `EnhancedMultiProtocolAdapter` in server boot

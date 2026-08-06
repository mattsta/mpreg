# transport_protocol_tour (L1 · plane)

## Story

MPREG's default curriculum path is WebSocket, but the platform also ships a
length-prefixed **TCP** transport and an enhanced multi-protocol adapter.

## Lesson

TCP framing (`[u32 BE length][payload]`), ping/pong constants, and
`EnhancedMultiProtocolAdapter` as the multi-scheme façade.

## Run

```bash
uv run mpreg-example run transport_protocol_tour
```

## What it proves

- Wire header size and PING/PONG constants
- Manual frame encode/decode
- Multi-protocol adapter types importable

## Non-claims

- Does not run a full external TCP client matrix in CI.

## Production exit ramp

- `tcp://` / `tcps://` URLs via `TransportFactory`

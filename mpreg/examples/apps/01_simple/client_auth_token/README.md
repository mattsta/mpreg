# client_auth_token (L1 · product)

## Story

Operators protect the monitoring HTTP surface with a bearer token while clients
can carry `auth_token` on the transport security config for RPC sessions.

## Lesson

`monitoring_auth_token` + `Authorization: Bearer` / `X-MPREG-Monitoring-Token`
gate `/health`. `MPREGClientAPI(auth_token=…)` wires `SecurityConfig.auth_token`.

## Run

```bash
uv run mpreg-example run client_auth_token
```

## What it proves

- Unauthenticated monitoring → 401
- Correct Bearer and header token → 200
- Wrong token → 401
- Client auth_token plumbing still performs RPC

## Non-claims

- Not full mutual TLS / client certificate enrollment.
- Local WS RPC does not enforce auth_token by default in this demo.

## Production exit ramp

- Always set `monitoring_auth_token` off-loopback
- Next: `mpreg doctor`, TLS profiles, network policies

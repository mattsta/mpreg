# client_auth_token (L1 · product)

## Story

Operators protect the monitoring HTTP surface with a bearer token **and** can
require the same class of credential on the local WebSocket RPC handshake via
`MPREGSettings.rpc_auth_token` (Phase J F11).

## Lesson

- `monitoring_auth_token` + `Authorization: Bearer` / `X-MPREG-Monitoring-Token`
  gate `/health`.
- `rpc_auth_token` rejects inbound WS clients missing matching
  `Authorization: Bearer` or `X-API-Key` before accept-cap admission.
- `MPREGClientAPI(auth_token=…)` + `SecurityConfig(auth_token=…)` unlock RPC.

## Run

```bash
uv run mpreg-example run client_auth_token
```

## What it proves

- Unauthenticated monitoring → 401; correct Bearer/header → 200; wrong → 401
- WS without token fail-closed when `rpc_auth_token` is set
- Matching `auth_token` unlocks RPC

## Non-claims

- Not full mutual TLS enrollment (see `tls_dev_handshake` / F12).

## Production exit ramp

- Always set `monitoring_auth_token` off-loopback
- Set `rpc_auth_token` when the WS plane is exposed beyond trusted mesh
- Next: `tls_dev_handshake`, `mpreg doctor`

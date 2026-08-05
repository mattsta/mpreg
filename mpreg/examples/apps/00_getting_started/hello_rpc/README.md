# hello_rpc (L0)

## Story

A single-node “hello world” service that registers math helpers and runs a
two-step RPC dependency chain: `add` → `multiply`.

## Lesson

Register commands with resource tags (`locs`) and compose results by name.

## Run

```bash
uv run mpreg-example run hello_rpc
```

## What it proves

- `sum = 20+22 = 42`
- `scaled = sum*3 = 126`

## Architecture

```text
Client ──WS──► MPREGServer (cpu,math)
                 add, multiply
```

## Non-claims

- Not multi-node; not HA; not durable.

## Production exit ramp

- Prefer `MPREGClientAPI.call` for single-function calls.
- Add `MPREGClusterClient` when you have multiple seeds (`ha_client_failover`).
- Next: `hello_cluster`.

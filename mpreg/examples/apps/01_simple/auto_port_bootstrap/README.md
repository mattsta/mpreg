# auto_port_bootstrap (L1)

## Story

Two nodes start with port=None; OS assigns ports; B peers to A; RPC works.

## Lesson

Zero fixed ports end-to-end using on_port_assigned callbacks.

## Run

```bash
uv run mpreg-example run auto_port_bootstrap
```

## What it proves

- Both nodes receive ports
- Client call via node-b succeeds

## Architecture

```text
node-a (auto port) ← peer — node-b (auto port) ← client
```

## Non-claims

- Does not cover NAT/public bind addresses.

## Production exit ramp

- Supersedes legacy auto_port_cluster_bootstrap.py

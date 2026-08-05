# hello_ports (L0)

## Story

Never hard-code ports: allocator picks a free port, then RPC works on it.

## Lesson

port_range_context is the default for every curriculum app.

## Run

```bash
uv run mpreg-example run hello_ports
```

## What it proves

- Allocated port is a positive int
- RPC succeeds on that port

## Architecture

```text
port_range_context → MPREGServer → client
```

## Non-claims

- Does not teach OS-level port reuse races.

## Production exit ramp

- Next: auto_port_bootstrap
- Production: profiles + server start-config

# discovery_join (L3)

## Story

Node C joins an existing A←B cluster; clients can call functions on all three.

## Lesson

Membership/join via peers=[] seed and function visibility.

## Run

```bash
uv run mpreg-example run discovery_join
```

## What it proves

- ping_a/b/c all succeed from hub client

## Architecture

```text
A (hub) ← B, C join via peers
```

## Non-claims

- Not DNS/SRV discovery backends.
- Not authenticated join tokens.

## Production exit ramp

- Production: discovery runbooks + DNS gateway
- Next: global_edge_control_plane

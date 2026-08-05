# discovery_rate_limit (L1 · plane)

## Story

Discovery catalog and summary queries can be abused by a hot viewer. Operators
need a per-viewer, per-command sliding window before the wire path.

## Lesson

`DiscoveryRateLimiter` + `DiscoveryRateLimitKey` + `DiscoveryRateLimitConfig`.

## Run

```bash
uv run mpreg-example run discovery_rate_limit
```

## Proves

- Disabled config always allows
- `max_requests=0` hard-denies
- Burst then deny inside the window
- Window expiry re-allows
- Isolation by viewer / command / namespace
- `max_keys` prunes under pressure

## Non-claims

- Not wired automatically into every discovery RPC path — this app teaches the
  limiter library surface; production attach points vary by plane.

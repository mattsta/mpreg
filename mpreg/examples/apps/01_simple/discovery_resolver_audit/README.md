# discovery_resolver_audit (L1 · plane)

## Story

Turn on `discovery_resolver_mode` and exercise the public discovery RPCs:
`resolver_cache_stats`, `resolver_resync`, and `discovery_access_audit`.

## Run

```bash
uv run mpreg-example run discovery_resolver_audit
```

## What it proves

- Resolver mode enables stats/resync
- Access audit returns a structured entries bag (may be empty)
- Catalog query still reachable alongside resolver

## Non-claims

- Not a multi-tenant denial flood simulator
- Signed discovery summaries (`disco.signatures`) remain a deeper product path

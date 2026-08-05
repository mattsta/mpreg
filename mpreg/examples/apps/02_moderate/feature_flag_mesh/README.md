# feature_flag_mesh (L2)

## Story

Feature flags written in one cluster become readable in another via cache federation.

## Lesson

L4 federation-scope cache as a flag mesh.

## Run

```bash
uv run mpreg-example run feature_flag_mesh
```

## What it proves

- Flag written on A is readable on B with enabled=True

## Architecture

```text
cluster-a cache ──L4──► cluster-b cache
```

## Non-claims

- Not percentage rollouts with sticky users.
- Not signed flag authority.

## Production exit ramp

- Next: multi_region_shop
- Production: policy + monitoring on flag changes

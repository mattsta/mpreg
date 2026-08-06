# Vision: Example Apps as the Primary Learning Surface

## Why

The best way to learn MPREG is **well-written, best-practice examples at every
level** — not only API tours. Capability demos (`tier1`–`tier3`) prove features
exist. **Example apps** prove how features compose into something you could
extend into production.

## Goals

1. **Curriculum, not a dump** — ordered levels L0→L4 with one primary lesson each.
2. **Product shape** — each app is a mini package (README, run, domain, asserts).
3. **Assertable** — failure is a regression; wired into smoke/suite runners.
4. **Central interface** — entrypoint `mpreg-example` (and `mpreg examples`) to list/run/smoke/suite/describe; pytest mirrors the same mains.
5. **Operate the platform** — every level teaches config, lifecycle, and day-2 hooks.
6. **Honest contracts** — document what is and is not guaranteed.
7. **Progressive reuse** — higher levels import shared runtime and lower patterns.
8. **Production exit ramp** — every app ends with “to ship this, do X/Y/Z”.

## Non-goals

- Replacing the unit/integration test suite.
- Claiming multi-region linearizability without Raft/app-level design.
- Building a second product monorepo of half-maintained services.
- Hiding fabric complexity behind magic — teach the operators explicitly.

## Audience ladder

| Level | Audience | Time |
|-------|----------|------|
| L0 Getting started | First-hour developer | minutes |
| L1 Simple | Single-plane service author | ~15 min each |
| L2 Moderate | Backend engineer composing planes | ~30 min |
| L3 Complex | Distributed systems / platform | ~45–60 min |
| L4 World | Architect / SRE reference | multi-hour tour |

## Design principles

### 1. One lesson per app

Do not mix Raft + DNS + DAO in “hello”. Complexity is earned by level.

### 2. Dynamic ports only

Use `port_range_context` / auto-allocation. No fixed `9001` in runnable apps.

### 3. Encapsulated lifecycle

Shared helpers start/stop servers, print endpoints, flush pending work, and
always clean up (try/finally). Users should never need to kill stray processes
after a green run.

### 4. Best-practice client usage

Prefer public APIs (`MPREGClientAPI.call`, `MPREGClusterClient`) over internal
`_client.request` where the lesson allows. Show `locs`, dependency graphs, and
structured errors.

### 5. Operate as you learn

Apps that touch a server should surface:

- how settings are built (`MPREGSettings`)
- where monitoring lands (`monitoring_port`, OpenAPI)
- how to correlate (`traceparent`, routing decisions when available)
- how HA clients choose seeds

### 6. Demo-as-test

`mpreg-example smoke` / `suite` and `pytest -m example_smoke` must stay green in CI. Treat failures like
product regressions.

## Success metrics

- New contributor runs `uv run mpreg-example smoke` in < 2 minutes after `uv sync`.
- Vertical slice path documented and runnable end-to-end.
- TRACKER.md shows Phases A–N complete (curriculum + platform DX through F23).
- No app claims stronger consistency than it proves.
- CI keeps `demo-smoke` + `demo-suite` green on every push.

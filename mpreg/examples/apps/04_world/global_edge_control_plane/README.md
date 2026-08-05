# global_edge_control_plane (L4)

## Story

Flagship: hub control plane plus US/EU edge POPs serving health with correlation timeline.

## Lesson

World-shaped topology sketch — fabric multi-cluster + monitoring together.

## Run

```bash
uv run mpreg-example run global_edge_control_plane
```

## What it proves

- control_plan policy=allow
- both edges status=200
- monitoring timeline ≥ 2

## Architecture

```text
Edge-US / Edge-EU → Hub (control) ; UnifiedMonitor timeline
```

## Non-claims

- Not multi-continent latency realism.
- Not global linearizability.
- Not full operator stack (doctor/prometheus) in-process — use exit ramp.

## Production exit ramp

- uv run mpreg profile list / doctor / monitor decisions
- Nightly: mpreg-example run global_edge_control_plane
- Expand POPs and route policy as needed

# chaos_checkout (L3)

## Story

Checkout happy path with deadlines, then FaultInjector partition model fail-closed.

## Lesson

Combine real RPC checkout with explicit chaos oracle for operator drills.

## Run

```bash
uv run mpreg-example run chaos_checkout
```

## What it proves

- Cart+charge succeed under deadline
- Partition blocks api→payments in injector
- Heal restores

## Architecture

```text
Checkout-API + Payments; FaultInjector overlays partition view
```

## Non-claims

- Injector does not automatically cut live WebSockets in this demo.
- Live socket kill is a stronger drill — extend carefully.
- Not exactly-once payments.

## Production exit ramp

- Wire FaultInjector into transport adapters for deeper chaos
- See testing/faults.py

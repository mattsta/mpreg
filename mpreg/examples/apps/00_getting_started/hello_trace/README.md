# hello_trace (L0)

## Story

Record a cross-system request timeline without standing up collectors — the
unified monitoring helper keeps an in-process event stream you can assert on.

## Lesson

Correlation IDs and multi-plane events are first-class; production wires the
same idea to HTTP monitoring and W3C `traceparent`.

## Run

```bash
uv run mpreg-example run hello_trace
```

## What it proves

- At least three timeline events for one tracking id.

## Non-claims

- Does not start Prometheus or Jaeger.
- Does not prove fabric hop injection by itself.

## Production exit ramp

```bash
export MPREG_MONITORING_URL=http://127.0.0.1:<port>
uv run mpreg monitor decisions --limit 20
curl -s "$MPREG_MONITORING_URL/openapi.json" | head
```

See `docs/examples-curriculum/OPERATE.md`.

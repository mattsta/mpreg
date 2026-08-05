# deadline_hop_budget (L3 · plane)

## Story

Multi-hop fabric RPCs carry a remaining deadline on message headers and
decrement it by measured hop latency so soft-RT calls fail closed mid-path.

## Lesson

`DeadlineBudget` + `decrement_deadline_headers` on `MessageHeaders`.

## Run

```bash
uv run mpreg-example run deadline_hop_budget
```

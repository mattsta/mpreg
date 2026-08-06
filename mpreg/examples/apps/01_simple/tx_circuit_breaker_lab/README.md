# tx_circuit_breaker_lab (L1 · plane)

Dedicated `CircuitBreaker` state machine: closed → open → half-open → closed.

```bash
uv run mpreg-example run tx_circuit_breaker_lab
```

## Proves

- `tx.circuit_breaker` — `can_execute` / `record_failure` / `record_success`
- F9: `timeout_seconds` initializes `current_timeout`
- Exponential backoff on half-open failure

## Non-claims

- Automatic wiring of every transport socket to a shared breaker registry

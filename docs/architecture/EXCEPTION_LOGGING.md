# Exception Handling & Operational Logging

**Authority for:** failure classification, dual-catch, operator log severity.  
**Canonical code:** `mpreg/core/errors.py` (header policy lines 16–50; helpers 52–149).

---

## 1. Taxonomy

```text
BaseException
├── CancelledError / KeyboardInterrupt / SystemExit  ← never in catch tuples
└── Exception
    ├── EXPECTED_EXCEPTIONS     OSError, TimeoutError, ConnectionError
    ├── CONDITION_EXCEPTIONS    ValueError, TypeError, KeyError, RuntimeError
    ├── OPERATIONAL_EXCEPTIONS  EXPECTED ∪ CONDITION   ← preferred catch set
    ├── MpregError / MPREGException  ← structured wire; “expected” if logged via LCE
    └── everything else         ← unknown; must carry stack
```

| Symbol | Role |
|--------|------|
| `with_operational(*extra)` | Compose operational + domain types |
| `is_expected_failure(exc)` | Auto-classify for log verbosity |
| `log_caught_exception(log, msg, exc, *, expected=None, level=…)` | Message-only vs stack |
| `map_exception(exc)` | **Wire** axis → `MpregError` (not log severity) |

### Target log policy

| Class | Log | Stack |
|-------|-----|-------|
| Expected / condition | `error`/`warning` message | **No** |
| Unknown absorbed by supervisor | `exception` / loguru `opt(exception=)` | **Yes** |
| Fatal | stack + re-raise | Yes |

There is **no** L0–L3 ladder for exceptions in code (curriculum L0–L4 is unrelated).

---

## 2. Dual-catch gold standard

`mpreg/server.py` RPC boundaries (~9578–9591, `run_rpc` ~9769–9784):

```text
except MPREGException:     # optional third arm on client path
    return wire error as-is
except OPERATIONAL_EXCEPTIONS as exc:
    log_caught_exception(logger, "...", exc)           # auto expected
    return internal_response(short msg)
except Exception as exc:  # noqa: BLE001 — boundary must not die
    log_caught_exception(logger, "...", exc, expected=False)  # stack
    return internal_response(traceback or internal)
```

**Contract:** keep the connection/supervisor alive; classify for operators; still return structured client error where applicable.

---

## 3. Call flow

```text
raise
  → boundary kind
      RPC/wire  → dual-catch → LCE → internal_response / map
      loop      → OPERATIONAL or Exception → LCE or ad-hoc → continue/degrade
      client    → map_exception → retry or raise MpregError
      fatal     → LCE unexpected → re-raise
```

---

## 4. Adoption map (approx, production `mpreg/`)

| Primitive | Sites |
|-----------|-------|
| `log_caught_exception` | **~25** (server dual-catch, `global_cache`, Raft impl) |
| `OPERATIONAL_EXCEPTIONS` catch | **~400+** |
| `with_operational` | **~2** live |
| `map_exception` | **~20** (client + rpc_responses) |
| fabric `log_caught_exception` | **0** |

| Package | Dominant gap |
|---------|----------------|
| fabric | OPERATIONAL catch + `logger.error(f…{e})` — **no LCE** |
| core (non-cache) | same |
| server non-RPC | OPERATIONAL heavy; LCE only on gold RPC paths |
| Raft impl | LCE on key loops; rpcs/storage still ad-hoc |
| client | map+raise (correct for libraries) |

**Two systems:** operator logging taxonomy vs wire `MpregError` taxonomy. They meet at RPC boundaries.

---

## 5. Unit contract

`tests/test_operational_exception_logging.py` — classification, message-only vs stack, `with_operational`.

**Missing:** sink-level integration that server RPC dual-catch and one Raft loop log as specified under a real logger.

---

## 6. Architectural rules for the program

1. Catch-set and `log_caught_exception` ship **together**.  
2. Promote server dual-catch as supervisor template.  
3. Do not overload `map_exception` for log severity.  
4. Metrics/scrape BLE001 walls may stay wide but must LCE when they log.  
5. CONDITION types (`RuntimeError`/`ValueError`) are soft by default; domain bugs should use types outside CONDITION or `expected=False`.

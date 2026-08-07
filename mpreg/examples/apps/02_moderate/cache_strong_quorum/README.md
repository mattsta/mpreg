# cache_strong_quorum (L2 · product)

## Story

EVENTUAL L3 gossip is fire-and-forget. Callers that need a **majority-commit
put** enable `cache_strong_enabled` and use `ConsistencyLevel.STRONG`. Failures
**aim** to leave **no visible residual** for that `op_id` on any replica in \(R\)
under CFT best-effort ABORT. Partial peer COMMIT + lost ABORT is a documented
CFT limit (not residual-free; not BFT).

## Lesson

- `StrongPutCoordinator`: PREPARE (pending invisible) → majority COMMIT_ACK →
  **origin-commit-last**; ABORT uncommits peer+origin L1 on failure (best-effort).
- Flag off / unbound coordinator → `1012 UNSUPPORTED_CONSISTENCY` (no local write).
- Insufficient eligible peers → `1015 INSUFFICIENT_QUORUM` (residual-free).
- **CFT:** partial COMMIT + lost ABORT may leave peer L1; pending TTL does **not**
  clear it; `quorum_info.abort_fail_peers` lists residual candidates; after
  recovery `retry_abort` can clear residual when ABORT lands (ops-driven, not
  automatic heal); later successful put can LWW-heal (not reliable ABORT).
- Caps: `cft_only`, `abort_best_effort`, `pending_ttl_clears_residual_l1=false`.
- This app teaches the same coordinator core in-process
  (`InProcessStrongTransport`); production uses `ServerCacheTransport` RR.

## Run

```bash
uv run mpreg-example run cache_strong_quorum
```

## What it proves

- 3-node majority-commit put succeeds; value readable on committers
- Insufficient peers path returns `1015` with no dirty residual
- GCM attach path still refuse-closed when coordinator/peers not production-wired
- STRONG **get** / **delete** always `1012`; EVENTUAL get RYW after STRONG put
- `strong_status.capabilities` denies `get_quorum` / `delete_quorum`; asserts
  `cft_only` / `abort_best_effort` / `pending_ttl_clears_residual_l1=false`
- CFT residual demo (partial COMMIT + lost ABORT) → `abort_fail_peers` →
  `retry_abort` clear → LWW heal path
- Feature tag: `cache.strong`

## API drill-down

| Surface     | API                                                      |
| ----------- | -------------------------------------------------------- |
| Enum        | `ConsistencyLevel.STRONG` on `CacheOptions`              |
| Coordinator | `StrongPutCoordinator.strong_put(...)`                   |
| GCM         | `GlobalCacheManager.attach_strong_coordinator` / `put`   |
| Codes       | `1012`, `1015`–`1018` (`MpregErrorCode`)                 |
| Settings    | `cache_strong_enabled`, `cache_strong_min_replicas=3`, … |
| Wire        | `CacheMessageKind.STRONG_*`                              |
| Claim       | `INV-CACHE-STRONG-01`                                    |

## Non-claims

- No STRONG **get** / quorum read MVP (always `1012`).
- No STRONG **delete** MVP (`1012`).
- Not WAN multi-region SLA, not BFT, not fsync/disk durability across replicas.
- Not residual-free under partial peer COMMIT apply + lost ABORT (CFT limit).
- Pending TTL purge is **not** residual L1 GC after COMMIT apply.
- LWW heal of a CFT residual is **not** reliable ABORT delivery.
- `location_consistency.ConsistencyLevel.STRONG` remains a separate fail-closed plane.

## Production exit ramp

- Set `cache_strong_enabled=true` with ≥ `cache_strong_min_replicas` live peers
- Prefer EVENTUAL/WEAK unless you need the commit barrier
- Docs: `docs/CACHING_SYSTEM.md` §STRONG, design doc, client guide error table

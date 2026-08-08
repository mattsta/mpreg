# Catalog Snapshot / Wire Serialize CPU — Finish the Job

Charter for closing the remaining fabric catalog snapshot serialize CPU
burst. Extends the 2026-08-04 `native_codec` + gossip hang series; does
**not** treat the residual as optional.

Full architecture, landed commits, proof style, and PR stack:
session plan (also summarized below).

## Already landed (do not re-do)

| Commit | Fix |
| --- | --- |
| `dd2c900` | `native_codec` + fingerprint / size bounds |
| `d589df6` | discovery-delta backlog off |
| `187fdbf` | gossip fingerprint; update_id coalesce |
| `429e656` | try-first orjson (no eager coerce) |
| `1c1730f` | skip discovery materialize with zero subscribers |
| `76a4a6f` | PERF-03 `serialize_model` (narrow) |
| peer-set coalesce | `CatalogSnapshotDispatchState` |

## Still broken (until this plan)

- N peers ⇒ N× `entries()` + `delta.to_dict()` + envelope encode
- register ⇒ full snapshot to all peers
- stored `rpc_spec` re-emitted on every snapshot under summary mode
- per-peer UUID `update_id` defeats applier dedup

## Target

1. Serialize-once / send-many on ttl=0 snapshot flush
2. Stable `catalog-rev:{cluster}:{generation}` update_id
3. Wire strip of `rpc_spec` unless share mode is `full`
4. Register = incremental gossip only; snapshot on connect / start
5. Proof tests: build-once counters + latency ratio (native_codec style)

## Definition of done

- Flush is O(catalog + N×cheap_send), not O(N×catalog_serialize)
- Tests fail if per-peer full rebuild returns
- Proof ledger lists this closed — not “optional residual”

# cache_strong_quorum (L2)

Majority-commit `ConsistencyLevel.STRONG` put (3-node Q=2) and residual-free
`1015 INSUFFICIENT_QUORUM`. Flag-off path remains `1012`.

**Flags:** `cache_strong_enabled=true` (production); this app uses the same
`StrongPutCoordinator` core in-process.

**Non-claims:** no quorum get/delete MVP; not WAN/BFT/disk durability.

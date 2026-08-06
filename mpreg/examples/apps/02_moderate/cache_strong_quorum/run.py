"""L2 cache_strong_quorum — majority-commit STRONG put + 1015 insufficient.

Uses in-process StrongPutCoordinator (same core as production path).
Non-claims: no quorum get/delete; not WAN/BFT/disk durability.
"""

from __future__ import annotations

import asyncio

from mpreg.core.cache_models import (
    CacheMetadata,
    CacheOptions,
    ConsistencyLevel,
    GlobalCacheKey,
)
from mpreg.core.cache_strong import (
    InProcessStrongTransport,
    StrongErrorCode,
    StrongLocalBackend,
    StrongPutCoordinator,
)
from mpreg.core.errors import MpregErrorCode
from mpreg.core.global_cache import GlobalCacheConfiguration, GlobalCacheManager
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "cache_strong_quorum",
        "Cache STRONG Quorum — majority-commit put",
        level="L2",
    ):
        transport = InProcessStrongTransport()
        backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(3)}
        for be in backends.values():
            transport.register(be)

        coord = StrongPutCoordinator(
            origin_id="n0",
            local=backends["n0"],
            transport=transport,
            cluster_id="strong-lab",
            replica_factor=3,
            min_replicas=3,
            prepare_timeout_s=1.0,
            commit_timeout_s=1.0,
        )
        gcm = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=False,
                local_cluster_id="strong-lab",
            )
        )
        gcm.attach_strong_coordinator(coord)

        try:
            key = GlobalCacheKey(namespace="strong", identifier="demo", version="v1")
            CacheOptions(consistency_level=ConsistencyLevel.STRONG)

            with scenario(
                "3-node majority-commit put",
                "cache.strong",
                "cache.put_get",
                "cache.l1",
            ):
                step("STRONG put value=42 across n0,n1,n2")
                # Coordinator selects from eligible; inject peers via backend map
                # GCM calls peer_ids on transport — ServerCacheTransport style.
                # For in-process, override by calling coordinator eligible via attach
                # and putting peers into transport.backends (already registered).
                # GlobalCacheManager._strong_put uses cache_protocol.peer_ids — none here.
                # So we call coordinator directly for the happy path teach, then GCM path.
                res = await coord.strong_put(
                    key,
                    42,
                    metadata=CacheMetadata(created_by="n0"),
                    eligible_peers=["n0", "n1", "n2"],
                )
                ensure(res.success, f"strong put failed: {res.error_message}")
                ensure(res.quorum_info is not None, "missing quorum_info")
                ensure(res.quorum_info["quorum"] == 2, f"Q={res.quorum_info}")
                # Apply to GCM L1 as production would
                if res.entry is not None:
                    gcm._put_to_l1(res.entry)
                got = await gcm.get(key)
                ensure(got.success and got.entry is not None, "L1 miss after strong")
                ensure(got.entry.value == 42, f"value {got.entry.value}")
                # Peers visible
                for nid in ("n0", "n1", "n2"):
                    ent = backends[nid].get_visible(key)
                    ensure(ent is not None and ent.value == 42, f"{nid} miss")
                ok(f"commit_acks={res.quorum_info.get('commit_acks')}")

            with scenario(
                "insufficient peers → 1015 residual-free",
                "cache.strong",
            ):
                step("min_replicas=3 with only origin eligible")
                key2 = GlobalCacheKey(
                    namespace="strong", identifier="fail", version="v1"
                )
                res2 = await coord.strong_put(
                    key2,
                    99,
                    eligible_peers=["n0"],  # insufficient
                )
                ensure(not res2.success, "should fail")
                ensure(
                    res2.error_code == int(StrongErrorCode.INSUFFICIENT_QUORUM),
                    f"code={res2.error_code}",
                )
                for be in backends.values():
                    ensure(be.get_visible(key2) is None, f"residual on {be.node_id}")
                ok("1015 INSUFFICIENT_QUORUM residual-free")

            with scenario(
                "GCM flag-off path is 1012",
                "cache.strong",
            ):
                gcm2 = GlobalCacheManager(
                    GlobalCacheConfiguration(
                        enable_l2_persistent=False,
                        enable_l3_distributed=False,
                        enable_l4_federation=False,
                    )
                )
                try:
                    r = await gcm2.put(
                        key,
                        1,
                        options=CacheOptions(consistency_level=ConsistencyLevel.STRONG),
                    )
                    ensure(not r.success, "should refuse")
                    ensure(
                        r.error_code == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
                        f"expected 1012 got {r.error_code}",
                    )
                    ok("disabled STRONG → 1012")
                finally:
                    await gcm2.shutdown()

            with scenario("non-claims", "cache.strong"):
                step("no quorum get/delete; not WAN SLA; not BFT; not fsync durability")
                ok("honesty banners retained outside majority-commit put claim")
        finally:
            await gcm.shutdown()

if __name__ == "__main__":
    asyncio.run(main())

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

            with scenario(
                "STRONG get/delete always 1012; EVENTUAL RYW after put",
                "cache.strong",
                "cache.put_get",
            ):
                step("lab single-node GCM: put STRONG, get/delete STRONG refuse")
                gcm3 = GlobalCacheManager(
                    GlobalCacheConfiguration(
                        enable_l2_persistent=False,
                        enable_l3_distributed=False,
                        enable_l4_federation=False,
                        local_cluster_id="refuse-lab",
                    )
                )
                be0 = StrongLocalBackend(node_id="origin")
                tr0 = InProcessStrongTransport()
                tr0.register(be0)
                gcm3.attach_strong_coordinator(
                    StrongPutCoordinator(
                        origin_id="origin",
                        local=be0,
                        transport=tr0,
                        lab_single_node=True,
                        min_replicas=1,
                        replica_factor=1,
                    )
                )
                try:
                    k3 = GlobalCacheKey(
                        namespace="strong", identifier="ryw", version="v1"
                    )
                    put3 = await gcm3.put(
                        k3,
                        {"ryw": True},
                        metadata=CacheMetadata(),
                        options=CacheOptions(
                            consistency_level=ConsistencyLevel.STRONG
                        ),
                    )
                    ensure(put3.success, f"put failed: {put3.error_message}")
                    bad_g = await gcm3.get(
                        k3,
                        options=CacheOptions(
                            consistency_level=ConsistencyLevel.STRONG
                        ),
                    )
                    ensure(not bad_g.success, "STRONG get must refuse")
                    ensure(
                        bad_g.error_code
                        == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
                        f"get code={bad_g.error_code}",
                    )
                    bad_d = await gcm3.delete(
                        k3,
                        options=CacheOptions(
                            consistency_level=ConsistencyLevel.STRONG
                        ),
                    )
                    ensure(not bad_d.success, "STRONG delete must refuse")
                    ensure(
                        bad_d.error_code
                        == int(MpregErrorCode.UNSUPPORTED_CONSISTENCY),
                        f"delete code={bad_d.error_code}",
                    )
                    ryw = await gcm3.get(k3)  # EVENTUAL/default
                    ensure(
                        ryw.success and ryw.entry is not None,
                        "EVENTUAL RYW after STRONG put",
                    )
                    ensure(ryw.entry.value == {"ryw": True}, f"ryw={ryw.entry.value}")
                    st = gcm3.strong_status()
                    ensure(st["gets_refused"] >= 1, f"gets_refused={st}")
                    ensure(st["deletes_refused"] >= 1, f"deletes_refused={st}")
                    caps = st.get("capabilities") or {}
                    ensure(caps.get("get_quorum") is False, "get_quorum must be false")
                    ensure(
                        caps.get("delete_quorum") is False,
                        "delete_quorum must be false",
                    )
                    ensure(
                        caps.get("put_majority_commit") is True,
                        "put_majority_commit when bound",
                    )
                    ensure(
                        caps.get("local_ryw_after_put") is True,
                        "local_ryw_after_put",
                    )
                    # T33: CFT / abort / TTL honesty caps
                    ensure(caps.get("cft_only") is True, "cft_only must be true")
                    ensure(
                        caps.get("abort_best_effort") is True,
                        "abort_best_effort must be true",
                    )
                    ensure(
                        caps.get("pending_ttl_clears_residual_l1") is False,
                        "pending_ttl must not claim residual GC",
                    )
                    ok(
                        f"1012 refuse counters gets={st['gets_refused']} "
                        f"dels={st['deletes_refused']}; EVENTUAL RYW ok; "
                        f"cft={caps.get('cft_only')} abort_be="
                        f"{caps.get('abort_best_effort')}"
                    )
                finally:
                    await gcm3.shutdown()

            with scenario(
                "CFT residual: partial COMMIT + lost ABORT (not residual-free)",
                "cache.strong",
            ):
                from mpreg.core.cache_strong import _entry_op_id

                step(
                    "n=5 Q=3: only n1 applies peer COMMIT; drop ABORT → peer L1 "
                    "residual; origin residual-free; pending purge does not clear"
                )
                tr_cft = InProcessStrongTransport()
                be_cft = {
                    f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(5)
                }
                for be in be_cft.values():
                    tr_cft.register(be)
                tr_cft.drop_commit |= {"n2", "n3", "n4"}
                tr_cft.drop_abort |= {"n1"}
                coord_cft = StrongPutCoordinator(
                    origin_id="n0",
                    local=be_cft["n0"],
                    transport=tr_cft,
                    replica_factor=5,
                    min_replicas=5,
                    prepare_timeout_s=0.4,
                    commit_timeout_s=0.25,
                    pending_ttl_s=0.05,
                    abort_attempts=3,
                )
                k_cft = GlobalCacheKey(
                    namespace="strong", identifier="cft", version="v1"
                )
                fail = await coord_cft.strong_put(
                    k_cft, {"stale": True}, eligible_peers=list(be_cft)
                )
                ensure(not fail.success, "put must fail without peer commit quorum")
                oid = fail.operation_id or ""
                o_ent = be_cft["n0"].get_visible(k_cft)
                ensure(
                    o_ent is None or _entry_op_id(o_ent) != oid,
                    "origin must be residual-free",
                )
                n1_ent = be_cft["n1"].get_visible(k_cft)
                ensure(
                    n1_ent is not None and _entry_op_id(n1_ent) == oid,
                    "expected CFT residual on n1 after partial commit + lost abort",
                )
                ensure(be_cft["n1"].pending_count() == 0, "pending already cleared")
                await asyncio.sleep(0.08)
                ensure(
                    be_cft["n1"].purge_expired_pending() == 0,
                    "no pending to purge",
                )
                n1_after = be_cft["n1"].get_visible(k_cft)
                ensure(
                    n1_after is not None and _entry_op_id(n1_after) == oid,
                    "pending TTL must not clear residual L1",
                )
                ensure(coord_cft.aborts_peer_fail >= 1, "aborts_peer_fail moved")
                fail_peers = list(
                    (fail.quorum_info or {}).get("abort_fail_peers") or []
                )
                ensure("n1" in fail_peers, f"abort_fail_peers missing n1: {fail_peers}")
                ensure(
                    "n1" in coord_cft.last_abort_fail_peers,
                    "last_abort_fail_peers missing n1",
                )
                # T78: residual_ops_hint on GCM status (ops guidance; not auto-heal)
                gcm_hint = GlobalCacheManager(
                    GlobalCacheConfiguration(
                        enable_l2_persistent=False,
                        enable_l3_distributed=False,
                        enable_l4_federation=False,
                        local_cluster_id="strong-lab-cft-hint",
                    )
                )
                gcm_hint.attach_strong_coordinator(coord_cft)
                try:
                    st_hint = gcm_hint.strong_status()
                    hop = st_hint.get("residual_ops_hint") or ""
                    ensure(hop, f"expected residual_ops_hint: {st_hint!r}")
                    ensure(
                        "cache-strong-retry-abort" in hop,
                        f"hint missing CLI: {hop}",
                    )
                    ensure("not auto-heal" in hop, f"hint missing honesty: {hop}")
                    ensure("--peer n1" in hop, f"hint missing peer: {hop}")
                    ensure(oid in hop or "--op-id" in hop, f"hint missing op: {hop}")
                    # T91: abort_fail_peer_count mirrors residual peers
                    n_peers = int(st_hint.get("abort_fail_peer_count") or 0)
                    ensure(n_peers >= 1, f"abort_fail_peer_count: {st_hint!r}")
                    ensure(
                        n_peers == len(list(st_hint.get("last_abort_fail_peers") or [])),
                        f"count mismatch: {st_hint!r}",
                    )
                    step(
                        "ERG: residual_ops_hint + abort_fail_peer_count → "
                        "cache-strong-retry-abort (ops-driven CFT; not auto-heal)"
                    )
                finally:
                    await gcm_hint.shutdown()
                # T37: retry_abort after drop_abort cleared (best-effort heal)
                tr_cft.drop_abort.clear()
                retry_out = await coord_cft.retry_abort(k_cft, oid)
                ensure(retry_out.get("cleared") is True, f"retry_abort: {retry_out}")
                n1_cleared = be_cft["n1"].get_visible(k_cft)
                ensure(
                    n1_cleared is None or _entry_op_id(n1_cleared) != oid,
                    "retry_abort must clear residual when ABORT can land",
                )
                ensure(coord_cft.last_abort_fail_peers == [], "fail peers cleared")
                # T38/T49: GCM.strong_retry_abort wraps coordinator (ops path)
                tr_cft.drop_commit |= {"n2", "n3", "n4"}
                tr_cft.drop_abort |= {"n1"}
                fail_gcm = await coord_cft.strong_put(
                    k_cft, {"stale_gcm": True}, eligible_peers=list(be_cft)
                )
                ensure(not fail_gcm.success, "gcm residual put must fail")
                oid_gcm = fail_gcm.operation_id or ""
                ensure(
                    be_cft["n1"].get_visible(k_cft) is not None
                    and _entry_op_id(be_cft["n1"].get_visible(k_cft)) == oid_gcm,
                    "expected residual for GCM retry path",
                )
                tr_cft.drop_abort.clear()
                tr_cft.drop_commit.clear()
                gcm_cft = GlobalCacheManager(
                    GlobalCacheConfiguration(
                        enable_l2_persistent=False,
                        enable_l3_distributed=False,
                        enable_l4_federation=False,
                        local_cluster_id="strong-lab-cft",
                    )
                )
                gcm_cft.attach_strong_coordinator(coord_cft)
                try:
                    gcm_out = await gcm_cft.strong_retry_abort(
                        k_cft, oid_gcm, peers=["n1"]
                    )
                    ensure(
                        gcm_out.get("cleared") is True,
                        f"GCM strong_retry_abort: {gcm_out}",
                    )
                    ensure(
                        int(gcm_cft.strong_status().get("retry_abort_calls") or 0) >= 1,
                        "GCM retry_abort_calls",
                    )
                    ensure(
                        int(gcm_cft.strong_status().get("retry_abort_cleared") or 0)
                        >= 1,
                        "GCM retry_abort_cleared",
                    )
                    n1_gcm = be_cft["n1"].get_visible(k_cft)
                    ensure(
                        n1_gcm is None or _entry_op_id(n1_gcm) != oid_gcm,
                        "GCM retry must clear residual",
                    )
                finally:
                    await gcm_cft.shutdown()
                step(
                    "ops path: GCM.strong_retry_abort → client RPC "
                    "MPREGClient.cache_strong_retry_abort / CLI "
                    "cache-strong-retry-abort (ops-driven; not auto-heal; not BFT)"
                )
                # Seed a fresh residual then LWW heal (not reliable ABORT)
                tr_cft.drop_commit |= {"n2", "n3", "n4"}
                tr_cft.drop_abort |= {"n1"}
                fail2 = await coord_cft.strong_put(
                    k_cft, {"stale2": True}, eligible_peers=list(be_cft)
                )
                ensure(not fail2.success, "second residual put must fail")
                tr_cft.drop_commit.clear()
                tr_cft.drop_abort.clear()
                heal = await coord_cft.strong_put(
                    k_cft, {"healed": True}, eligible_peers=list(be_cft)
                )
                ensure(heal.success, f"LWW heal put failed: {heal.error_message}")
                for nid, be in be_cft.items():
                    ent = be.get_visible(k_cft)
                    ensure(ent is not None, f"{nid} missing healed")
                    ensure(
                        _entry_op_id(ent) == heal.operation_id,
                        f"{nid} still residual",
                    )
                    ensure(ent.value == {"healed": True}, f"{nid} value")
                ensure(
                    be_cft["n1"].backups_count() <= 1,
                    "orphan backups pruned after heal",
                )
                ok(
                    f"CFT residual → retry_abort clear → LWW heal path; "
                    f"abort_fail={coord_cft.aborts_peer_fail} "
                    f"pruned={be_cft['n1'].backups_pruned_total}"
                )

            with scenario("non-claims", "cache.strong"):
                step(
                    "no quorum get/delete (always 1012); not WAN SLA; not BFT; "
                    "not fsync durability; local RYW is EVENTUAL/WEAK get; "
                    "ABORT best-effort (CFT residual possible); pending TTL ≠ "
                    "residual GC; LWW heal is not reliable ABORT; "
                    "retry_abort is ops best-effort (not automatic heal, not BFT); "
                    "client RPC/CLI is the same ops path, not SIEM orchestration"
                )
                ok("honesty banners retained outside majority-commit put claim")
        finally:
            await gcm.shutdown()

if __name__ == "__main__":
    asyncio.run(main())

"""L1 cache_atomic_ops — CAS / incr / structures / namespace bulk (cache.atomic*)."""

from __future__ import annotations

import asyncio

from mpreg.core.advanced_cache_ops import (
    AdvancedCacheOperations,
    AtomicOperation,
    AtomicOperationRequest,
    DataStructureOperation,
    DataStructureType,
    NamespaceOperation,
)
from mpreg.core.global_cache import (
    GlobalCacheConfiguration,
    GlobalCacheKey,
    GlobalCacheManager,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.cache_federation import FabricCacheProtocol
from mpreg.fabric.cache_transport import InProcessCacheTransport

async def main() -> None:
    with app_run(
        "cache_atomic_ops",
        "Cache Atomic Ops — CAS, counters, structures, namespace bulk",
        level="L1",
    ):
        transport = InProcessCacheTransport()
        protocol = FabricCacheProtocol(
            "atomic-node", transport=transport, gossip_interval=60.0
        )
        cache = GlobalCacheManager(
            GlobalCacheConfiguration(
                enable_l2_persistent=False,
                enable_l3_distributed=False,
                enable_l4_federation=False,
            ),
            cache_protocol=protocol,
        )
        ops = AdvancedCacheOperations(cache)
        try:
            counter_key = GlobalCacheKey(namespace="metrics", identifier="hits")
            flag_key = GlobalCacheKey(namespace="flags", identifier="beta")
            set_key = GlobalCacheKey(namespace="tags", identifier="sku-set")
            list_key = GlobalCacheKey(namespace="jobs", identifier="pending")

            with scenario(
                "increment counter from zero",
                "cache.atomic",
                "cache.put_get",
            ):
                step("INCREMENT delta=3 on missing key")
                inc = await ops.atomic_operation(
                    AtomicOperationRequest(
                        operation=AtomicOperation.INCREMENT,
                        key=counter_key,
                        delta=3,
                    )
                )
                ensure(inc.success, f"increment failed: {inc.error_message}")
                ensure(inc.old_value == 0, f"expected old 0 got {inc.old_value}")
                ensure(inc.new_value == 3, f"expected new 3 got {inc.new_value}")
                ok(f"counter 0 → {inc.new_value}")

            with scenario("compare-and-swap success and failure", "cache.atomic"):
                cas_ok = await ops.atomic_operation(
                    AtomicOperationRequest(
                        operation=AtomicOperation.COMPARE_AND_SWAP,
                        key=counter_key,
                        expected_value=3,
                        new_value=10,
                    )
                )
                ensure(cas_ok.success, f"CAS should succeed: {cas_ok.error_message}")
                ensure(cas_ok.new_value == 10, f"CAS value {cas_ok.new_value}")
                cas_fail = await ops.atomic_operation(
                    AtomicOperationRequest(
                        operation=AtomicOperation.COMPARE_AND_SWAP,
                        key=counter_key,
                        expected_value=3,
                        new_value=99,
                    )
                )
                ensure(not cas_fail.success, "CAS with stale expected must fail")
                still = await cache.get(counter_key)
                ensure(
                    still.entry is not None and still.entry.value == 10,
                    f"value corrupted after failed CAS: {still}",
                )
                ok("CAS success + fail-closed stale write")

            with scenario(
                "test-and-set flag + if_not_exists",
                "cache.atomic",
            ):
                tas = await ops.atomic_operation(
                    AtomicOperationRequest(
                        operation=AtomicOperation.TEST_AND_SET,
                        key=flag_key,
                        expected_value=None,
                        new_value={"on": True, "cohort": "a"},
                        if_not_exists=True,
                    )
                )
                ensure(tas.success, f"TAS create failed: {tas.error_message}")
                dup = await ops.atomic_operation(
                    AtomicOperationRequest(
                        operation=AtomicOperation.TEST_AND_SET,
                        key=flag_key,
                        expected_value=None,
                        new_value={"on": False},
                        if_not_exists=True,
                    )
                )
                ensure(not dup.success, "if_not_exists must reject existing key")
                ok("flag created once via if_not_exists")

            with scenario(
                "server-side set and list structures",
                "cache.structures",
            ):
                add1 = await ops.data_structure_operation(
                    DataStructureOperation(
                        structure_type=DataStructureType.SET,
                        operation="add",
                        key=set_key,
                        member="red",
                    )
                )
                add2 = await ops.data_structure_operation(
                    DataStructureOperation(
                        structure_type=DataStructureType.SET,
                        operation="add",
                        key=set_key,
                        member="blue",
                    )
                )
                ensure(add1.success and add2.success, "set add failed")
                ensure(add2.size == 2, f"set size expected 2 got {add2.size}")
                append = await ops.data_structure_operation(
                    DataStructureOperation(
                        structure_type=DataStructureType.LIST,
                        operation="append",
                        key=list_key,
                        member="job-1",
                    )
                )
                ensure(append.success, f"list append failed: {append.error_message}")
                ensure(append.size >= 1, f"list size {append.size}")
                ok(f"set size={add2.size} list size={append.size}")

            with scenario(
                "namespace count / list / clear",
                "cache.namespace_ops",
            ):
                # Seed another metrics key so namespace has ≥2
                k2 = GlobalCacheKey(namespace="metrics", identifier="misses")
                await cache.put(k2, 1)
                counted = await ops.namespace_operation(
                    NamespaceOperation(namespace="metrics", operation="count")
                )
                ensure(counted.success, f"count failed: {counted.error_message}")
                ensure(counted.count >= 2, f"expected ≥2 metrics keys, got {counted.count}")
                listed = await ops.namespace_operation(
                    NamespaceOperation(namespace="metrics", operation="list", limit=50)
                )
                ensure(listed.success, f"list failed: {listed.error_message}")
                ensure(len(listed.keys) >= 2, f"list keys {listed.keys}")
                cleared = await ops.namespace_operation(
                    NamespaceOperation(namespace="metrics", operation="clear")
                )
                ensure(cleared.success, f"clear failed: {cleared.error_message}")
                ensure(
                    cleared.cleared_count >= 2,
                    f"cleared_count {cleared.cleared_count}",
                )
                after = await ops.namespace_operation(
                    NamespaceOperation(namespace="metrics", operation="count")
                )
                ensure(after.success and after.count == 0, f"post-clear count {after.count}")
                ok(f"cleared {cleared.cleared_count} metrics keys")

            # flags namespace untouched by metrics clear
            flag_hit = await cache.get(flag_key)
            ensure(
                flag_hit.success
                and flag_hit.entry is not None
                and flag_hit.entry.value.get("on") is True,
                "flag namespace incorrectly cleared",
            )
            ok("namespace clear scoped to metrics only")
        finally:
            await cache.shutdown()
            await protocol.shutdown()

if __name__ == "__main__":
    asyncio.run(main())

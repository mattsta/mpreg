"""L1 fabric_policy_modes — strict/explicit configs + catalog + link-state."""

from __future__ import annotations

import asyncio
import time

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario
from mpreg.fabric.catalog import FunctionCatalog, TopicCatalog
from mpreg.fabric.federation_config import (
    FederationMode,
    create_explicit_bridging_config,
    create_strict_isolation_config,
)
from mpreg.fabric.link_state import (
    LinkStateMode,
    LinkStateNeighbor,
    LinkStateTable,
    LinkStateUpdate,
)


async def main() -> None:
    with app_run(
        "fabric_policy_modes",
        "Fabric Policy Modes — strict/explicit + catalog + link-state",
        level="L1",
    ):
        with scenario(
            "strict isolation factory",
            "fabric.strict",
        ):
            strict = create_strict_isolation_config("cluster-a")
            ensure(
                strict.federation_mode is FederationMode.STRICT_ISOLATION,
                f"mode={strict.federation_mode}",
            )
            ensure(strict.local_cluster_id == "cluster-a", "cluster id")
            ensure(
                strict.log_cross_federation_attempts is True,
                "should log cross attempts",
            )
            ok(f"strict mode={strict.federation_mode.value}")

        with scenario(
            "explicit bridging allowlist",
            "fabric.explicit",
        ):
            explicit = create_explicit_bridging_config(
                "cluster-a",
                allowed_clusters={"cluster-b", "cluster-c"},
            )
            ensure(
                explicit.federation_mode is FederationMode.EXPLICIT_BRIDGING,
                f"mode={explicit.federation_mode}",
            )
            ensure(
                "cluster-b" in explicit.allowed_foreign_cluster_ids,
                f"allow={explicit.allowed_foreign_cluster_ids}",
            )
            ensure(
                "cluster-z" not in explicit.allowed_foreign_cluster_ids,
                "z should not be allowed",
            )
            ok(f"explicit allows={sorted(explicit.allowed_foreign_cluster_ids)}")

        with scenario(
            "routing catalog types",
            "fabric.catalog",
        ):
            fn_cat = FunctionCatalog()
            topic_cat = TopicCatalog()
            ensure(fn_cat.entry_count() == 0, "fn catalog should start empty")
            ensure(
                hasattr(topic_cat, "entry_count")
                or hasattr(topic_cat, "entries")
                or hasattr(topic_cat, "register"),
                f"topic catalog surface {type(topic_cat)}",
            )
            # TopicCatalog API: register if present
            if hasattr(topic_cat, "entry_count"):
                ensure(topic_cat.entry_count() == 0, "topic empty")
            ok(
                f"FunctionCatalog + TopicCatalog constructed "
                f"fn_n={fn_cat.entry_count()}"
            )

        with scenario(
            "link-state table apply update",
            "fabric.link_state",
        ):
            ensure(LinkStateMode.PREFER.value == "prefer", "mode enum")
            table = LinkStateTable(local_cluster="cluster-a")
            update = LinkStateUpdate(
                origin="cluster-a",
                neighbors=(
                    LinkStateNeighbor(
                        cluster_id="cluster-b",
                        latency_ms=12.0,
                        bandwidth_mbps=1000.0,
                        reliability_score=0.99,
                    ),
                    LinkStateNeighbor(
                        cluster_id="cluster-c",
                        latency_ms=40.0,
                        reliability_score=0.95,
                    ),
                ),
                sequence=1,
                advertised_at=time.time(),
                ttl_seconds=60.0,
            )
            applied = table.apply_update(update)
            ensure(applied is True, "first update should apply")
            neighbors = table.neighbors_for("cluster-a", None)
            ensure(len(neighbors) >= 2, f"neighbors={neighbors}")
            ids = {n.cluster_id for n in neighbors}
            ensure("cluster-b" in ids and "cluster-c" in ids, f"ids={ids}")
            # Stale sequence ignored
            stale = LinkStateUpdate(
                origin="cluster-a",
                neighbors=(LinkStateNeighbor(cluster_id="cluster-z", latency_ms=1.0),),
                sequence=0,
                advertised_at=time.time(),
            )
            table.apply_update(stale)
            neighbors2 = table.neighbors_for("cluster-a", None)
            ids2 = {n.cluster_id for n in neighbors2}
            ensure("cluster-z" not in ids2, f"stale applied? {ids2}")
            ok(f"link-state neighbors={sorted(ids)} seq-guard ok")

        await asyncio.sleep(0)
        ok("fabric_policy_modes complete")


if __name__ == "__main__":
    asyncio.run(main())

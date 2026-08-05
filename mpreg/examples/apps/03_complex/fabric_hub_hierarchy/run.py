"""L3 fabric_hub_hierarchy — Global/Regional/Local hub topology (library track)."""

from __future__ import annotations

import asyncio
import time

from mpreg.core.model import PubSubMessage
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.federation_graph import GeographicCoordinate
from mpreg.fabric.federation_optimized import ClusterIdentity
from mpreg.fabric.hubs import (
    GlobalHub,
    HubCapabilities,
    HubTier,
    HubTopology,
    LocalHub,
    RegionalHub,
)

def _caps(*, max_clusters: int = 100, radius: float = 500.0) -> HubCapabilities:
    return HubCapabilities(
        max_clusters=max_clusters,
        max_child_hubs=20,
        max_subscriptions=10000,
        coverage_radius_km=radius,
    )

def _cluster(cid: str, region: str, lat: float, lon: float) -> ClusterIdentity:
    return ClusterIdentity(
        cluster_id=cid,
        cluster_name=cid,
        region=region,
        bridge_url=f"ws://{cid}.example",
        public_key_hash=f"hash-{cid}",
        created_at=time.time(),
        geographic_coordinates=(lat, lon),
        network_tier=1,
        max_bandwidth_mbps=1000,
        preference_weight=1.0,
    )

async def main() -> None:
    with app_run(
        "fabric_hub_hierarchy",
        "Fabric Hub Hierarchy — G/R/L topology (library track)",
        level="L3",
    ):
        nyc = GeographicCoordinate(latitude=40.71, longitude=-74.01)
        bos = GeographicCoordinate(latitude=42.36, longitude=-71.06)
        lon = GeographicCoordinate(latitude=51.51, longitude=-0.13)

        with scenario("build three-tier topology", "fabric.hubs"):
            topo = HubTopology()
            g = GlobalHub(
                hub_id="g1",
                hub_tier=HubTier.GLOBAL,
                capabilities=_caps(max_clusters=1000),
                coordinates=GeographicCoordinate(0.0, 0.0),
                region="global",
            )
            r_us = RegionalHub(
                hub_id="r-us",
                hub_tier=HubTier.REGIONAL,
                capabilities=_caps(),
                coordinates=nyc,
                region="us-east",
            )
            r_eu = RegionalHub(
                hub_id="r-eu",
                hub_tier=HubTier.REGIONAL,
                capabilities=_caps(),
                coordinates=lon,
                region="eu-west",
            )
            l_nyc = LocalHub(
                hub_id="l-nyc",
                hub_tier=HubTier.LOCAL,
                capabilities=_caps(radius=300.0),
                coordinates=nyc,
                region="us-east",
            )
            l_bos = LocalHub(
                hub_id="l-bos",
                hub_tier=HubTier.LOCAL,
                capabilities=_caps(radius=300.0),
                coordinates=bos,
                region="us-east",
            )
            l_lon = LocalHub(
                hub_id="l-lon",
                hub_tier=HubTier.LOCAL,
                capabilities=_caps(radius=300.0),
                coordinates=lon,
                region="eu-west",
            )
            ensure(topo.add_global_hub(g) is True, "add global")
            ensure(topo.add_regional_hub(r_us, "g1") is True, "add r-us")
            ensure(topo.add_regional_hub(r_eu, "g1") is True, "add r-eu")
            ensure(topo.add_local_hub(l_nyc, "r-us") is True, "add l-nyc")
            ensure(topo.add_local_hub(l_bos, "r-us") is True, "add l-bos")
            ensure(topo.add_local_hub(l_lon, "r-eu") is True, "add l-lon")
            ensure(topo.get_hub("l-nyc") is l_nyc, "get hub")
            ensure(l_nyc.parent_hub is r_us, "parent link nyc")
            ensure(r_us.parent_hub is g, "parent link us")
            ok("G→R(us,eu)→L(nyc,bos,lon)")

        with scenario("register clusters + load metrics", "fabric.hubs"):
            ok_reg = await l_nyc.register_cluster(
                "c-nyc-1", _cluster("c-nyc-1", "us-east", 40.7, -74.0)
            )
            ensure(ok_reg is True, "reg fail")
            dup = await l_nyc.register_cluster(
                "c-nyc-1", _cluster("c-nyc-1", "us-east", 40.7, -74.0)
            )
            ensure(dup is False, "dup should fail")
            await l_bos.register_cluster(
                "c-bos-1", _cluster("c-bos-1", "us-east", 42.3, -71.0)
            )
            await l_lon.register_cluster(
                "c-lon-1", _cluster("c-lon-1", "eu-west", 51.5, -0.1)
            )
            ensure(len(l_nyc.registered_clusters) == 1, l_nyc.registered_clusters)
            ensure(l_nyc.load_metrics.active_clusters == 1, "load metrics")
            ok("3 clusters registered")

        with scenario("find_best_local_hub by geography", "fabric.hubs"):
            best = topo.find_best_local_hub(
                GeographicCoordinate(40.75, -73.98), "us-east"
            )
            ensure(best is not None and best.hub_id == "l-nyc", f"best={best}")
            best_eu = topo.find_best_local_hub(
                GeographicCoordinate(51.5, -0.12), "eu-west"
            )
            ensure(best_eu is not None and best_eu.hub_id == "l-lon", f"eu={best_eu}")
            none = topo.find_best_local_hub(
                GeographicCoordinate(40.7, -74.0), "ap-south"
            )
            ensure(none is None, "wrong region should miss")
            ok(f"best us={best.hub_id} eu={best_eu.hub_id}")

        with scenario("subscription interest + local route", "fabric.hubs"):
            # Seed cluster interest so local hub routes without parent climb
            state = l_nyc.cluster_states["c-nyc-1"]
            state.pattern_set.add("orders.created")
            msg = PubSubMessage(
                topic="orders.created",
                payload={"id": 1},
                timestamp=time.time(),
                message_id="m1",
                publisher="shop",
            )
            routed = await l_nyc.route_message(msg)
            ensure(routed is True, "local interest should route")
            # No interest → climb / fail closed at leaf without parent interest chain
            msg2 = PubSubMessage(
                topic="metrics.cpu",
                payload={},
                timestamp=time.time(),
                message_id="m2",
                publisher="agent",
            )
            # With parent chain present, uninterested local may climb; just prove call works
            _ = await l_nyc.route_message(msg2)
            ok("interested topic routed")

        with scenario("topology statistics", "fabric.hubs"):
            stats = topo.get_topology_statistics()
            ensure(stats.total_hubs == 6, f"hubs {stats.total_hubs}")
            ensure(stats.hierarchy_depth == 3, stats.hierarchy_depth)
            ensure(stats.total_clusters == 3, f"clusters {stats.total_clusters}")
            ensure(stats.hub_counts.get("global_hubs", 0) == 1, stats.hub_counts)
            ensure(stats.hub_counts.get("regional_hubs", 0) == 2, stats.hub_counts)
            ensure(stats.hub_counts.get("local_hubs", 0) == 3, stats.hub_counts)
            ok(
                f"hubs={stats.total_hubs} clusters={stats.total_clusters} "
                f"counts={dict(stats.hub_counts)}"
            )

        with scenario("start/stop all hubs", "fabric.hubs"):
            await topo.start_all_hubs()
            ensure("hub_started" in g.routing_stats, g.routing_stats)
            await topo.stop_all_hubs()
            ensure("hub_stopped" in g.routing_stats, g.routing_stats)
            step(
                "non-claim: HubTopology is library/research — not MPREGServer "
                "default control plane"
            )
            ok("start/stop lifecycle")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())

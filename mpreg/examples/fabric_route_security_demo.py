#!/usr/bin/env python3
"""Fabric route security + neighbor policy demo (key rotation + policy filter)."""

from __future__ import annotations

import asyncio
import time

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.showcase_utils import run_with_servers
from mpreg.fabric.federation_config import create_permissive_bridging_config
from mpreg.fabric.route_control import RouteDestination, RoutePolicy, RouteTable
from mpreg.fabric.route_keys import RouteKeyRegistry
from mpreg.fabric.route_policy_directory import (
    RouteNeighborPolicy,
    RoutePolicyDirectory,
)
from mpreg.fabric.route_security import RouteAnnouncementSigner, RouteSecurityConfig
from mpreg.server import MPREGServer


async def _wait_for_route(
    route_table: RouteTable,
    destination: RouteDestination,
    *,
    timeout: float = 5.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if route_table.select_route(destination, now=time.time()) is not None:
            return
        await asyncio.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for route to {destination.cluster_id}")


async def demo_route_security_and_policy() -> None:
    with port_range_context(3, "servers") as ports:
        signer_b_v1 = RouteAnnouncementSigner.create()
        signer_c = RouteAnnouncementSigner.create()

        registry_a = RouteKeyRegistry()
        registry_a.register_key(
            cluster_id="cluster-b",
            public_key=signer_b_v1.public_key,
        )
        registry_a.register_key(
            cluster_id="cluster-c",
            public_key=signer_c.public_key,
        )

        neighbor_policies = RoutePolicyDirectory(default_policy=RoutePolicy())
        neighbor_policies.register(
            RouteNeighborPolicy(
                cluster_id="cluster-c",
                policy=RoutePolicy(allowed_tags={"gold"}),
            )
        )

        settings = [
            MPREGSettings(
                port=ports[0],
                name="route-a",
                cluster_id="cluster-a",
                peers=[f"ws://127.0.0.1:{ports[1]}", f"ws://127.0.0.1:{ports[2]}"],
                federation_config=create_permissive_bridging_config("cluster-a"),
                gossip_interval=0.5,
                fabric_route_security_config=RouteSecurityConfig(
                    require_signatures=True,
                    allow_unsigned=False,
                ),
                fabric_route_key_registry=registry_a,
                fabric_route_neighbor_policies=neighbor_policies,
                fabric_route_announce_interval_seconds=1.0,
            ),
            MPREGSettings(
                port=ports[1],
                name="route-b",
                cluster_id="cluster-b",
                peers=[f"ws://127.0.0.1:{ports[0]}"],
                federation_config=create_permissive_bridging_config("cluster-b"),
                gossip_interval=0.5,
                fabric_route_signer=signer_b_v1,
                fabric_route_announce_interval_seconds=1.0,
            ),
            MPREGSettings(
                port=ports[2],
                name="route-c",
                cluster_id="cluster-c",
                peers=[f"ws://127.0.0.1:{ports[0]}"],
                federation_config=create_permissive_bridging_config("cluster-c"),
                gossip_interval=0.5,
                fabric_route_signer=signer_c,
                fabric_route_announce_interval_seconds=1.0,
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            server_a, server_b, server_c = servers
            control_a = server_a._fabric_control_plane
            control_b = server_b._fabric_control_plane
            control_c = server_c._fabric_control_plane
            if not (control_a and control_b and control_c):
                raise RuntimeError("Fabric control plane not initialized")

            table_a = control_a.route_table
            dest_b = RouteDestination(cluster_id="cluster-b")
            dest_c = RouteDestination(cluster_id="cluster-c")

            await _wait_for_route(table_a, dest_b, timeout=6.0)
            print("Route to cluster-b accepted with signed announcements.")

            if table_a.select_route(dest_c, now=time.time()) is None:
                print("Neighbor policy blocked cluster-c route (missing required tag).")
            else:
                raise RuntimeError("Expected cluster-c route to be filtered")

            tagged = control_c.route_table.build_local_announcement(
                ttl_seconds=30.0,
                epoch=1,
                now=time.time(),
                route_tags=("gold",),
            )
            await control_c.route_publisher.publish(tagged)
            await _wait_for_route(table_a, dest_c, timeout=4.0)
            print("Cluster-c route accepted after tagged announcement.")

            signer_b_v2 = RouteAnnouncementSigner.create()
            registry_a.rotate_key(
                cluster_id="cluster-b",
                public_key=signer_b_v2.public_key,
                overlap_seconds=5.0,
                now=time.time(),
            )
            control_b.route_publisher.signer = signer_b_v2
            if control_b.route_announcer:
                await control_b.route_announcer.announce_once(now=time.time())
            await _wait_for_route(table_a, dest_b, timeout=4.0)
            print("Key rotation applied; cluster-b route still valid.")

        await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(demo_route_security_and_policy())

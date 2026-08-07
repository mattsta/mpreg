"""L3 signed_route_border — signed route announcements + neighbor policy + key rotation."""

from __future__ import annotations

import asyncio
import time

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.examples.apps._shared.runtime import (
    app_run,
    ensure,
    ok,
    run_with_servers,
    scenario,
    step,
)
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
    timeout: float = 6.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if route_table.select_route(destination, now=time.time()) is not None:
            return
        await asyncio.sleep(0.2)
    raise RuntimeError(f"Timed out waiting for route to {destination.cluster_id}")


async def main() -> None:
    with (
        app_run(
            "signed_route_border",
            "Signed Route Border — security + policy + rotation",
            level="L3",
        ),
        port_range_context(3, "servers") as ports,
    ):
        signer_b_v1 = RouteAnnouncementSigner.create()
        signer_c = RouteAnnouncementSigner.create()

        registry_a = RouteKeyRegistry()
        registry_a.register_key(
            cluster_id="cluster-b", public_key=signer_b_v1.public_key
        )
        registry_a.register_key(cluster_id="cluster-c", public_key=signer_c.public_key)

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
                peers=[
                    f"ws://127.0.0.1:{ports[1]}",
                    f"ws://127.0.0.1:{ports[2]}",
                ],
                federation_config=create_permissive_bridging_config("cluster-a"),
                gossip_interval=0.5,
                log_level="WARNING",
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
                log_level="WARNING",
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
                log_level="WARNING",
                fabric_route_signer=signer_c,
                fabric_route_announce_interval_seconds=1.0,
            ),
        ]

        async def _run(servers: list[MPREGServer]) -> None:
            server_a, server_b, server_c = servers
            control_a = server_a._fabric_control_plane
            control_b = server_b._fabric_control_plane
            control_c = server_c._fabric_control_plane

            with scenario(
                "control planes online with signature required",
                "fabric.route_security",
                "fabric.strict",
            ):
                ensure(
                    control_a is not None
                    and control_b is not None
                    and control_c is not None,
                    "control plane missing on one or more nodes",
                )
                ensure(
                    bool(getattr(control_a, "route_table", None)),
                    "route table missing on A",
                )
                ok("A/B/C fabric control planes ready")

            table_a = control_a.route_table
            dest_b = RouteDestination(cluster_id="cluster-b")
            dest_c = RouteDestination(cluster_id="cluster-c")

            with scenario(
                "signed route to cluster-b accepted",
                "fabric.route_security",
                "fabric.route_keys",
            ):
                step("wait signed route to cluster-b")
                await _wait_for_route(table_a, dest_b, timeout=8.0)
                route_b = table_a.select_route(dest_b, now=time.time())
                ensure(route_b is not None, "cluster-b route missing after wait")
                ok("cluster-b route accepted under require_signatures")

            with scenario(
                "neighbor policy blocks untagged cluster-c",
                "fabric.route_policy",
            ):
                ensure(
                    table_a.select_route(dest_c, now=time.time()) is None,
                    "cluster-c should be filtered without gold tag",
                )
                ok("neighbor policy blocked untagged cluster-c")

            with scenario(
                "gold-tagged announcement admits cluster-c",
                "fabric.route_policy",
                "fabric.route_security",
            ):
                step("publish gold-tagged announcement from C")
                tagged = control_c.route_table.build_local_announcement(
                    ttl_seconds=30.0,
                    epoch=1,
                    now=time.time(),
                    route_tags=("gold",),
                )
                await control_c.route_publisher.publish(tagged)
                await _wait_for_route(table_a, dest_c, timeout=6.0)
                route_c = table_a.select_route(dest_c, now=time.time())
                ensure(route_c is not None, "cluster-c still missing after gold tag")
                ok("cluster-c accepted after gold tag")

            with scenario(
                "rotate cluster-b signing key under traffic",
                "fabric.route_keys",
                "fabric.route_security",
            ):
                step("rotate cluster-b signing key")
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
                await _wait_for_route(table_a, dest_b, timeout=6.0)
                still_b = table_a.select_route(dest_b, now=time.time())
                ensure(still_b is not None, "cluster-b lost after key rotation")
                # C still present
                still_c = table_a.select_route(dest_c, now=time.time())
                ensure(still_c is not None, "cluster-c lost during B rotation")
                ok("key rotation applied; B and C still routable")

            step("non-claim: not Byzantine multi-signer consensus")

        await run_with_servers(settings, _run)


if __name__ == "__main__":
    asyncio.run(main())

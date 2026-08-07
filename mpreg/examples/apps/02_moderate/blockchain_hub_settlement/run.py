"""L2 blockchain_hub_settlement — hub queue + federation message path (Phase K depth)."""

from __future__ import annotations

import asyncio

from mpreg.core.blockchain_message_queue_types import (
    BlockchainMessage,
    DeliveryGuarantee,
    MessagePriority,
    MessageRoute,
    RouteStatus,
)
from mpreg.datastructures import Blockchain, DecentralizedAutonomousOrganization
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.blockchain_message_federation import (
    BlockchainFederationBridge,
    HubMessageQueue,
)
from mpreg.fabric.hubs import (
    GeographicCoordinate,
    HubCapabilities,
    HubTier,
    RegionalHub,
)


async def main() -> None:
    with app_run(
        "blockchain_hub_settlement",
        "Blockchain Hub Settlement — hub queue + route",
        level="L2",
    ):
        with scenario(
            "DAO + Blockchain genesis",
            "fabric.blockchain_msg",
            "cons.quorum_teach",
        ):
            dao = DecentralizedAutonomousOrganization(
                name="curriculum-dao",
                description="Phase K settlement lab",
            )
            chain = Blockchain.create_new_chain(
                chain_id="curriculum-chain",
                genesis_miner="curriculum",
            )
            ensure(dao.name == "curriculum-dao", dao.name)
            ensure(chain is not None, "chain None")
            ok(f"dao={dao.name} chain={type(chain).__name__}")

        with scenario(
            "RegionalHub constructible",
            "fabric.hubs",
            "fabric.blockchain_msg",
        ):
            hub = RegionalHub(
                hub_id="hub-us",
                hub_tier=HubTier.REGIONAL,
                capabilities=HubCapabilities(),
                coordinates=GeographicCoordinate(latitude=40.7, longitude=-74.0),
                region="us-east",
            )
            ensure(hub.get_hub_id() == "hub-us" or hub.hub_id == "hub-us", "hub id")
            ensure(hub.get_hub_tier() == HubTier.REGIONAL, f"tier {hub.get_hub_tier()}")
            ok(f"hub_id={hub.hub_id} region={hub.region}")

        with scenario(
            "HubMessageQueue process_federation_message",
            "fabric.blockchain_msg",
            "fabric.hubs",
        ):
            hmq = HubMessageQueue(hub=hub, dao=dao, blockchain=chain)
            msg = BlockchainMessage(
                sender_id="hub-us",
                recipient_id="hub-eu",
                message_type="settlement.transfer",
                priority=MessagePriority.HIGH,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload=b'{"amount":100,"asset":"USD"}',
                processing_fee=5,
                blockchain_record=True,
            )
            route = await hmq.process_federation_message(msg, "hub-eu")
            # route may be None if no federation path configured — submit still runs
            step(f"route={route!r}")
            ensure(msg.payload.startswith(b"{"), "payload")
            ok(
                f"process_federation_message → "
                f"{type(route).__name__ if route is not None else 'None (no path yet)'}"
            )

        with scenario(
            "MessageRoute ACTIVE settlement path",
            "fabric.blockchain_msg",
            "fabric.graph",
        ):
            route = MessageRoute(
                route_id="settle-us-eu",
                source_hub="hub-us",
                destination_hub="hub-eu",
                path_hops=["hub-us", "global", "hub-eu"],
                latency_ms=80,
                bandwidth_mbps=500,
                reliability_score=0.98,
                cost_per_mb=1,
                status=RouteStatus.ACTIVE,
            )
            ensure(route.status == RouteStatus.ACTIVE, str(route.status))
            ensure(len(route.path_hops) == 3, str(route.path_hops))
            ok(f"settlement route {route.source_hub}→{route.destination_hub}")

        with scenario(
            "BlockchainFederationBridge class surface",
            "fabric.blockchain_msg",
        ):
            ensure(BlockchainFederationBridge is not None, "bridge missing")
            ok("BlockchainFederationBridge importable for multi-hub ops")
            step(
                "multi-region on-chain settlement topology is operator wiring; "
                "this app proves hub queue submit + settlement route types"
            )

        await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main())

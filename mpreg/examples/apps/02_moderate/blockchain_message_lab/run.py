"""L2 blockchain_message_lab — BlockchainMessage + route types (Phase J)."""

from __future__ import annotations

import asyncio

from mpreg.core.blockchain_message_queue_types import (
    BlockchainMessage,
    DeliveryGuarantee,
    MessagePriority,
    MessageRoute,
    RouteStatus,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "blockchain_message_lab",
        "Blockchain Message Lab — wire types + validation",
        level="L2",
    ):
        with scenario(
            "MessagePriority + DeliveryGuarantee enums",
            "fabric.blockchain_msg",
        ):
            prios = {p.value for p in MessagePriority}
            ensure("normal" in prios, f"prios {prios}")
            ensure("emergency" in prios, f"missing emergency in {prios}")
            guars = {g.value for g in DeliveryGuarantee}
            ensure("at_least_once" in guars, f"guars {guars}")
            ok(f"priorities={sorted(prios)} guarantees={sorted(guars)}")

        with scenario(
            "MessageRoute validation",
            "fabric.blockchain_msg",
            "fabric.graph",
        ):
            route = MessageRoute(
                route_id="r-us-eu",
                source_hub="us-east",
                destination_hub="eu-west",
                path_hops=["us-east", "core", "eu-west"],
                latency_ms=42,
                bandwidth_mbps=1000,
                reliability_score=0.99,
                status=RouteStatus.ACTIVE,
            )
            ensure(route.route_id == "r-us-eu", route.route_id)
            ensure(route.status == RouteStatus.ACTIVE, f"status {route.status}")
            ensure(len(route.path_hops) == 3, f"hops {route.path_hops}")
            ok(
                f"route {route.source_hub}→{route.destination_hub} lat={route.latency_ms}ms"
            )

        with scenario(
            "BlockchainMessage construction",
            "fabric.blockchain_msg",
        ):
            msg = BlockchainMessage(
                sender_id="hub-us",
                recipient_id="hub-eu",
                message_type="federation.coord",
                priority=MessagePriority.HIGH,
                delivery_guarantee=DeliveryGuarantee.AT_LEAST_ONCE,
                payload=b'{"op":"sync"}',
                route_id=route.route_id,
                processing_fee=10,
                blockchain_record=True,
                metadata={"curriculum": True},
            )
            ensure(msg.sender_id == "hub-us", msg.sender_id)
            ensure(msg.recipient_id == "hub-eu", msg.recipient_id)
            ensure(msg.payload == b'{"op":"sync"}', msg.payload)
            ensure(msg.blockchain_record is True, "record flag")
            ensure(
                msg.message_id.startswith("msg_") or len(msg.message_id) > 0,
                msg.message_id,
            )
            ok(f"msg_id={msg.message_id} fee={msg.processing_fee}")

        with scenario(
            "validation fail-closed on empty sender",
            "fabric.blockchain_msg",
        ):
            raised = False
            try:
                BlockchainMessage(
                    sender_id="",
                    recipient_id="x",
                    message_type="bad",
                )
            except ValueError as exc:
                raised = True
                step(f"ValueError: {exc}")
            ensure(raised, "empty sender must raise")
            ok("invalid message rejected")

        with scenario(
            "federation manager import (non-claim depth)",
            "fabric.blockchain_msg",
            "fabric.hubs",
        ):
            from mpreg.fabric.blockchain_message_federation import (
                BlockchainFederationBridge,
            )

            ensure(
                BlockchainFederationBridge is not None,
                "federation class missing",
            )
            step(
                "BlockchainFederationBridge needs live hub mesh + queue — "
                "taught as library types here; multi_region / edge tours cover "
                "operational federation"
            )
            ok("BlockchainFederationBridge importable")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())

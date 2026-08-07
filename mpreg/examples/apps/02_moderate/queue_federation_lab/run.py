"""L2 queue_federation_lab — queue federation message types + manager stats."""

from __future__ import annotations

import asyncio

from mpreg.core.message_queue import DeliveryGuarantee as QueueDeliveryGuarantee
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.queue_messages import (
    QueueFederationAck,
    QueueFederationKind,
    QueueFederationRequest,
    QueueFederationSubscription,
    queue_message_from_dict,
)


async def main() -> None:
    with app_run(
        "queue_federation_lab",
        "Queue Federation Lab — wire types + round-trip codec",
        level="L2",
    ):
        with scenario(
            "QueueFederationKind enum surface",
            "fabric.queue_fed",
        ):
            kinds = {k.value for k in QueueFederationKind}
            ensure(len(kinds) >= 2, f"expected multiple kinds got {kinds}")
            ok(f"kinds={sorted(kinds)}")

        with scenario(
            "request → dict → from_dict round-trip",
            "fabric.queue_fed",
        ):
            req = QueueFederationRequest(
                request_id="r1",
                queue_name="orders",
                topic="order.created",
                payload={"sku": "A1"},
                delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
                source_cluster="us",
                target_cluster="eu",
            )
            payload = req.to_dict()
            ensure(isinstance(payload, dict), "to_dict must be dict")
            ensure(
                payload.get("kind") == QueueFederationKind.REQUEST.value
                or "request" in str(payload.get("kind")),
                f"kind {payload.get('kind')}",
            )
            back = QueueFederationRequest.from_dict(payload)
            ensure(back.request_id == "r1", f"id {back.request_id}")
            ensure(back.queue_name == "orders", f"queue {back.queue_name}")
            ensure(back.topic == "order.created", f"topic {back.topic}")
            ok(f"request round-trip queue={back.queue_name} topic={back.topic}")

        with scenario(
            "ack + subscription codecs",
            "fabric.queue_fed",
        ):
            ack = QueueFederationAck(
                ack_token="tok-1",
                message_id="r1",
                acknowledging_cluster="eu",
                acknowledging_subscriber="worker-1",
                success=True,
            )
            ack_d = ack.to_dict()
            ack2 = QueueFederationAck.from_dict(ack_d)
            ensure(ack2.success is True, f"ack success {ack2.success}")
            ensure(ack2.ack_token == "tok-1", f"token {ack2.ack_token}")

            sub = QueueFederationSubscription(
                subscription_id="s1",
                subscriber_id="sub-1",
                queue_pattern="orders.*",
                topic_pattern="order.#",
                delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
                source_cluster="us",
            )
            sub_d = sub.to_dict(kind=QueueFederationKind.SUBSCRIBE)
            ensure(isinstance(sub_d, dict), "sub dict")
            ensure(sub_d.get("subscription_id") == "s1", f"sub {sub_d}")
            ok(f"ack success + sub keys={sorted(sub_d.keys())[:8]}")

        with scenario(
            "queue_message_from_dict dispatch",
            "fabric.queue_fed",
        ):
            req = QueueFederationRequest(
                request_id="r2",
                queue_name="q",
                topic="t",
                payload={},
                delivery_guarantee=QueueDeliveryGuarantee.AT_LEAST_ONCE,
                source_cluster="a",
                target_cluster="b",
            )
            msg = queue_message_from_dict(req.to_dict())
            ensure(isinstance(msg, QueueFederationRequest), f"got {type(msg)}")
            ok(f"dispatch → {type(msg).__name__}")

        with scenario(
            "manager class importable (live mesh non-claim depth)",
            "fabric.queue_fed",
        ):
            from mpreg.fabric.queue_federation import (
                FabricQueueFederationManager,
                FabricQueueStatistics,
            )

            ensure(
                FabricQueueFederationManager is not None,
                "manager missing",
            )
            stats = FabricQueueStatistics()
            ensure(stats is not None, "stats")
            step(
                "FabricQueueFederationManager needs full fabric messenger + "
                "queue manager wiring — taught as library surface here; live "
                "cross-cluster queue mesh is multi_region_shop / tier3 territory"
            )
            ok("manager + stats types importable")

        await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main())

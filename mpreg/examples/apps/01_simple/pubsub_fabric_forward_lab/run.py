"""L1 pubsub_fabric_forward_lab — PubSubForwardingMetadata hop/path guards."""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.pubsub_forwarding import (
    FABRIC_PUBSUB_FORWARDING_KEY,
    PubSubForwardingMetadata,
)

async def main() -> None:
    with app_run(
        "pubsub_fabric_forward_lab",
        "PubSub Fabric Forward — hop metadata + path guards",
        level="L1",
    ):
        with scenario(
            "origin + empty path hop_count",
            "pubsub.fabric_forward",
        ):
            meta = PubSubForwardingMetadata(origin_node="node-a")
            ensure(meta.hop_count == 0, f"hop={meta.hop_count}")
            ensure(meta.can_forward() is True, "unlimited should forward")
            ensure(meta.has_visited("node-a") is False, "origin not in path yet")
            ok("origin-only metadata")

        with scenario(
            "with_hop builds routing_path",
            "pubsub.fabric_forward",
        ):
            m0 = PubSubForwardingMetadata(origin_node="node-a", max_hops=3)
            m1 = m0.with_hop("node-a")
            m2 = m1.with_hop("node-b")
            m3 = m2.with_hop("node-c")
            ensure(m1.routing_path == ("node-a",), f"p1={m1.routing_path}")
            ensure(m2.routing_path == ("node-a", "node-b"), f"p2={m2.routing_path}")
            ensure(m3.hop_count == 2, f"hops={m3.hop_count}")
            ensure(m3.has_visited("node-b") is True, "visited b")
            ensure(m3.can_forward() is True, "2 < max 3")
            ok(f"path={m3.routing_path} hops={m3.hop_count}")

        with scenario(
            "max_hops blocks further forward",
            "pubsub.fabric_forward",
        ):
            m = PubSubForwardingMetadata(origin_node="a", max_hops=1)
            m = m.with_hop("a").with_hop("b")  # hop_count=1
            ensure(m.hop_count == 1, f"hops={m.hop_count}")
            ensure(m.can_forward() is False, "at max should not forward")
            ok("max_hops=1 fail-closed")

        with scenario(
            "headers round-trip fabric_forwarding key",
            "pubsub.fabric_forward",
            "pubsub.headers",
        ):
            meta = PubSubForwardingMetadata(
                origin_node="edge-1",
                routing_path=("edge-1", "hub-1"),
                max_hops=5,
            )
            headers = meta.with_headers({"x-trace": "fwd-lab"})
            ensure(FABRIC_PUBSUB_FORWARDING_KEY in headers, "key missing")
            ensure(headers["x-trace"] == "fwd-lab", "other headers preserved")
            parsed = PubSubForwardingMetadata.from_headers(headers)
            ensure(parsed is not None, "parse None")
            ensure(parsed.origin_node == "edge-1", f"origin={parsed.origin_node}")
            ensure(
                parsed.routing_path == ("edge-1", "hub-1"),
                f"path={parsed.routing_path}",
            )
            ensure(parsed.max_hops == 5, f"max={parsed.max_hops}")
            ok(f"round-trip key={FABRIC_PUBSUB_FORWARDING_KEY}")

        with scenario(
            "from_headers rejects garbage",
            "pubsub.fabric_forward",
        ):
            ensure(
                PubSubForwardingMetadata.from_headers({}) is None,
                "empty should None",
            )
            ensure(
                PubSubForwardingMetadata.from_headers(
                    {FABRIC_PUBSUB_FORWARDING_KEY: {"no_origin": True}}
                )
                is None,
                "missing origin should None",
            )
            ensure(
                PubSubForwardingMetadata.from_headers(
                    {FABRIC_PUBSUB_FORWARDING_KEY: "not-a-map"}
                )
                is None,
                "bad type should None",
            )
            ok("fail-closed parse")

        with scenario(
            "idempotent hop when last node repeats",
            "pubsub.fabric_forward",
        ):
            m = PubSubForwardingMetadata(origin_node="a", routing_path=("a", "b"))
            m2 = m.with_hop("b")
            ensure(m2.routing_path == ("a", "b"), f"dup hop path={m2.routing_path}")
            ok("repeat last hop is no-op")

        await asyncio.sleep(0)
        ok("pubsub_fabric_forward_lab complete")

if __name__ == "__main__":
    asyncio.run(main())

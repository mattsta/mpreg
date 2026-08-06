"""L1 discovery_signatures_lab — HMAC summary + gossip signatures (Phase K)."""

from __future__ import annotations

import asyncio

from mpreg.core.discovery_signatures import (
    SIGNATURE_KEY,
    sign_summary,
    verify_summary,
)
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.fabric.gossip_signatures import (
    SIGNATURE_KEY as GOSSIP_SIG_KEY,
)
from mpreg.fabric.gossip_signatures import (
    sign_gossip_payload,
    verify_gossip_payload,
)

async def main() -> None:
    with app_run(
        "discovery_signatures_lab",
        "Discovery + Gossip HMAC Signatures",
        level="L1",
    ):
        secret = "curriculum-hmac-secret"

        with scenario("sign_summary attaches mpreg_summary_sig", "disco.signatures"):
            payload = {
                "cluster_id": "lab",
                "node_count": 3,
                "namespaces": ["app", "api"],
            }
            signed = sign_summary(payload, secret)
            ensure(SIGNATURE_KEY in signed, f"missing {SIGNATURE_KEY}")
            ensure(
                signed[SIGNATURE_KEY].startswith("hmac-sha256:"), signed[SIGNATURE_KEY]
            )
            ensure("cluster_id" in signed, "body lost")
            ok(f"sig={signed[SIGNATURE_KEY][:32]}…")

        with scenario("verify_summary accept/reject", "disco.signatures"):
            signed = sign_summary({"v": 1}, secret)
            ensure(verify_summary(signed, secret) is True, "valid should pass")
            ensure(verify_summary(signed, "wrong") is False, "wrong secret must fail")
            tampered = dict(signed)
            tampered["v"] = 2
            ensure(verify_summary(tampered, secret) is False, "tamper must fail")
            ensure(verify_summary({"v": 1}, "") is True, "empty secret = verify off")
            ok("accept/reject/tamper/off paths")

        with scenario(
            "sign_gossip_payload envelope HMAC", "disco.signatures", "fabric.gossip"
        ):
            env = {"type": "membership", "peers": ["a", "b"]}
            signed = sign_gossip_payload(env, secret)
            ensure(GOSSIP_SIG_KEY in signed, f"missing {GOSSIP_SIG_KEY}")
            ensure(verify_gossip_payload(signed, secret) is True, "gossip valid")
            ensure(
                verify_gossip_payload(signed, "nope") is False, "gossip wrong secret"
            )
            ok(f"gossip sig present key={GOSSIP_SIG_KEY}")

        with scenario(
            "GossipMessage hop/TTL/propagation model",
            "fabric.gossip",
        ):
            from mpreg.fabric.gossip import GossipMessage, GossipMessageType

            msg = GossipMessage(
                message_id="lab-g1",
                message_type=GossipMessageType.STATE_UPDATE,
                sender_id="node-a",
                payload={"key": "lab.peers", "value": ["a", "b"], "version": 1},
                ttl=3,
                hop_count=0,
                max_hops=2,
            )
            ensure(msg.can_propagate() is True, "fresh msg should propagate")
            ensure(msg.is_expired() is False, "fresh msg not expired")
            ensure(bool(msg.digest) and bool(msg.checksum), "digest/checksum missing")
            hopped = msg.prepare_for_propagation("node-b")
            ensure(hopped.hop_count == 1, f"hop={hopped.hop_count}")
            ensure(hopped.ttl == 2, f"ttl={hopped.ttl}")
            ensure(hopped.sender_id == "node-b", f"sender={hopped.sender_id}")
            ensure(
                "node-b" in hopped.propagation_path, f"path={hopped.propagation_path}"
            )
            wire = msg.to_dict()
            back = GossipMessage.from_dict(wire)
            ensure(back.message_id == "lab-g1", f"roundtrip id={back.message_id}")
            ensure(
                back.message_type == GossipMessageType.STATE_UPDATE,
                f"type={back.message_type}",
            )
            step(f"digest={msg.digest} hops={hopped.hop_count}/{hopped.max_hops}")
            ok("GossipMessage hop/TTL + dict roundtrip")

        with scenario(
            "settings knobs for live signing",
            "disco.signatures",
            "boot.settings",
        ):
            from mpreg.core.config import MPREGSettings

            s = MPREGSettings(
                discovery_summary_signing_secret=secret,
                fabric_gossip_require_hmac=True,
                fabric_gossip_hmac_secret=secret,
            )
            ensure(s.discovery_summary_signing_secret == secret, "summary secret")
            ensure(s.fabric_gossip_require_hmac is True, "require hmac")
            ensure(s.fabric_gossip_hmac_secret == secret, "gossip secret")
            step(
                "live export: discovery_summary_export_enabled + signing secret; "
                "gossip: fabric_gossip_require_hmac on both peers"
            )
            ok("settings surface for production enablement")

        with scenario("unsigned payload fails when secret set", "disco.signatures"):
            ensure(verify_summary({"x": 1}, secret) is False, "unsigned must fail")
            ensure(
                verify_gossip_payload({"x": 1}, secret) is False, "unsigned gossip fail"
            )
            ok("fail-closed without signature field")

        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())

"""L2 shared_audit_mesh — multi-node G-Set audit visibility (scope=cluster).

Teaches SharedAuditStore + SharedAuditReplicator with the same in-process
transport used in unit tests (production binds FabricSharedAuditTransport).

Non-claims: not SIEM, not BFT, bounded watermark window.
"""

from __future__ import annotations

import asyncio

from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step
from mpreg.server_pkg.shared_audit import (
    InProcessSharedAuditTransport,
    SharedAuditReplicator,
    SharedAuditStore,
    build_audit_response,
    record_from_mgmt_entry,
)

def _node(node_id: str, transport: InProcessSharedAuditTransport):
    store = SharedAuditStore(
        cluster_id="shared-audit-mesh",
        local_node=node_id,
        max_entries=500,
    )

    def peers():
        return [p for p in transport.peers if p != node_id]

    rep = SharedAuditReplicator(
        store=store,
        node_id=node_id,
        cluster_id="shared-audit-mesh",
        transport=transport,
        peer_list=peers,
        reconcile_interval_s=60.0,
        gossip_targets=3,
    )
    transport.register(rep)
    return store, rep

async def main() -> None:
    with app_run(
        "shared_audit_mesh",
        "Shared Audit Mesh — cluster scope G-Set visibility",
        level="L2",
    ):
        transport = InProcessSharedAuditTransport()
        sa, ra = _node("ws://a", transport)
        sb, rb = _node("ws://b", transport)
        sc, rc = _node("ws://c", transport)

        with scenario(
            "drain on A; cluster audit on B",
            "ops.mgmt_drain",
            "ops.mgmt_audit",
            "ops.shared_audit",
        ):
            step("mint drain record on A and epidemic DELTA")
            rec = record_from_mgmt_entry(
                event="node_drain",
                timestamp=1.0,
                actor="curriculum",
                success=True,
                detail={"draining": True, "reason": "demo"},
                cluster_id="shared-audit-mesh",
                origin_node="ws://a",
                origin_url="ws://a",
            )
            sa.insert_and_persist(rec)
            ra.publish(rec)
            await ra._flush_outbound()
            ensure(sb.get("shared-audit-mesh", rec.entry_id) is not None, "B miss")
            ensure(sc.get("shared-audit-mesh", rec.entry_id) is not None, "C miss")

            body = build_audit_response(
                store=sb,
                local_entries=[],
                scope="cluster",
                shared_enabled=True,
                self_node="ws://b",
                limit=50,
            )
            ensure(body["scope"] == "cluster", f"scope {body}")
            ensure(body["mutation_count"] >= 1, f"empty {body}")
            ids = {m.get("entry_id") for m in body["mutations"]}
            ensure(rec.entry_id in ids, f"entry missing {ids}")
            ok(f"B cluster view has entry_id={rec.entry_id[:12]}…")

        with scenario(
            "digest + PULL repairs dropped DELTA",
            "ops.shared_audit",
        ):
            step("drop DELTA then repair via digest/PULL")
            transport.drop_types.add("mgmt_audit_delta")
            rec2 = record_from_mgmt_entry(
                event="node_detach",
                timestamp=2.0,
                actor="curriculum",
                success=True,
                detail={"peer": "ws://x"},
                cluster_id="shared-audit-mesh",
                origin_node="ws://a",
                origin_url="ws://a",
            )
            sa.insert(rec2)
            ra.publish(rec2)
            await ra._flush_outbound()
            ensure(sb.get("shared-audit-mesh", rec2.entry_id) is None, "should miss")
            transport.drop_types.clear()
            await rb._on_digest(ra.build_digest())
            ensure(
                sb.get("shared-audit-mesh", rec2.entry_id) is not None,
                "PULL repair failed",
            )
            ok("anti-entropy PULL restored detach event on B")

        with scenario("scope=local default + non-claims", "ops.mgmt_audit"):
            local = build_audit_response(
                store=None,
                local_entries=[{"event": "local_only", "timestamp": 0.0}],
                scope="local",
                shared_enabled=True,
                self_node="ws://c",
            )
            ensure(local["scope"] == "local", "local scope")
            cluster_off = build_audit_response(
                store=None,
                local_entries=[],
                scope="cluster",
                shared_enabled=False,
            )
            ensure(cluster_off.get("error") == "shared_audit_disabled", "must refuse")
            step("not SIEM / not BFT / bounded watermark window")
            ok("honesty + default local scope")

        _ = (sc, rc)
        await asyncio.sleep(0)

if __name__ == "__main__":
    asyncio.run(main())

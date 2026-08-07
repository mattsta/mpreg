"""Multi-node shared audit integration (INV-SHARED-AUDIT-01 L2).

Uses live MPREGServer processes when fabric gossip is available; falls back
to proving the server wiring path (store + replicator boot) + in-process mesh
equivalence for the G-Set visibility claim.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from mpreg.core.config import MPREGSettings
from mpreg.core.port_allocator import port_range_context
from mpreg.server import MPREGServer
from mpreg.server_pkg.mgmt_mutations import apply_node_drain
from mpreg.server_pkg.shared_audit import (
    InProcessSharedAuditTransport,
    SharedAuditReplicator,
    SharedAuditStore,
    build_audit_response,
    record_from_mgmt_entry,
)


@pytest.mark.asyncio
async def test_three_node_server_boot_shared_store_and_local_publish() -> None:
    """Each node with mgmt_audit_shared_enabled owns a SharedAuditStore;
    drain publishes into the origin store with ULID entry_id."""
    with port_range_context(6, "servers") as ports:
        ws = ports[0:3]
        mon = ports[3:6]
        audit_dir = tempfile.mkdtemp(prefix="mpreg-audit-int-")
        url_a = f"ws://127.0.0.1:{ws[0]}"

        def settings(i: int, peers: list[str] | None = None) -> MPREGSettings:
            return MPREGSettings(
                host="127.0.0.1",
                port=ws[i],
                name=f"A{i}",
                cluster_id="audit-int",
                resources={f"r{i}"},
                peers=peers,
                log_level="ERROR",
                gossip_interval=0.5,
                monitoring_enabled=True,
                monitoring_port=mon[i],
                mgmt_audit_path=str(Path(audit_dir) / f"n{i}.jsonl"),
                mgmt_audit_shared_enabled=True,
                mgmt_audit_shared_reconcile_interval_s=0.4,
            )

        servers = [
            MPREGServer(settings(0)),
            MPREGServer(settings(1, [url_a])),
            MPREGServer(settings(2, [url_a])),
        ]
        tasks = [asyncio.create_task(s.server()) for s in servers]
        try:
            await asyncio.sleep(1.2)
            for s in servers:
                store = getattr(s, "_shared_audit_store", None)
                assert store is not None, f"missing store on {s.settings.name}"
                assert store.cluster_id == "audit-int"

            apply_node_drain(
                servers[0], draining=True, actor="integration", reason="int-test"
            )
            store_a = servers[0]._shared_audit_store
            assert store_a.size() >= 1
            rec = store_a.snapshot()[-1]
            assert rec.event == "node_drain"
            assert rec.entry_id
            assert rec.gossip_eligible is True
            assert rec.cluster_id == "audit-int"

            # Local ring mirrors origin
            local = servers[0]._mgmt_audit_log.snapshot()
            assert any(e.get("event") == "node_drain" for e in local)

            # Snapshot API cluster scope
            snap = servers[0]._mgmt_audit_snapshot(scope="cluster", limit=50)
            assert isinstance(snap, dict)
            assert snap.get("scope") == "cluster"
            assert snap.get("shared_enabled") is True
            assert snap.get("mutation_count", 0) >= 1

            # Default local scope
            local_snap = servers[1]._mgmt_audit_snapshot(scope="local", limit=50)
            assert isinstance(local_snap, dict)
            assert local_snap.get("scope") == "local"

            # Flag-off node refuses cluster
            off = MPREGServer(
                MPREGSettings(
                    host="127.0.0.1",
                    port=0,
                    name="off",
                    cluster_id="x",
                    log_level="ERROR",
                    monitoring_enabled=False,
                    mgmt_audit_shared_enabled=False,
                )
            )
            try:
                body = off._mgmt_audit_snapshot(scope="cluster", limit=10)
                assert isinstance(body, dict)
                assert body.get("error") == "shared_audit_disabled"
            finally:
                # off never started
                pass
        finally:
            for s in servers:
                try:
                    await s.shutdown()
                except Exception:  # noqa: BLE001
                    pass
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_inprocess_mesh_rejoin_anti_entropy() -> None:
    """Node joins late; digest+PULL backfills history still above watermarks."""
    transport = InProcessSharedAuditTransport()

    def make(nid: str):
        store = SharedAuditStore(cluster_id="c1", local_node=nid, max_entries=200)

        def peers():
            return [p for p in transport.peers if p != nid]

        rep = SharedAuditReplicator(
            store=store,
            node_id=nid,
            cluster_id="c1",
            transport=transport,
            peer_list=peers,
            reconcile_interval_s=60.0,
            gossip_targets=5,
        )
        transport.register(rep)
        return store, rep

    sa, ra = make("a")
    sb, _rb = make("b")

    # A produces history while C is absent
    ids = []
    for i in range(8):
        rec = record_from_mgmt_entry(
            event="node_drain",
            timestamp=float(i),
            actor="ops",
            success=True,
            detail={"i": i},
            cluster_id="c1",
            origin_node="a",
            entry_id=f"hist-{i}",
        )
        sa.insert(rec)
        ra.publish(rec)
        ids.append(rec.entry_id)
    await ra._flush_outbound()
    for eid in ids:
        assert sb.get("c1", eid) is not None

    # Late joiner C
    sc, rc = make("c")
    assert sc.size() == 0
    await rc._on_digest(ra.build_digest())
    for eid in ids:
        assert sc.get("c1", eid) is not None, f"late joiner missing {eid}"

    body = build_audit_response(
        store=sc,
        local_entries=[],
        scope="cluster",
        shared_enabled=True,
        self_node="c",
    )
    assert body["mutation_count"] >= 8


@pytest.mark.asyncio
async def test_partition_heal_via_digest() -> None:
    """DELTA dropped during partition; heal with digest exchange."""
    transport = InProcessSharedAuditTransport()

    def make(nid: str):
        store = SharedAuditStore(cluster_id="c1", local_node=nid)

        def peers():
            return [p for p in transport.peers if p != nid]

        rep = SharedAuditReplicator(
            store=store,
            node_id=nid,
            cluster_id="c1",
            transport=transport,
            peer_list=peers,
            reconcile_interval_s=60.0,
        )
        transport.register(rep)
        return store, rep

    sa, ra = make("a")
    sb, rb = make("b")
    transport.drop_types.add("mgmt_audit_delta")
    rec = record_from_mgmt_entry(
        event="detach",
        timestamp=1.0,
        actor=None,
        success=True,
        detail={},
        cluster_id="c1",
        origin_node="a",
        entry_id="part-1",
    )
    sa.insert(rec)
    ra.publish(rec)
    await ra._flush_outbound()
    assert sb.get("c1", "part-1") is None

    # heal
    transport.drop_types.clear()
    await rb._on_digest(ra.build_digest())
    assert sb.get("c1", "part-1") is not None

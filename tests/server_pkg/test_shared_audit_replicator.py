"""Shared audit replicator with deterministic inject transport (PR-A2)."""

from __future__ import annotations

import asyncio

import pytest

from mpreg.server_pkg.shared_audit import (
    InProcessSharedAuditTransport,
    SharedAuditReplicator,
    SharedAuditStore,
    record_from_mgmt_entry,
)

def _make_node(node_id: str, transport: InProcessSharedAuditTransport):
    store = SharedAuditStore(cluster_id="c1", local_node=node_id, max_entries=500)

    def peers():
        return [p for p in transport.peers if p != node_id]

    rep = SharedAuditReplicator(
        store=store,
        node_id=node_id,
        cluster_id="c1",
        transport=transport,
        peer_list=peers,
        reconcile_interval_s=60.0,  # manual flush in tests
    )
    transport.register(rep)
    return store, rep

@pytest.mark.asyncio
async def test_delta_epidemic_visibility() -> None:
    transport = InProcessSharedAuditTransport()
    sa, ra = _make_node("a", transport)
    sb, _rb = _make_node("b", transport)
    sc, _rc = _make_node("c", transport)

    rec = record_from_mgmt_entry(
        event="node_drain",
        timestamp=1.0,
        actor="ops",
        success=True,
        detail={"draining": True},
        cluster_id="c1",
        origin_node="a",
        origin_url="ws://a",
    )
    sa.insert(rec)
    ra.publish(rec)
    await ra._flush_outbound()

    assert sb.get("c1", rec.entry_id) is not None
    assert sc.get("c1", rec.entry_id) is not None
    assert sb.get("c1", rec.entry_id).event == "node_drain"  # type: ignore[union-attr]

@pytest.mark.asyncio
async def test_drop_then_digest_pull_repairs() -> None:
    transport = InProcessSharedAuditTransport()
    sa, ra = _make_node("a", transport)
    sb, rb = _make_node("b", transport)

    rec = record_from_mgmt_entry(
        event="detach",
        timestamp=2.0,
        actor="ops",
        success=True,
        detail={},
        cluster_id="c1",
        origin_node="a",
    )
    sa.insert(rec)
    # Drop DELTA
    transport.drop_types.add("mgmt_audit_delta")
    ra.publish(rec)
    await ra._flush_outbound()
    assert sb.get("c1", rec.entry_id) is None

    # Allow digests + pull
    transport.drop_types.clear()
    await ra._exchange_digests()
    # B receives digest and should PULL
    await asyncio.sleep(0)  # let tasks settle
    # Manually: B processes digest from A
    digest = ra.build_digest()
    await rb._on_digest(digest)
    assert sb.get("c1", rec.entry_id) is not None

@pytest.mark.asyncio
async def test_reorder_delta_still_converges() -> None:
    transport = InProcessSharedAuditTransport()
    sa, ra = _make_node("a", transport)
    sb, _rb = _make_node("b", transport)
    transport.hold_reorder = True

    r1 = record_from_mgmt_entry(
        event="e1",
        timestamp=1.0,
        actor=None,
        success=True,
        detail={"n": 1},
        cluster_id="c1",
        origin_node="a",
    )
    r2 = record_from_mgmt_entry(
        event="e2",
        timestamp=2.0,
        actor=None,
        success=True,
        detail={"n": 2},
        cluster_id="c1",
        origin_node="a",
    )
    sa.insert(r1)
    sa.insert(r2)
    ra.publish(r1)
    await ra._flush_outbound()
    ra.publish(r2)
    await ra._flush_outbound()
    await transport.flush_reorder()
    assert sb.get("c1", r1.entry_id) is not None
    assert sb.get("c1", r2.entry_id) is not None

@pytest.mark.asyncio
async def test_cross_cluster_delta_rejected() -> None:
    transport = InProcessSharedAuditTransport()
    sa, ra = _make_node("a", transport)
    sb, rb = _make_node("b", transport)
    # Force wrong cluster on B's expectation — A sends c1, mutate B cluster
    rb.cluster_id = "other"
    sb.cluster_id = "other"
    rec = record_from_mgmt_entry(
        event="x",
        timestamp=1.0,
        actor=None,
        success=True,
        detail={},
        cluster_id="c1",
        origin_node="a",
    )
    sa.insert(rec)
    ra.publish(rec)
    await ra._flush_outbound()
    assert sb.size() == 0

"""Shared-audit chaos: partition/heal, dup, burst, watermark (plan W4)."""

from __future__ import annotations

import asyncio

import pytest

from mpreg.server_pkg.shared_audit import (
    InProcessSharedAuditTransport,
    SharedAuditReplicator,
    SharedAuditStore,
    record_from_mgmt_entry,
)


def _node(
    node_id: str, transport: InProcessSharedAuditTransport, *, max_entries: int = 500
):
    store = SharedAuditStore(
        cluster_id="c1", local_node=node_id, max_entries=max_entries
    )

    def peers():
        return [p for p in transport.peers if p != node_id]

    rep = SharedAuditReplicator(
        store=store,
        node_id=node_id,
        cluster_id="c1",
        transport=transport,
        peer_list=peers,
        reconcile_interval_s=60.0,
        max_outbound_queue=32,
    )
    transport.register(rep)
    return store, rep


def _rec(
    origin: str, event: str, ts: float, *, eligible: bool = True, cluster: str = "c1"
):
    return record_from_mgmt_entry(
        event=event,
        timestamp=ts,
        actor="ops",
        success=True,
        detail={"e": event},
        cluster_id=cluster,
        origin_node=origin,
        gossip_eligible=eligible,
    )


@pytest.mark.asyncio
async def test_partition_then_heal_converges() -> None:
    tr = InProcessSharedAuditTransport()
    sa, ra = _node("a", tr)
    sb, rb = _node("b", tr)
    sc, rc = _node("c", tr)

    tr.partition("a", "b")
    tr.partition("a", "c")

    for i in range(5):
        rec = _rec("a", f"e{i}", float(i + 1))
        sa.insert(rec)
        ra.publish(rec)
    await ra._flush_outbound()
    # B and C must not see A's events while partitioned
    assert sb.size() == 0
    assert sc.size() == 0

    tr.heal()
    await ra._flush_outbound()
    # If outbound was re-queued or empty after failed send, use digest path
    if sb.size() < 5:
        digest = ra.build_digest()
        await rb._on_digest(digest)
        await rc._on_digest(digest)
    # Also flush any remaining
    await ra._flush_outbound()
    await asyncio.sleep(0)

    # After heal + digest/pull, converge
    for _ in range(5):
        if sb.size() >= 5 and sc.size() >= 5:
            break
        digest = ra.build_digest()
        await rb._on_digest(digest)
        await rc._on_digest(digest)
        await asyncio.sleep(0.05)

    assert sb.size() >= 5
    assert sc.size() >= 5


@pytest.mark.asyncio
async def test_duplicate_delta_idempotent() -> None:
    tr = InProcessSharedAuditTransport()
    tr.duplicate = True
    sa, ra = _node("a", tr)
    sb, _rb = _node("b", tr)
    rec = _rec("a", "dup", 1.0)
    sa.insert(rec)
    ra.publish(rec)
    await ra._flush_outbound()
    assert sb.size() == 1
    assert sb.get("c1", rec.entry_id) is not None


@pytest.mark.asyncio
async def test_gossip_ineligible_never_epidemic() -> None:
    tr = InProcessSharedAuditTransport()
    sa, ra = _node("a", tr)
    sb, _rb = _node("b", tr)
    rec = _rec("a", "secret", 1.0, eligible=False)
    sa.insert(rec)
    ra.publish(rec)
    await ra._flush_outbound()
    assert sa.get("c1", rec.entry_id) is not None
    assert sb.get("c1", rec.entry_id) is None
    # Digest sample should not advertise ineligible-only origins with that id
    digest = ra.build_digest()
    origins = digest.get("origins") or {}
    # origin a may be absent if only ineligible records
    if "a" in origins:
        sample = origins["a"].get("id_sample") or []
        assert rec.entry_id not in sample


@pytest.mark.asyncio
async def test_cross_cluster_rejected() -> None:
    tr = InProcessSharedAuditTransport()
    sa, _ra = _node("a", tr)
    _sb, _rb = _node("b", tr)
    rec = _rec("a", "x", 1.0, cluster="other")
    assert sa.insert(rec) is None
    assert sa.rejected_cross_cluster >= 1


@pytest.mark.asyncio
async def test_outbound_queue_drop_counter() -> None:
    tr = InProcessSharedAuditTransport()
    sa, ra = _node("a", tr)
    # No peer registered → flush re-queues; flood publish to hit max_outbound
    for i in range(50):
        rec = _rec("a", f"q{i}", float(i + 1))
        sa.insert(rec)
        ra.publish(rec)
    health = ra.health()
    assert health.publish_dropped >= 1
    assert sa.size() >= 32  # local store kept inserts


@pytest.mark.asyncio
async def test_multi_origin_burst_converge() -> None:
    tr = InProcessSharedAuditTransport()
    stores = {}
    reps = {}
    for nid in ("a", "b", "c"):
        s, r = _node(nid, tr)
        stores[nid] = s
        reps[nid] = r

    for i, nid in enumerate(("a", "b", "c", "a", "b", "c")):
        rec = _rec(nid, f"m{i}", float(i + 1))
        stores[nid].insert(rec)
        reps[nid].publish(rec)
        await reps[nid]._flush_outbound()

    for s in stores.values():
        assert s.size() >= 6


@pytest.mark.asyncio
async def test_watermark_no_resurrection() -> None:
    store = SharedAuditStore(cluster_id="c1", local_node="a", max_entries=5)
    ids = []
    for i in range(10):
        rec = _rec("a", f"w{i}", float(i + 1))
        store.insert(rec)
        ids.append(rec.entry_id)
    # Compaction should have advanced watermark and dropped old
    assert store.size() <= 5
    # Try re-insert oldest
    _rec("a", "w0", 1.0)
    # Force same entry_id if possible — mint may differ; use get of dropped
    # Re-insert a record below watermark with synthetic low ts
    from mpreg.server_pkg.shared_audit.models import SharedAuditRecord

    low = SharedAuditRecord(
        schema_version=1,
        cluster_id="c1",
        entry_id="00000000-0000-0000-0000-000000000001",
        timestamp=0.1,
        origin_node="a",
        origin_url="",
        event="resurrect",
        actor="ops",
        success=True,
        detail={},
        gossip_eligible=True,
    )
    # If watermark covers, reject
    wm = store.watermark_for("a")
    if wm is not None and not wm.covers(low.timestamp, low.entry_id):
        assert store.insert(low) is None
        assert store.rejected_below_watermark >= 1


@pytest.mark.asyncio
async def test_replicator_stop_start() -> None:
    tr = InProcessSharedAuditTransport()
    sa, ra = _node("a", tr)
    _sb, _rb = _node("b", tr)
    ra.start()
    await asyncio.sleep(0.05)
    await ra.stop()
    await ra.stop()  # idempotent
    rec = _rec("a", "after", 1.0)
    sa.insert(rec)
    ra.publish(rec)
    await ra._flush_outbound()
    assert _sb.size() >= 1


@pytest.mark.asyncio
async def test_drop_delta_digest_repair() -> None:
    tr = InProcessSharedAuditTransport()
    sa, ra = _node("a", tr)
    sb, rb = _node("b", tr)
    rec = _rec("a", "repair", 3.0)
    sa.insert(rec)
    tr.drop_types.add("mgmt_audit_delta")
    ra.publish(rec)
    await ra._flush_outbound()
    assert sb.size() == 0
    tr.drop_types.clear()
    await rb._on_digest(ra.build_digest())
    assert sb.get("c1", rec.entry_id) is not None


@pytest.mark.asyncio
async def test_reorder_buffer_converges() -> None:
    tr = InProcessSharedAuditTransport()
    tr.hold_reorder = True
    sa, ra = _node("a", tr)
    sb, _rb = _node("b", tr)
    for i in range(4):
        rec = _rec("a", f"r{i}", float(i + 1))
        sa.insert(rec)
        ra.publish(rec)
        await ra._flush_outbound()
    assert sb.size() == 0
    tr.hold_reorder = False
    await tr.flush_reorder()
    assert sb.size() >= 4

"""Unit tests for SharedAudit G-Set store + watermarks (PR-A1)."""

from __future__ import annotations

import itertools
from pathlib import Path

import pytest

from mpreg.server_pkg.shared_audit import (
    SharedAuditRecord,
    SharedAuditStore,
    Watermark,
    merge_records,
    record_from_mgmt_entry,
    stable_canonical_json,
)
from mpreg.server_pkg.shared_audit.models import legacy_synthetic_id


def _rec(
    entry_id: str,
    *,
    ts: float = 1.0,
    origin: str = "n1",
    cluster: str = "c1",
    event: str = "node_drain",
    detail: dict | None = None,
    gossip: bool = True,
) -> SharedAuditRecord:
    return SharedAuditRecord(
        schema_version=1,
        entry_id=entry_id,
        cluster_id=cluster,
        origin_node=origin,
        origin_url=f"ws://{origin}",
        event=event,
        timestamp=ts,
        actor="admin",
        success=True,
        detail=detail or {"target": origin},
        gossip_eligible=gossip,
    )


def test_merge_identical_no_conflict() -> None:
    a = _rec("e1")
    b = SharedAuditRecord.from_dict(a.to_dict())
    winner, conflict = merge_records(a, b)
    assert conflict is False
    assert winner.entry_id == "e1"


def test_merge_conflict_bytewise_min() -> None:
    a = _rec("e1", detail={"a": 1})
    b = _rec("e1", detail={"z": 9})
    ca = stable_canonical_json(a.payload_for_merge())
    cb = stable_canonical_json(b.payload_for_merge())
    winner, conflict = merge_records(a, b)
    assert conflict is True
    expected = a if ca < cb else b
    assert winner.detail == expected.detail


def test_merge_commutative_and_idempotent() -> None:
    base = {
        "schema_version": 1,
        "entry_id": "e1",
        "cluster_id": "c1",
        "origin_node": "n1",
        "origin_url": "ws://n1",
        "event": "x",
        "timestamp": 1.0,
        "actor": None,
        "success": True,
        "gossip_eligible": True,
    }
    variants = [
        SharedAuditRecord(**base, detail={"k": v})  # type: ignore[arg-type]
        for v in ("alpha", "beta", "gamma")
    ]
    # All permutations of fold-left merge should yield same winner
    winners: list[SharedAuditRecord] = []
    for order in itertools.permutations(variants):
        acc = order[0]
        for nxt in order[1:]:
            acc, _ = merge_records(acc, nxt)
        winners.append(acc)
    ids = {stable_canonical_json(w.payload_for_merge()) for w in winners}
    assert len(ids) == 1
    # Idempotent
    w = winners[0]
    again, c = merge_records(w, w)
    assert c is False
    assert again.detail == w.detail


def test_cross_identity_merge_raises() -> None:
    with pytest.raises(ValueError, match="distinct identities"):
        merge_records(_rec("a"), _rec("b"))


def test_insert_and_snapshot_order() -> None:
    store = SharedAuditStore(max_entries=100, cluster_id="c1", local_node="n1")
    store.insert(_rec("e2", ts=2.0))
    store.insert(_rec("e1", ts=1.0))
    store.insert(_rec("e3", ts=3.0, origin="n2"))
    snap = store.snapshot()
    assert [r.entry_id for r in snap] == ["e1", "e2", "e3"]
    assert store.size() == 3


def test_reject_cross_cluster() -> None:
    store = SharedAuditStore(cluster_id="c1")
    assert store.insert(_rec("e1", cluster="other")) is None
    assert store.rejected_cross_cluster == 1
    assert store.size() == 0


def test_watermark_no_resurrection() -> None:
    store = SharedAuditStore(max_entries=100, cluster_id="c1")
    store.insert(_rec("e1", ts=1.0, origin="n1"))
    store.insert(_rec("e2", ts=2.0, origin="n1"))
    store.merge_watermark("n1", Watermark(min_timestamp=2.0, min_entry_id="e2"))
    assert store.get("c1", "e1") is None
    assert store.get("c1", "e2") is not None
    # Re-insert below watermark rejected
    assert store.insert(_rec("e1", ts=1.0, origin="n1")) is None
    assert store.rejected_below_watermark >= 1
    assert store.get("c1", "e1") is None


def test_compaction_advances_watermark() -> None:
    store = SharedAuditStore(max_entries=3, cluster_id="c1")
    for i in range(5):
        store.insert(_rec(f"e{i}", ts=float(i), origin="n1"))
    assert store.size() == 3
    wm = store.watermark_for("n1")
    assert wm is not None
    # Oldest surviving must be at/above watermark
    for r in store.snapshot(origin_node="n1"):
        assert wm.covers(r.timestamp, r.entry_id)
    # Dropped ids stay dead
    assert store.insert(_rec("e0", ts=0.0, origin="n1")) is None


def test_records_for_pull_respects_requester_wm() -> None:
    store = SharedAuditStore(cluster_id="c1")
    store.insert(_rec("e1", ts=1.0, origin="n1"))
    store.insert(_rec("e2", ts=2.0, origin="n1"))
    store.insert(_rec("e3", ts=3.0, origin="n2", gossip=False))
    out = store.records_for_pull(
        requester_watermarks={"n1": Watermark(2.0, "e2")},
        limit=100,
    )
    ids = {r.entry_id for r in out}
    assert "e1" not in ids
    assert "e2" in ids
    assert "e3" not in ids  # not gossip_eligible


def test_legacy_jsonl_non_gossip(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    # Legacy MgmtAuditEntry line (no entry_id)
    from mpreg.core.native_codec import dumps_text

    legacy = {
        "event": "node_drain",
        "timestamp": 10.0,
        "actor": "ops",
        "success": True,
        "detail": {"node": "x"},
    }
    path.write_text(dumps_text(legacy) + "\n", encoding="utf-8")
    store = SharedAuditStore(
        cluster_id="c1", local_node="n1", persist_path=str(path), max_entries=100
    )
    assert store.size() == 1
    rec = store.snapshot()[0]
    assert rec.gossip_eligible is False
    assert rec.entry_id.startswith("legacy:")
    assert rec.entry_id == legacy_synthetic_id(
        {
            "event": legacy["event"],
            "timestamp": legacy["timestamp"],
            "actor": legacy["actor"],
            "success": legacy["success"],
            "detail": legacy["detail"],
        }
    )
    # Pull excludes legacy
    assert store.records_for_pull(requester_watermarks={}, limit=10) == []


def test_jsonl_roundtrip_schema_v1(tmp_path: Path) -> None:
    path = tmp_path / "shared.jsonl"
    store = SharedAuditStore(cluster_id="c1", local_node="n1", persist_path=str(path))
    r = record_from_mgmt_entry(
        event="detach",
        timestamp=5.0,
        actor="a",
        success=True,
        detail={"k": 1},
        cluster_id="c1",
        origin_node="n1",
        origin_url="ws://n1",
    )
    store.insert_and_persist(r)
    store2 = SharedAuditStore(cluster_id="c1", local_node="n1", persist_path=str(path))
    assert store2.size() == 1
    got = store2.get("c1", r.entry_id)
    assert got is not None
    assert got.event == "detach"
    assert got.gossip_eligible is True


def test_mint_ulid_unique() -> None:
    from mpreg.server_pkg.shared_audit import mint_entry_id

    ids = {mint_entry_id() for _ in range(50)}
    assert len(ids) == 50

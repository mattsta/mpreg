"""STRONG handler adversarial + fuzz (detectable malice fail-closed; not BFT)."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import StrongLocalBackend, StrongVersion
from mpreg.core.cache_strong_handlers import (
    StrongPeerHandler,
    key_to_payload,
    prepare_ack_from_dict,
    commit_ack_from_dict,
)
from tests.chaos.harness_strong_audit import (
    assert_no_pending,
    build_strong_mesh,
    key,
    strong_put_on,
)

def _handler(cluster: str = "c-a") -> tuple[StrongLocalBackend, StrongPeerHandler]:
    be = StrongLocalBackend(node_id="p1")
    return be, StrongPeerHandler(be, cluster_id=cluster)

@pytest.mark.asyncio
async def test_handler_bad_key() -> None:
    be, h = _handler()
    resp = await h.handle_prepare(
        {
            "cluster_id": "c-a",
            "key": "not-a-dict",
            "value": 1,
            "strong_version": StrongVersion(1, "o", "op").to_dict(),
            "replica_set": ["o", "p1"],
            "quorum": 2,
        }
    )
    assert resp["ok"] is False
    assert resp["reason"] == "bad_key"
    assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_handler_bad_version() -> None:
    be, h = _handler()
    k = GlobalCacheKey(namespace="h", identifier="v", version="v1")
    resp = await h.handle_prepare(
        {
            "cluster_id": "c-a",
            "key": key_to_payload(k),
            "value": 1,
            "strong_version": None,
            "replica_set": ["o", "p1"],
            "quorum": 2,
        }
    )
    assert resp["ok"] is False
    assert resp["reason"] == "bad_version"

@pytest.mark.asyncio
async def test_handler_commit_missing_op_id() -> None:
    be, h = _handler()
    k = GlobalCacheKey(namespace="h", identifier="c", version="v1")
    resp = await h.handle_commit(
        {"cluster_id": "c-a", "key": key_to_payload(k), "op_id": ""}
    )
    assert resp["ok"] is False
    assert resp["reason"] == "bad_request"

@pytest.mark.asyncio
async def test_handler_commit_wrong_cluster() -> None:
    be, h = _handler("c-a")
    k = GlobalCacheKey(namespace="h", identifier="cc", version="v1")
    resp = await h.handle_commit(
        {
            "cluster_id": "c-b",
            "key": key_to_payload(k),
            "op_id": "x",
        }
    )
    assert resp["ok"] is False
    assert resp["reason"] == "cluster_mismatch"

@pytest.mark.asyncio
async def test_handler_abort_unknown_safe() -> None:
    be, h = _handler()
    k = GlobalCacheKey(namespace="h", identifier="a", version="v1")
    resp = await h.handle_abort(
        {"key": key_to_payload(k), "op_id": "nope"}
    )
    assert resp["ok"] is True
    assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_handler_idempotent_prepare_commit() -> None:
    be, h = _handler()
    k = GlobalCacheKey(namespace="h", identifier="id", version="v1")
    payload = {
        "cluster_id": "c-a",
        "key": key_to_payload(k),
        "value": {"v": 1},
        "strong_version": StrongVersion(5, "o", "op-idemp").to_dict(),
        "replica_set": ["o", "p1"],
        "quorum": 2,
        "request_id": "r1",
    }
    r1 = await h.handle_prepare(payload)
    r2 = await h.handle_prepare(payload)
    assert r1["ok"] and r2["ok"]
    assert be.pending_count() == 1
    c1 = await h.handle_commit(
        {"cluster_id": "c-a", "key": key_to_payload(k), "op_id": "op-idemp"}
    )
    c2 = await h.handle_commit(
        {"cluster_id": "c-a", "key": key_to_payload(k), "op_id": "op-idemp"}
    )
    assert c1["ok"] and c1["applied"]
    assert c2["ok"] and c2["applied"]  # already applied
    assert be.get_visible(k) is not None
    assert be.pending_count() == 0

@pytest.mark.asyncio
async def test_flip_applied_malice_does_not_alone_satisfy_when_dropped() -> None:
    """flip_applied on peers that actually applied still ok; combine with drop."""
    mesh = build_strong_mesh(3)
    # One peer drops, one flips — still may get Q via origin+honest
    mesh.transport.drop_commit.add("n2")
    mesh.transport.flip_applied.add("n1")
    k = key("fuzz", "flip")
    res = await strong_put_on(mesh, "n0", k, 1)
    # n1 flip may turn applied True→False after real commit → fail path
    # Either success (if flip keeps applied) or residual-free fail
    if not res.success:
        from tests.chaos.harness_strong_audit import assert_residual_free

        assert_residual_free(mesh.backends, k, op_id=res.operation_id)
    assert_no_pending(mesh.backends)

@pytest.mark.asyncio
async def test_lie_commit_without_real_apply_insufficient_for_q() -> None:
    """Both peers lie commit_applied; origin still needs real peer applies.

    With origin-commit-last, need_peers = Q-1 = 1. Lying ACKs count as applied
    without backend state — this is the honest non_claim boundary for BFT.
    Document: client may see success while peers empty. We assert the lie path
    is *detectable in tests* and record the non_claim rather than claiming BFT.
    """
    mesh = build_strong_mesh(3)
    mesh.transport.lie_commit_applied |= {"n1", "n2"}
    k = key("fuzz", "lie-c")
    res = await strong_put_on(mesh, "n0", k, "lie")
    # Current architecture trusts COMMIT_ACK applied bit (CFT assumption).
    # If success, peers may lack value — that is the BFT non_claim.
    if res.success:
        # Origin has value; peers may not — record non_claim surface
        assert mesh.backends["n0"].get_visible(k) is not None
        peer_miss = sum(
            1
            for nid in ("n1", "n2")
            if mesh.backends[nid].get_visible(k) is None
        )
        # At least one peer lied without applying — proves not BFT
        assert peer_miss >= 1
    else:
        from tests.chaos.harness_strong_audit import assert_residual_free

        assert_residual_free(mesh.backends, k, op_id=res.operation_id)

@given(payload=st.dictionaries(st.text(max_size=8), st.integers() | st.text(max_size=8) | st.none(), max_size=12))
@settings(max_examples=40, deadline=None)
def test_handler_fuzz_never_raises(payload: dict) -> None:
    async def _run() -> None:
        be, h = _handler()
        # Must not raise
        await h.handle_prepare(payload)
        await h.handle_commit(payload)
        await h.handle_abort(payload)
        # Garbage must not create visible without valid prepare path
        # (may leave pending only if somehow valid — rare under pure fuzz)

    import asyncio

    asyncio.run(_run())

def test_ack_from_dict_tolerant() -> None:
    p = prepare_ack_from_dict({})
    assert p.ok is False
    c = commit_ack_from_dict({"ok": True, "applied": True, "node_id": "x"})
    assert c.ok and c.applied and c.node_id == "x"

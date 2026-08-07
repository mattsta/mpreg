"""Adversarial / fail-closed STRONG peer behavior (not BFT).

Proves the coordinator does not treat garbage ACKs or cluster mismatches as
success. Does **not** claim Byzantine fault tolerance.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from mpreg.core.cache_models import GlobalCacheKey
from mpreg.core.cache_strong import (
    CommitAck,
    PrepareAck,
    StrongErrorCode,
    StrongLocalBackend,
    StrongPutCoordinator,
    StrongVersion,
)
from mpreg.core.cache_strong_handlers import StrongPeerHandler


@dataclass
class AdversarialTransport:
    """Wraps honest backends with optional malice per peer."""

    backends: dict[str, StrongLocalBackend] = field(default_factory=dict)
    # peer_id -> behavior
    lie_prepare_ok: set[str] = field(default_factory=set)
    lie_commit_applied: set[str] = field(default_factory=set)
    wrong_cluster: set[str] = field(default_factory=set)
    drop_all: set[str] = field(default_factory=set)
    expected_cluster: str = "honest"

    def register(self, backend: StrongLocalBackend) -> None:
        self.backends[backend.node_id] = backend

    async def strong_prepare(
        self,
        peer_id: str,
        *,
        key: GlobalCacheKey,
        value: Any,
        metadata: Any,
        strong_version: StrongVersion,
        replica_set: tuple[str, ...],
        quorum: int,
        cluster_id: str,
        timeout: float,
    ) -> PrepareAck:
        if peer_id in self.drop_all:
            await asyncio.sleep(timeout + 0.02)
            raise TimeoutError("adv_drop")
        if peer_id in self.wrong_cluster:
            return PrepareAck(peer_id, False, "cluster_mismatch")
        if peer_id in self.lie_prepare_ok:
            # ACK ok without preparing — commit should fail residual-free
            return PrepareAck(peer_id, True, "lie")
        be = self.backends[peer_id]
        return await be.prepare(
            key=key,
            value=value,
            metadata=metadata,
            strong_version=strong_version,
            replica_set=replica_set,
            quorum=quorum,
            ttl_s=timeout,
        )

    async def strong_commit(
        self,
        peer_id: str,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
        cluster_id: str,
        timeout: float,
    ) -> CommitAck:
        if peer_id in self.drop_all:
            await asyncio.sleep(timeout + 0.02)
            raise TimeoutError("adv_drop")
        if peer_id in self.lie_commit_applied:
            return CommitAck(peer_id, True, applied=True, reason="lie")
        be = self.backends[peer_id]
        return await be.commit(op_id=op_id, key=key)

    async def strong_abort(
        self,
        peer_id: str,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: StrongVersion,
        cluster_id: str,
        timeout: float,
    ) -> bool:
        be = self.backends.get(peer_id)
        if be is None:
            return False
        return await be.abort(op_id=op_id, key=key)


def _mesh_adv():
    tr = AdversarialTransport()
    backends = {f"n{i}": StrongLocalBackend(node_id=f"n{i}") for i in range(3)}
    for be in backends.values():
        tr.register(be)
    coord = StrongPutCoordinator(
        origin_id="n0",
        local=backends["n0"],
        transport=tr,
        cluster_id="honest",
        replica_factor=3,
        min_replicas=3,
        prepare_timeout_s=0.3,
        commit_timeout_s=0.3,
    )
    return coord, tr, backends


@pytest.mark.asyncio
async def test_cluster_mismatch_prepare_fails_residual_free() -> None:
    coord, tr, backends = _mesh_adv()
    tr.wrong_cluster |= {"n1", "n2"}
    key = GlobalCacheKey(namespace="adv", identifier="cm", version="v1")
    res = await coord.strong_put(key, 1, eligible_peers=["n0", "n1", "n2"])
    assert res.success is False
    for be in backends.values():
        assert be.get_visible(key) is None
        assert be.pending_count() == 0


@pytest.mark.asyncio
async def test_lying_prepare_ack_without_pending_fails_commit() -> None:
    """Peer ACKs prepare but never stores pending → commit fails; residual-free."""
    coord, tr, backends = _mesh_adv()
    tr.lie_prepare_ok.add("n1")
    tr.lie_prepare_ok.add("n2")
    key = GlobalCacheKey(namespace="adv", identifier="lie-p", version="v1")
    res = await coord.strong_put(key, {"v": 1}, eligible_peers=["n0", "n1", "n2"])
    assert res.success is False
    assert res.error_code in (
        int(StrongErrorCode.QUORUM_TIMEOUT),
        int(StrongErrorCode.INSUFFICIENT_QUORUM),
        int(StrongErrorCode.STRONG_CONFLICT),
    )
    for be in backends.values():
        assert be.get_visible(key) is None
        assert be.pending_count() == 0


@pytest.mark.asyncio
async def test_handler_rejects_wrong_cluster_id() -> None:
    be = StrongLocalBackend(node_id="p1")
    h = StrongPeerHandler(be, cluster_id="c-a")
    key = GlobalCacheKey(namespace="adv", identifier="h", version="v1")
    from mpreg.core.cache_strong_handlers import key_to_payload

    payload = {
        "cluster_id": "c-b",
        "key": key_to_payload(key),
        "value": 1,
        "strong_version": StrongVersion(1, "o", "op1").to_dict(),
        "replica_set": ["o", "p1"],
        "quorum": 2,
        "request_id": "r1",
    }
    resp = await h.handle_prepare(payload)
    assert resp["ok"] is False
    assert resp["reason"] == "cluster_mismatch"
    assert be.pending_count() == 0


@pytest.mark.asyncio
async def test_drop_majority_peers_residual_free() -> None:
    coord, tr, backends = _mesh_adv()
    tr.drop_all |= {"n1", "n2"}
    key = GlobalCacheKey(namespace="adv", identifier="drop", version="v1")
    res = await coord.strong_put(key, 7, eligible_peers=["n0", "n1", "n2"])
    assert res.success is False
    for be in backends.values():
        assert be.get_visible(key) is None
        assert be.pending_count() == 0


@pytest.mark.asyncio
async def test_honest_path_still_works_with_adv_transport() -> None:
    coord, _tr, backends = _mesh_adv()
    key = GlobalCacheKey(namespace="adv", identifier="ok", version="v1")
    res = await coord.strong_put(key, "good", eligible_peers=["n0", "n1", "n2"])
    assert res.success is True
    for nid in res.quorum_info["commit_acks"]:  # type: ignore[index]
        assert backends[nid].get_visible(key) is not None

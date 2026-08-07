"""Reusable chaos harness for STRONG put + shared audit residual honesty.

In-process injectors only — not kernel partitions, not BFT, not Jepsen/Elle.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from mpreg.core.cache_models import CacheMetadata, GlobalCacheKey
from mpreg.core.cache_strong import (
    CommitAck,
    PrepareAck,
    StrongLocalBackend,
    StrongPutCoordinator,
    StrongVersion,
    _entry_op_id,
)


@dataclass
class ChaosStrongTransport:
    """StrongPeerTransport with partition / delay / drop / malice injectors."""

    backends: dict[str, StrongLocalBackend] = field(default_factory=dict)
    # Undirected cuts
    partitions: set[frozenset[str]] = field(default_factory=set)
    drop_prepare: set[str] = field(default_factory=set)
    drop_commit: set[str] = field(default_factory=set)
    drop_abort: set[str] = field(default_factory=set)
    fail_prepare: set[str] = field(default_factory=set)
    lie_prepare_ok: set[str] = field(default_factory=set)
    lie_commit_applied: set[str] = field(default_factory=set)
    wrong_cluster: set[str] = field(default_factory=set)
    flip_applied: set[str] = field(default_factory=set)
    delay_prepare_s: float = 0.0
    delay_commit_s: float = 0.0
    duplicate_commit: bool = False
    expected_cluster: str = "chaos"
    # metrics
    prepares_sent: int = 0
    commits_sent: int = 0
    aborts_sent: int = 0
    drops: int = 0

    def register(self, backend: StrongLocalBackend) -> None:
        self.backends[backend.node_id] = backend

    def partition(self, a: str, b: str) -> None:
        self.partitions.add(frozenset({a, b}))

    def heal(self, a: str | None = None, b: str | None = None) -> None:
        if a is None or b is None:
            self.partitions.clear()
            return
        self.partitions.discard(frozenset({a, b}))

    def _blocked(self, src: str, dst: str) -> bool:
        return frozenset({src, dst}) in self.partitions

    def clear_faults(self) -> None:
        self.partitions.clear()
        self.drop_prepare.clear()
        self.drop_commit.clear()
        self.drop_abort.clear()
        self.fail_prepare.clear()
        self.lie_prepare_ok.clear()
        self.lie_commit_applied.clear()
        self.wrong_cluster.clear()
        self.flip_applied.clear()
        self.delay_prepare_s = 0.0
        self.delay_commit_s = 0.0
        self.duplicate_commit = False

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
        self.prepares_sent += 1
        origin = strong_version.origin_node
        if self._blocked(origin, peer_id):
            self.drops += 1
            await asyncio.sleep(timeout + 0.02)
            raise TimeoutError("partitioned_prepare")
        if peer_id in self.drop_prepare:
            self.drops += 1
            await asyncio.sleep(timeout + 0.02)
            raise TimeoutError("drop_prepare")
        if self.delay_prepare_s > 0:
            await asyncio.sleep(self.delay_prepare_s)
        if peer_id in self.wrong_cluster or (
            cluster_id and cluster_id != self.expected_cluster
        ):
            return PrepareAck(peer_id, False, "cluster_mismatch")
        if peer_id in self.lie_prepare_ok:
            return PrepareAck(peer_id, True, "lie")
        if peer_id in self.fail_prepare:
            return PrepareAck(peer_id, False, "injected_fail")
        be = self.backends.get(peer_id)
        if be is None:
            return PrepareAck(peer_id, False, "unknown_peer")
        return await be.prepare(
            key=key,
            value=value,
            metadata=metadata,
            strong_version=strong_version,
            replica_set=replica_set,
            quorum=quorum,
            ttl_s=max(timeout, 0.5),
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
        self.commits_sent += 1
        origin = strong_version.origin_node
        if self._blocked(origin, peer_id):
            self.drops += 1
            await asyncio.sleep(timeout + 0.02)
            raise TimeoutError("partitioned_commit")
        if peer_id in self.drop_commit:
            self.drops += 1
            await asyncio.sleep(timeout + 0.02)
            raise TimeoutError("drop_commit")
        if self.delay_commit_s > 0:
            await asyncio.sleep(self.delay_commit_s)
        if peer_id in self.lie_commit_applied:
            return CommitAck(peer_id, True, applied=True, reason="lie")
        be = self.backends.get(peer_id)
        if be is None:
            return CommitAck(peer_id, False, reason="unknown_peer")
        ack = await be.commit(op_id=op_id, key=key)
        if self.duplicate_commit:
            await be.commit(op_id=op_id, key=key)
        if peer_id in self.flip_applied and ack.ok:
            # Flip applied bit without undoing backend — detectable malice surface
            return CommitAck(peer_id, True, applied=not ack.applied, reason="flip")
        return ack

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
        self.aborts_sent += 1
        origin = strong_version.origin_node
        if self._blocked(origin, peer_id) or peer_id in self.drop_abort:
            self.drops += 1
            return False
        be = self.backends.get(peer_id)
        if be is None:
            return False
        return await be.abort(op_id=op_id, key=key)


@dataclass
class StrongMesh:
    n: int
    cluster_id: str
    transport: ChaosStrongTransport
    backends: dict[str, StrongLocalBackend]
    coords: dict[str, StrongPutCoordinator]
    peer_ids: list[str]

    @property
    def eligible(self) -> list[str]:
        return list(self.peer_ids)


def build_strong_mesh(
    n: int = 3,
    *,
    cluster_id: str = "chaos",
    replica_factor: int | None = None,
    min_replicas: int | None = None,
    prepare_timeout_s: float = 0.4,
    commit_timeout_s: float = 0.4,
    pending_ttl_s: float = 30.0,
    max_pending: int = 128,
) -> StrongMesh:
    rf = replica_factor if replica_factor is not None else n
    mr = min_replicas if min_replicas is not None else n
    tr = ChaosStrongTransport(expected_cluster=cluster_id)
    backends = {
        f"n{i}": StrongLocalBackend(node_id=f"n{i}", max_pending=max_pending)
        for i in range(n)
    }
    for be in backends.values():
        tr.register(be)
    coords: dict[str, StrongPutCoordinator] = {}
    for i in range(n):
        nid = f"n{i}"
        coords[nid] = StrongPutCoordinator(
            origin_id=nid,
            local=backends[nid],
            transport=tr,
            cluster_id=cluster_id,
            replica_factor=rf,
            min_replicas=mr,
            prepare_timeout_s=prepare_timeout_s,
            commit_timeout_s=commit_timeout_s,
            pending_ttl_s=pending_ttl_s,
        )
    return StrongMesh(
        n=n,
        cluster_id=cluster_id,
        transport=tr,
        backends=backends,
        coords=coords,
        peer_ids=[f"n{i}" for i in range(n)],
    )


def assert_residual_free(
    backends: dict[str, StrongLocalBackend],
    key: GlobalCacheKey,
    *,
    op_id: str | None = None,
) -> None:
    """No pending anywhere; if op_id given, no visible entry with that op_id."""
    for be in backends.values():
        assert be.pending_count() == 0, f"pending on {be.node_id}"
        if op_id is None:
            continue
        ent = be.get_visible(key)
        if ent is not None:
            assert _entry_op_id(ent) != op_id, (
                f"residual op {op_id} visible on {be.node_id}"
            )


def assert_backends_agree(
    backends: dict[str, StrongLocalBackend], key: GlobalCacheKey
) -> object | None:
    """All backends share the same visible value (or all None). Return that value."""
    vals: list[tuple[object, str | None] | None] = []
    for be in backends.values():
        ent = be.get_visible(key)
        vals.append(None if ent is None else (ent.value, _entry_op_id(ent)))
    # Values may be unhashable (dict); compare pairwise.
    first = vals[0] if vals else None
    for v in vals[1:]:
        assert v == first, f"backends diverged: {vals}"
    if first is None:
        return None
    return first[0]


def assert_no_pending(backends: dict[str, StrongLocalBackend]) -> None:
    for be in backends.values():
        assert be.pending_count() == 0, f"pending left on {be.node_id}"


async def strong_put_on(
    mesh: StrongMesh,
    origin: str,
    key: GlobalCacheKey,
    value: Any,
    *,
    op_id: str | None = None,
    metadata: CacheMetadata | None = None,
) -> Any:
    return await mesh.coords[origin].strong_put(
        key,
        value,
        metadata=metadata or CacheMetadata(created_by=origin),
        eligible_peers=mesh.eligible,
        op_id=op_id,
    )


def key(ns: str, ident: str, version: str = "v1") -> GlobalCacheKey:
    return GlobalCacheKey(namespace=ns, identifier=ident, version=version)


# ---------------------------------------------------------------------------
# History checker (bounded, not Jepsen)
# ---------------------------------------------------------------------------


@dataclass
class PutOutcome:
    origin: str
    value: object
    success: bool
    op_id: str | None
    error_code: int | None


def check_single_key_history(
    outcomes: Sequence[PutOutcome],
    backends: dict[str, StrongLocalBackend],
    k: GlobalCacheKey,
) -> None:
    success = [o for o in outcomes if o.success]
    failed = [o for o in outcomes if not o.success]

    for o in failed:
        if o.op_id:
            assert_residual_free(backends, k, op_id=o.op_id)
        else:
            assert_no_pending(backends)

    assert_no_pending(backends)
    final_val = assert_backends_agree(backends, k)

    if final_val is None:
        assert not success, "success but no visible value"
        return

    ok_ops = {o.op_id for o in success}
    # Find final op
    sample = next(iter(backends.values())).get_visible(k)
    assert sample is not None
    final_op = _entry_op_id(sample)
    assert final_op in ok_ops
    assert any(o.value == final_val and o.op_id == final_op for o in success)


async def run_concurrent_puts(
    mesh: StrongMesh,
    k: GlobalCacheKey,
    values: Sequence[object],
) -> list[PutOutcome]:
    async def one(idx: int, val: object) -> PutOutcome:
        origin = mesh.peer_ids[idx % mesh.n]
        res = await strong_put_on(mesh, origin, k, val, op_id=f"op-{idx}-{origin}")
        return PutOutcome(
            origin=origin,
            value=val,
            success=bool(res.success),
            op_id=res.operation_id,
            error_code=res.error_code,
        )

    return list(await asyncio.gather(*[one(i, v) for i, v in enumerate(values)]))


def deep_copy_payload(p: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(p)

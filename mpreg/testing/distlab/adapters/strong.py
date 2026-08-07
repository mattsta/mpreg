"""STRONG put system-under-test adapter for DistLab."""

from __future__ import annotations

import asyncio
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
from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import OpKind
from mpreg.testing.faults import FaultInjector


@dataclass
class StrongChaosTransport:
    """StrongPeerTransport driven by FaultInjector + malice sets."""

    backends: dict[str, StrongLocalBackend] = field(default_factory=dict)
    injector: FaultInjector = field(default_factory=FaultInjector)
    # Undirected edge cuts (in addition to FaultInjector partition groups)
    edge_cuts: set[frozenset[str]] = field(default_factory=set)
    drop_prepare: set[str] = field(default_factory=set)
    drop_commit: set[str] = field(default_factory=set)
    drop_abort: set[str] = field(default_factory=set)
    fail_prepare: set[str] = field(default_factory=set)
    lie_prepare_ok: set[str] = field(default_factory=set)
    lie_commit_applied: set[str] = field(default_factory=set)
    wrong_cluster: set[str] = field(default_factory=set)
    expected_cluster: str = "distlab"
    crashed: set[str] = field(default_factory=set)
    delay_override_s: float = 0.0
    duplicate_commit: bool = False
    prepares_sent: int = 0
    commits_sent: int = 0
    aborts_sent: int = 0
    drops: int = 0

    def register(self, backend: StrongLocalBackend) -> None:
        self.backends[backend.node_id] = backend

    def cut_edge(self, a: str, b: str) -> None:
        self.edge_cuts.add(frozenset({a, b}))

    def heal_edges(self) -> None:
        self.edge_cuts.clear()

    def clear_malice(self) -> None:
        self.drop_prepare.clear()
        self.drop_commit.clear()
        self.drop_abort.clear()
        self.fail_prepare.clear()
        self.lie_prepare_ok.clear()
        self.lie_commit_applied.clear()
        self.wrong_cluster.clear()
        self.crashed.clear()
        self.delay_override_s = 0.0
        self.duplicate_commit = False
        self.heal_edges()
        self.injector.heal()
        self.injector.control_drop_rate = 0.0
        self.injector.data_drop_rate = 0.0
        self.injector.control_delay_seconds = 0.0
        self.injector.data_delay_seconds = 0.0

    def _blocked(self, src: str, dst: str) -> bool:
        if dst in self.crashed or src in self.crashed:
            return True
        if frozenset({src, dst}) in self.edge_cuts:
            return True
        return bool(not self.injector.can_deliver(src, dst, plane="data"))

    async def _maybe_delay(self) -> None:
        d = self.delay_override_s or self.injector.delay_for(plane="data")
        if d > 0:
            await asyncio.sleep(d)

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
        if self._blocked(origin, peer_id) or peer_id in self.drop_prepare:
            self.drops += 1
            await asyncio.sleep(min(timeout + 0.02, timeout + 0.05))
            raise TimeoutError("strong_prepare_dropped")
        await self._maybe_delay()
        if peer_id in self.wrong_cluster:
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
        if self._blocked(origin, peer_id) or peer_id in self.drop_commit:
            self.drops += 1
            await asyncio.sleep(min(timeout + 0.02, timeout + 0.05))
            raise TimeoutError("strong_commit_dropped")
        await self._maybe_delay()
        if peer_id in self.lie_commit_applied:
            return CommitAck(peer_id, True, applied=True, reason="lie")
        be = self.backends.get(peer_id)
        if be is None:
            return CommitAck(peer_id, False, reason="unknown_peer")
        ack = await be.commit(op_id=op_id, key=key)
        if self.duplicate_commit or self.injector.should_duplicate():
            await be.commit(op_id=op_id, key=key)
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
class StrongStateSnapshot:
    """Checker-facing snapshot of a STRONG mesh."""

    backends: dict[str, StrongLocalBackend]
    key_of: Any  # callable str -> GlobalCacheKey | None

    def pending_count(self) -> int:
        return sum(be.pending_count() for be in self.backends.values())

    def visible_op_ids(self, key: str) -> set[str]:
        gk = self.key_of(key)
        if gk is None:
            return set()
        out: set[str] = set()
        for be in self.backends.values():
            ent = be.get_visible(gk)
            if ent is not None:
                oid = _entry_op_id(ent)
                if oid:
                    out.add(oid)
        return out

    def replica_views(self, key: str) -> dict[str, tuple[Any, str | None] | None]:
        gk = self.key_of(key)
        views: dict[str, tuple[Any, str | None] | None] = {}
        if gk is None:
            return views
        for nid, be in self.backends.items():
            ent = be.get_visible(gk)
            if ent is None:
                views[nid] = None
            else:
                views[nid] = (ent.value, _entry_op_id(ent))
        return views

    def final_op_id(self, key: str) -> str | None:
        """First non-null op_id among replicas (callers should also run agreement)."""
        views = self.replica_views(key)
        for v in views.values():
            if v is not None:
                return v[1]
        return None

    def final_value(self, key: str) -> Any:
        views = self.replica_views(key)
        for v in views.values():
            if v is not None:
                return v[0]
        return None


@dataclass
class StrongSUT:
    """In-process N-node STRONG majority-commit system under test."""

    n: int
    cluster_id: str
    transport: StrongChaosTransport
    backends: dict[str, StrongLocalBackend]
    coords: dict[str, StrongPutCoordinator]
    peer_ids: list[str]
    _keys: dict[str, GlobalCacheKey] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        n: int = 3,
        *,
        cluster_id: str = "distlab",
        replica_factor: int | None = None,
        min_replicas: int | None = None,
        prepare_timeout_s: float = 0.4,
        commit_timeout_s: float = 0.4,
        pending_ttl_s: float = 30.0,
        max_pending: int = 128,
        seed: int = 0,
    ) -> StrongSUT:
        rf = replica_factor if replica_factor is not None else n
        mr = min_replicas if min_replicas is not None else n
        inj = FaultInjector(seed=seed)
        tr = StrongChaosTransport(injector=inj, expected_cluster=cluster_id)
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
        return cls(
            n=n,
            cluster_id=cluster_id,
            transport=tr,
            backends=backends,
            coords=coords,
            peer_ids=[f"n{i}" for i in range(n)],
        )

    def register_key(self, logical: str, key: GlobalCacheKey) -> GlobalCacheKey:
        self._keys[logical] = key
        return key

    def key(self, logical: str) -> GlobalCacheKey:
        if logical not in self._keys:
            # auto GlobalCacheKey
            self._keys[logical] = GlobalCacheKey(
                namespace="distlab", identifier=logical, version="v1"
            )
        return self._keys[logical]

    def snapshot_state(self) -> StrongStateSnapshot:
        return StrongStateSnapshot(
            backends=self.backends, key_of=lambda k: self._keys.get(k) or self.key(k)
        )

    async def put(
        self,
        history: History,
        *,
        process: str,
        origin: str,
        logical_key: str,
        value: Any,
        op_id: str | None = None,
    ) -> Any:
        k = self.key(logical_key)
        history.invoke(process, OpKind.PUT, key=logical_key, value=value, op_id=op_id)
        try:
            res = await self.coords[origin].strong_put(
                k,
                value,
                metadata=CacheMetadata(created_by=origin),
                eligible_peers=list(self.peer_ids),
                op_id=op_id,
            )
        except Exception as exc:
            history.info(
                process,
                OpKind.PUT,
                key=logical_key,
                value=value,
                op_id=op_id,
                error_message=type(exc).__name__,
            )
            raise
        if res.success:
            history.ok(
                process,
                OpKind.PUT,
                key=logical_key,
                value=value,
                op_id=res.operation_id,
                meta={"quorum": (res.quorum_info or {})},
            )
        else:
            history.fail(
                process,
                OpKind.PUT,
                key=logical_key,
                value=value,
                op_id=res.operation_id,
                error_code=res.error_code,
                error_message=res.error_message,
            )
        return res

    def nemesis_hooks(self) -> dict[str, Any]:
        """Callbacks for FaultInjectorNemesisTarget."""
        tr = self.transport
        nodes = list(self.peer_ids)

        def on_partition(groups: Sequence[set[str]]) -> None:
            tr.heal_edges()
            # Cut all cross-group edges
            labeled = [set(g) for g in groups]
            for i, g1 in enumerate(labeled):
                for g2 in labeled[i + 1 :]:
                    for a in g1:
                        for b in g2:
                            tr.cut_edge(a, b)

        def on_heal() -> None:
            tr.heal_edges()
            tr.injector.heal()

        def on_crash(node_id: str) -> None:
            tr.crashed.add(node_id)

        def on_recover(node_id: str) -> None:
            tr.crashed.discard(node_id)

        def on_drop(rate: float) -> None:
            pass  # injector already set

        def on_delay(seconds: float) -> None:
            tr.delay_override_s = seconds

        return {
            "nodes": nodes,
            "on_partition": on_partition,
            "on_heal": on_heal,
            "on_crash": on_crash,
            "on_recover": on_recover,
            "on_drop_rate": on_drop,
            "on_delay": on_delay,
            "injector": tr.injector,
        }

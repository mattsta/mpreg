"""Shared-audit G-Set system-under-test adapter for DistLab."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from mpreg.server_pkg.shared_audit import (
    InProcessSharedAuditTransport,
    SharedAuditReplicator,
    SharedAuditStore,
    record_from_mgmt_entry,
)
from mpreg.testing.distlab.history import History
from mpreg.testing.distlab.models import OpKind
from mpreg.testing.faults import FaultInjector

@dataclass
class AuditStateSnapshot:
    stores: dict[str, SharedAuditStore]

    def gset_ids_by_node(self) -> dict[str, set[str]]:
        out: dict[str, set[str]] = {}
        for nid, store in self.stores.items():
            ids: set[str] = set()
            for r in store.snapshot(gossip_eligible_only=True):
                ids.add(r.entry_id)
            out[nid] = ids
        return out

    def pending_count(self) -> int:
        return 0

@dataclass
class AuditSUT:
    """In-process shared-audit mesh under test."""

    cluster_id: str
    transport: InProcessSharedAuditTransport
    stores: dict[str, SharedAuditStore]
    reps: dict[str, SharedAuditReplicator]
    node_ids: list[str]
    injector: FaultInjector = field(default_factory=FaultInjector)
    _ts: float = 1.0

    @classmethod
    def create(
        cls,
        n: int = 3,
        *,
        cluster_id: str = "distlab-audit",
        max_entries: int = 500,
        max_outbound_queue: int = 256,
        seed: int = 0,
    ) -> AuditSUT:
        tr = InProcessSharedAuditTransport()
        stores: dict[str, SharedAuditStore] = {}
        reps: dict[str, SharedAuditReplicator] = {}
        nodes = [f"a{i}" for i in range(n)]
        for nid in nodes:
            store = SharedAuditStore(
                cluster_id=cluster_id, local_node=nid, max_entries=max_entries
            )

            def peers(n=nid):
                return [p for p in tr.peers if p != n]

            rep = SharedAuditReplicator(
                store=store,
                node_id=nid,
                cluster_id=cluster_id,
                transport=tr,
                peer_list=peers,
                reconcile_interval_s=60.0,
                max_outbound_queue=max_outbound_queue,
            )
            tr.register(rep)
            stores[nid] = store
            reps[nid] = rep
        return cls(
            cluster_id=cluster_id,
            transport=tr,
            stores=stores,
            reps=reps,
            node_ids=nodes,
            injector=FaultInjector(seed=seed),
        )

    def snapshot_state(self) -> AuditStateSnapshot:
        return AuditStateSnapshot(stores=dict(self.stores))

    async def publish(
        self,
        history: History,
        *,
        process: str,
        origin: str,
        event: str,
        eligible: bool = True,
    ) -> str:
        self._ts += 1.0
        rec = record_from_mgmt_entry(
            event=event,
            timestamp=self._ts,
            actor=process,
            success=True,
            detail={"event": event},
            cluster_id=self.cluster_id,
            origin_node=origin,
            gossip_eligible=eligible,
        )
        history.invoke(
            process,
            OpKind.AUDIT_PUBLISH,
            key=origin,
            value=event,
            op_id=rec.entry_id,
        )
        stored = self.stores[origin].insert(rec)
        if stored is None:
            history.fail(
                process,
                OpKind.AUDIT_PUBLISH,
                key=origin,
                value=event,
                op_id=rec.entry_id,
                error_message="insert_rejected",
            )
            return rec.entry_id
        if eligible:
            self.reps[origin].publish(rec)
            await self.reps[origin]._flush_outbound()
        history.ok(
            process,
            OpKind.AUDIT_PUBLISH,
            key=origin,
            value=event,
            op_id=rec.entry_id,
            meta={"eligible": eligible},
        )
        return rec.entry_id

    async def reconcile_all(self) -> None:
        """Digest exchange among all pairs (anti-entropy pass)."""
        for src in self.node_ids:
            digest = self.reps[src].build_digest()
            for dst in self.node_ids:
                if dst == src:
                    continue
                await self.reps[dst]._on_digest(digest)

    async def flush_all(self) -> None:
        for nid in self.node_ids:
            await self.reps[nid]._flush_outbound()

    def nemesis_hooks(self) -> dict[str, Any]:
        tr = self.transport
        nodes = list(self.node_ids)

        def on_partition(groups: Sequence[set[str]]) -> None:
            tr.heal()
            labeled = [set(g) for g in groups]
            for i, g1 in enumerate(labeled):
                for g2 in labeled[i + 1 :]:
                    for a in g1:
                        for b in g2:
                            tr.partition(a, b)

        def on_heal() -> None:
            tr.heal()

        return {
            "nodes": nodes,
            "on_partition": on_partition,
            "on_heal": on_heal,
            "on_crash": lambda _n: None,
            "on_recover": lambda _n: None,
            "on_drop_rate": lambda r: setattr(
                tr, "drop_types", set(tr.drop_types) | (
                    {"mgmt_audit_delta"} if r > 0.3 else set()
                )
            ),
            "on_delay": lambda s: setattr(tr, "delay_s", s),
            "injector": self.injector,
        }

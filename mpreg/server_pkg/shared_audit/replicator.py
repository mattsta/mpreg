"""Shared audit epidemic DELTA + digest/PULL anti-entropy replicator.

Transport is injected (fake for tests, fabric gossip for production). The
replicator never blocks local :meth:`SharedAuditStore.insert`.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from mpreg.server_pkg.shared_audit.models import SharedAuditRecord
from mpreg.server_pkg.shared_audit.store import SharedAuditStore, Watermark

MAX_DELTA_RECORDS = 64

@dataclass(frozen=True, slots=True)
class SharedAuditHealth:
    enabled: bool
    store_size: int
    peers_known: int
    publish_dropped: int
    merge_conflicts: int
    last_digest_at: float | None
    last_delta_at: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "store_size": self.store_size,
            "peers_known": self.peers_known,
            "publish_dropped": self.publish_dropped,
            "merge_conflicts": self.merge_conflicts,
            "last_digest_at": self.last_digest_at,
            "last_delta_at": self.last_delta_at,
        }

class SharedAuditTransport(Protocol):
    """Minimal fanout + unicast surface for the replicator."""

    async def send_epidemic(self, message_type: str, payload: dict[str, Any]) -> int:
        """Fanout to up to N peers; return peers contacted."""
        ...

    async def send_unicast(
        self, peer_id: str, message_type: str, payload: dict[str, Any]
    ) -> bool:
        """Directed send to one peer; return success."""
        ...

PeerListFn = Callable[[], Sequence[str]]

@dataclass(slots=True)
class SharedAuditReplicator:
    """Self-managing G-Set audit replicator."""

    store: SharedAuditStore
    node_id: str
    cluster_id: str
    transport: SharedAuditTransport
    peer_list: PeerListFn
    gossip_targets: int = 3
    reconcile_interval_s: float = 2.0
    max_outbound_queue: int = 256
    _outbound: deque[SharedAuditRecord] = field(default_factory=deque)
    _publish_dropped: int = 0
    _last_digest_at: float | None = None
    _last_delta_at: float | None = None
    _task: asyncio.Task[None] | None = None
    _stopped: bool = False
    _pending_pulls: dict[str, asyncio.Future[list[SharedAuditRecord]]] = field(
        default_factory=dict
    )

    def start(self) -> None:
        if self._task is not None:
            return
        self._stopped = False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._task = loop.create_task(self._run(), name="shared-audit-replicator")

    async def stop(self) -> None:
        self._stopped = True
        t = self._task
        self._task = None
        if t is not None:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t

    def publish(self, record: SharedAuditRecord) -> None:
        """Queue for epidemic DELTA; never raises into mutation path."""
        if not record.gossip_eligible:
            return
        if len(self._outbound) >= self.max_outbound_queue:
            with contextlib.suppress(IndexError):
                self._outbound.popleft()
            self._publish_dropped += 1
        self._outbound.append(record)

    def health(self) -> SharedAuditHealth:
        return SharedAuditHealth(
            enabled=True,
            store_size=self.store.size(),
            peers_known=len(list(self.peer_list())),
            publish_dropped=self._publish_dropped,
            merge_conflicts=self.store.merge_conflicts,
            last_digest_at=self._last_digest_at,
            last_delta_at=self._last_delta_at,
        )

    def on_message(self, message_type: str, payload: dict[str, Any]) -> None:
        """Synchronous dispatch entry (may schedule async work)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._handle_sync(message_type, payload)
            return
        loop.create_task(self._handle_async(message_type, payload))

    def _handle_sync(self, message_type: str, payload: dict[str, Any]) -> None:
        if message_type in ("mgmt_audit_delta", "MGMT_AUDIT_DELTA"):
            self._apply_delta(payload)
        elif message_type in ("mgmt_audit_digest", "MGMT_AUDIT_DIGEST"):
            # digest needs async pull — stash only when no loop
            pass
        elif message_type in ("mgmt_audit_pull_resp", "MGMT_AUDIT_PULL_RESP"):
            self._apply_pull_resp(payload)

    async def _handle_async(self, message_type: str, payload: dict[str, Any]) -> None:
        mt = message_type.lower().replace("mgmt_audit_", "")
        if message_type in ("mgmt_audit_delta", "MGMT_AUDIT_DELTA") or mt == "delta":
            self._apply_delta(payload)
        elif (
            message_type in ("mgmt_audit_digest", "MGMT_AUDIT_DIGEST") or mt == "digest"
        ):
            await self._on_digest(payload)
        elif message_type in ("mgmt_audit_pull", "MGMT_AUDIT_PULL") or mt == "pull":
            await self._on_pull(payload)
        elif (
            message_type in ("mgmt_audit_pull_resp", "MGMT_AUDIT_PULL_RESP")
            or mt == "pull_resp"
        ):
            self._apply_pull_resp(payload)

    def _apply_delta(self, payload: dict[str, Any]) -> None:
        if str(payload.get("cluster_id") or "") != self.cluster_id:
            return
        records = payload.get("records") or []
        if not isinstance(records, list):
            return
        for raw in records:
            if not isinstance(raw, dict):
                continue
            try:
                rec = SharedAuditRecord.from_dict(raw)
            except Exception:  # noqa: BLE001
                continue
            if not rec.gossip_eligible:
                continue
            self.store.insert(rec)
        # Merge peer watermarks
        wms = payload.get("watermarks") or {}
        if isinstance(wms, dict):
            for origin, wm_raw in wms.items():
                if isinstance(wm_raw, dict):
                    wm = Watermark.from_dict(
                        {
                            "min_timestamp": wm_raw.get("ts")
                            or wm_raw.get("min_timestamp")
                            or 0.0,
                            "min_entry_id": wm_raw.get("entry_id")
                            or wm_raw.get("min_entry_id")
                            or "",
                        }
                    )
                    if wm is not None:
                        self.store.merge_watermark(str(origin), wm)
        self._last_delta_at = time.time()

    def build_digest(self) -> dict[str, Any]:
        origins: dict[str, Any] = {}
        by_origin: dict[str, list[SharedAuditRecord]] = {}
        for r in self.store.snapshot(gossip_eligible_only=True):
            by_origin.setdefault(r.origin_node, []).append(r)
        for origin, items in by_origin.items():
            items_sorted = sorted(items, key=lambda x: x.sort_key())
            sample = [i.entry_id for i in items_sorted[-8:]]
            last = items_sorted[-1]
            origins[origin] = {
                "count": len(items_sorted),
                "max_ts": last.timestamp,
                "max_entry_id": last.entry_id,
                "id_sample": sample,
            }
        wms = {
            o: {"ts": w.min_timestamp, "entry_id": w.min_entry_id}
            for o, w in self.store.watermarks_snapshot().items()
        }
        return {
            "cluster_id": self.cluster_id,
            "from_node": self.node_id,
            "watermarks": wms,
            "origins": origins,
        }

    async def _on_digest(self, payload: dict[str, Any]) -> None:
        if str(payload.get("cluster_id") or "") != self.cluster_id:
            return
        peer = str(payload.get("from_node") or "")
        if not peer or peer == self.node_id:
            return
        self._last_digest_at = time.time()
        # Merge peer watermarks
        wms = payload.get("watermarks") or {}
        if isinstance(wms, dict):
            for origin, wm_raw in wms.items():
                if isinstance(wm_raw, dict):
                    wm = Watermark.from_dict(
                        {
                            "min_timestamp": wm_raw.get("ts")
                            or wm_raw.get("min_timestamp")
                            or 0.0,
                            "min_entry_id": wm_raw.get("entry_id")
                            or wm_raw.get("min_entry_id")
                            or "",
                        }
                    )
                    if wm is not None:
                        self.store.merge_watermark(str(origin), wm)

        peer_origins = payload.get("origins") or {}
        if not isinstance(peer_origins, dict):
            return
        # If peer has more for any origin, PULL
        need_pull = False
        local_by = {}
        for r in self.store.snapshot(gossip_eligible_only=True):
            local_by.setdefault(r.origin_node, 0)
            local_by[r.origin_node] += 1
        for origin, info in peer_origins.items():
            if not isinstance(info, dict):
                continue
            peer_count = int(info.get("count") or 0)
            if peer_count > local_by.get(str(origin), 0):
                need_pull = True
                break
            sample = info.get("id_sample") or []
            if isinstance(sample, list):
                for eid in sample:
                    if self.store.get(self.cluster_id, str(eid)) is None:
                        need_pull = True
                        break
            if need_pull:
                break
        if need_pull:
            await self._send_pull(peer)

    async def _send_pull(self, peer_id: str) -> None:
        request_id = str(uuid.uuid4())
        wms = {
            o: {"ts": w.min_timestamp, "entry_id": w.min_entry_id}
            for o, w in self.store.watermarks_snapshot().items()
        }
        payload = {
            "cluster_id": self.cluster_id,
            "request_id": request_id,
            "requester_node": self.node_id,
            "watermarks": wms,
            "limit": 200,
        }
        await self.transport.send_unicast(peer_id, "mgmt_audit_pull", payload)

    async def _on_pull(self, payload: dict[str, Any]) -> None:
        if str(payload.get("cluster_id") or "") != self.cluster_id:
            return
        requester = str(payload.get("requester_node") or "")
        request_id = str(payload.get("request_id") or "")
        if not requester or not request_id:
            return
        req_wms: dict[str, Watermark] = {}
        raw_wms = payload.get("watermarks") or {}
        if isinstance(raw_wms, dict):
            for o, w in raw_wms.items():
                if isinstance(w, dict):
                    wm = Watermark.from_dict(
                        {
                            "min_timestamp": w.get("ts") or w.get("min_timestamp") or 0,
                            "min_entry_id": w.get("entry_id")
                            or w.get("min_entry_id")
                            or "",
                        }
                    )
                    if wm is not None:
                        req_wms[str(o)] = wm
        limit = int(payload.get("limit") or 200)
        records = self.store.records_for_pull(requester_watermarks=req_wms, limit=limit)
        resp = {
            "cluster_id": self.cluster_id,
            "request_id": request_id,
            "from_node": self.node_id,
            "records": [r.to_dict() for r in records],
            "watermarks": {
                o: {"ts": w.min_timestamp, "entry_id": w.min_entry_id}
                for o, w in self.store.watermarks_snapshot().items()
            },
        }
        await self.transport.send_unicast(requester, "mgmt_audit_pull_resp", resp)

    def _apply_pull_resp(self, payload: dict[str, Any]) -> None:
        if str(payload.get("cluster_id") or "") != self.cluster_id:
            return
        self._apply_delta(payload)

    async def _run(self) -> None:
        while not self._stopped:
            try:
                await self._flush_outbound()
                await self._exchange_digests()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                pass
            try:
                await asyncio.sleep(self.reconcile_interval_s)
            except asyncio.CancelledError:
                raise

    async def _flush_outbound(self) -> None:
        if not self._outbound:
            return
        batch: list[SharedAuditRecord] = []
        while self._outbound and len(batch) < MAX_DELTA_RECORDS:
            batch.append(self._outbound.popleft())
        payload = {
            "cluster_id": self.cluster_id,
            "from_node": self.node_id,
            "records": [r.to_dict() for r in batch],
            "watermarks": {
                o: {"ts": w.min_timestamp, "entry_id": w.min_entry_id}
                for o, w in self.store.watermarks_snapshot().items()
            },
        }
        sent = await self.transport.send_epidemic("mgmt_audit_delta", payload)
        if sent:
            self._last_delta_at = time.time()
        else:
            # No connected peers yet — re-queue so the next reconcile cycle retries.
            for rec in reversed(batch):
                self._outbound.appendleft(rec)

    async def _exchange_digests(self) -> None:
        digest = self.build_digest()
        sent = await self.transport.send_epidemic("mgmt_audit_digest", digest)
        if sent:
            self._last_digest_at = time.time()

@dataclass(slots=True)
class InProcessSharedAuditTransport:
    """Deterministic inject transport for unit tests (drop/reorder capable)."""

    peers: dict[str, SharedAuditReplicator] = field(default_factory=dict)
    drop_types: set[str] = field(default_factory=set)
    reorder_buffer: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    hold_reorder: bool = False
    sent: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)

    def register(self, rep: SharedAuditReplicator) -> None:
        self.peers[rep.node_id] = rep

    async def send_epidemic(self, message_type: str, payload: dict[str, Any]) -> int:
        if message_type in self.drop_types:
            return 0
        sender = str(payload.get("from_node") or "")
        n = 0
        targets = [p for p in self.peers if p != sender]
        for peer_id in targets:
            await self._deliver(peer_id, message_type, payload)
            n += 1
        return n

    async def send_unicast(
        self, peer_id: str, message_type: str, payload: dict[str, Any]
    ) -> bool:
        if message_type in self.drop_types:
            return False
        return await self._deliver(peer_id, message_type, payload)

    async def _deliver(
        self, peer_id: str, message_type: str, payload: dict[str, Any]
    ) -> bool:
        self.sent.append((peer_id, message_type, payload))
        if self.hold_reorder:
            self.reorder_buffer.append((peer_id, message_type, payload))
            return True
        rep = self.peers.get(peer_id)
        if rep is None:
            return False
        await rep._handle_async(message_type, payload)
        return True

    async def flush_reorder(self) -> None:
        buf = list(self.reorder_buffer)
        self.reorder_buffer.clear()
        # reverse order = reorder stress
        for peer_id, mt, payload in reversed(buf):
            rep = self.peers.get(peer_id)
            if rep is not None:
                await rep._handle_async(mt, payload)

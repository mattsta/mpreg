"""Transport abstraction for fabric cache federation."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from mpreg.core.serialization import JsonSerializer
from mpreg.datastructures.type_aliases import JsonDict, NodeId

from .cache_selection import CachePeerSelector
from .message import DeliveryGuarantee, MessageHeaders, MessageType, UnifiedMessage

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mpreg.core.cache_models import GlobalCacheEntry, GlobalCacheKey
    from mpreg.core.connection import Connection
    from mpreg.fabric.index import RoutingIndex
    from mpreg.server import MPREGServer

    from .cache_federation import CacheDigest, CacheOperationMessage


class CacheMessageKind(StrEnum):
    OPERATION = "cache_operation"
    DIGEST_REQUEST = "cache_digest_request"
    DIGEST_RESPONSE = "cache_digest_response"
    ENTRY_REQUEST = "cache_entry_request"
    ENTRY_RESPONSE = "cache_entry_response"
    # ConsistencyLevel.STRONG majority-commit barrier (RR)
    STRONG_PREPARE = "cache_strong_prepare"
    STRONG_PREPARE_ACK = "cache_strong_prepare_ack"
    STRONG_COMMIT = "cache_strong_commit"
    STRONG_COMMIT_ACK = "cache_strong_commit_ack"
    STRONG_ABORT = "cache_strong_abort"
    STRONG_ABORT_ACK = "cache_strong_abort_ack"


class CacheReceiver(Protocol):
    node_id: NodeId

    async def handle_cache_message(self, message: CacheOperationMessage) -> bool: ...

    def create_cache_digest(self) -> CacheDigest: ...

    def get_cache_entry(self, key: GlobalCacheKey) -> GlobalCacheEntry | None: ...


class CacheTransport(Protocol):
    def register(self, receiver: CacheReceiver) -> None: ...

    def unregister(self, node_id: NodeId) -> None: ...

    def peer_ids(self, *, exclude: NodeId | None = None) -> tuple[NodeId, ...]: ...

    def select_peers(
        self,
        message: CacheOperationMessage,
        *,
        exclude: NodeId | None = None,
        max_peers: int | None = None,
    ) -> tuple[NodeId, ...]: ...

    async def send_operation(
        self, peer_id: NodeId, message: CacheOperationMessage
    ) -> bool: ...

    async def fetch_digest(self, peer_id: NodeId) -> CacheDigest | None: ...

    async def fetch_entry(
        self, peer_id: NodeId, key: GlobalCacheKey
    ) -> GlobalCacheEntry | None: ...


@dataclass(slots=True)
class InProcessCacheTransport:
    """In-process transport for cache federation (test/local use)."""

    _peers: dict[NodeId, CacheReceiver] = field(default_factory=dict)

    def register(self, receiver: CacheReceiver) -> None:
        self._peers[receiver.node_id] = receiver

    def unregister(self, node_id: NodeId) -> None:
        self._peers.pop(node_id, None)

    def peer_ids(self, *, exclude: NodeId | None = None) -> tuple[NodeId, ...]:
        peers = [peer_id for peer_id in self._peers if peer_id != exclude]
        return tuple(sorted(peers))

    def select_peers(
        self,
        message: CacheOperationMessage,
        *,
        exclude: NodeId | None = None,
        max_peers: int | None = None,
    ) -> tuple[NodeId, ...]:
        peers = self.peer_ids(exclude=exclude)
        if max_peers is None:
            return peers
        return peers[:max_peers]

    async def send_operation(
        self, peer_id: NodeId, message: CacheOperationMessage
    ) -> bool:
        receiver = self._peers.get(peer_id)
        if receiver is None:
            return False
        return await receiver.handle_cache_message(message)

    async def fetch_digest(self, peer_id: NodeId) -> CacheDigest | None:
        receiver = self._peers.get(peer_id)
        if receiver is None:
            return None
        return receiver.create_cache_digest()

    async def fetch_entry(
        self, peer_id: NodeId, key: GlobalCacheKey
    ) -> GlobalCacheEntry | None:
        receiver = self._peers.get(peer_id)
        if receiver is None:
            return None
        return receiver.get_cache_entry(key)


@dataclass(slots=True)
class ServerCacheTransport:
    """Server-backed transport for fabric cache federation."""

    server: MPREGServer
    serializer: JsonSerializer
    routing_index: RoutingIndex | None = None
    allowed_clusters: frozenset[str] | None = None
    peer_selector: CachePeerSelector | None = None
    response_timeout: float = 5.0
    _receiver: CacheReceiver | None = None
    _pending_digests: dict[str, asyncio.Future[CacheDigest | None]] = field(
        default_factory=dict
    )
    _pending_entries: dict[str, asyncio.Future[GlobalCacheEntry | None]] = field(
        default_factory=dict
    )
    _pending_strong: dict[str, asyncio.Future[dict]] = field(default_factory=dict)
    # Optional STRONG peer handler (prepare/commit/abort local apply).
    strong_handler: Any = None

    def register(self, receiver: CacheReceiver) -> None:
        self._receiver = receiver

    def unregister(self, node_id: NodeId) -> None:
        if self._receiver and self._receiver.node_id == node_id:
            self._receiver = None

    def peer_ids(self, *, exclude: NodeId | None = None) -> tuple[NodeId, ...]:
        peers: set[NodeId] = set()
        if self.routing_index is not None:
            from mpreg.fabric.catalog import CacheRole
            from mpreg.fabric.index import CacheQuery

            if self.allowed_clusters is None:
                query = CacheQuery(role=CacheRole.SYNC, cluster_id=None)
                entries = self.routing_index.find_cache_roles(query)
            else:
                entries = []
                for cluster_id in sorted(self.allowed_clusters):
                    query = CacheQuery(role=CacheRole.SYNC, cluster_id=cluster_id)
                    entries.extend(self.routing_index.find_cache_roles(query))
            for entry in entries:
                peers.add(entry.node_id)
        else:
            peers.update(self._active_connections().keys())

        if exclude:
            peers.discard(exclude)
        return tuple(sorted(peers))

    def select_peers(
        self,
        message: CacheOperationMessage,
        *,
        exclude: NodeId | None = None,
        max_peers: int | None = None,
    ) -> tuple[NodeId, ...]:
        peers = self.peer_ids(exclude=exclude)
        if not peers or self.peer_selector is None or self.routing_index is None:
            if max_peers is None:
                return peers
            return peers[:max_peers]

        from mpreg.fabric.index import CacheProfileQuery

        if self.allowed_clusters is None:
            profiles = self.routing_index.find_cache_profiles(CacheProfileQuery())
        else:
            profiles = []
            for cluster_id in sorted(self.allowed_clusters):
                profiles.extend(
                    self.routing_index.find_cache_profiles(
                        CacheProfileQuery(cluster_id=cluster_id)
                    )
                )
        peer_set = set(peers)
        candidate_profiles = tuple(
            profile for profile in profiles if profile.node_id in peer_set
        )
        if not candidate_profiles:
            if max_peers is None:
                return peers
            return peers[:max_peers]

        return self.peer_selector.select_peers(
            candidate_profiles,
            metadata=message.metadata,
            max_peers=max_peers,
        )

    async def send_operation(
        self, peer_id: NodeId, message: CacheOperationMessage
    ) -> bool:
        payload = {
            "kind": CacheMessageKind.OPERATION.value,
            "operation": message.to_dict(),
            "target_node": peer_id,
        }
        return await self._send_payload(
            peer_id,
            payload,
            correlation_id=message.operation_id,
            topic="mpreg.cache.sync.operation",
        )

    async def fetch_digest(self, peer_id: NodeId) -> CacheDigest | None:
        request_id = str(uuid.uuid4())
        future = self._create_future(self._pending_digests, request_id)
        if future is None:
            return None

        payload = {
            "kind": CacheMessageKind.DIGEST_REQUEST.value,
            "request_id": request_id,
            "reply_to": self.server.cluster.local_url,
            "target_node": peer_id,
        }
        sent = await self._send_payload(
            peer_id,
            payload,
            correlation_id=request_id,
            topic="mpreg.cache.sync.digest.request",
        )
        if not sent:
            future = self._pending_digests.pop(request_id, None)
            if future and not future.done():
                future.set_result(None)
            return None
        return await self._await_future(self._pending_digests, request_id, future)

    async def fetch_entry(
        self, peer_id: NodeId, key: GlobalCacheKey
    ) -> GlobalCacheEntry | None:
        from mpreg.core.cache_protocol import CacheKeyMessage

        request_id = str(uuid.uuid4())
        future = self._create_future(self._pending_entries, request_id)
        if future is None:
            return None

        payload = {
            "kind": CacheMessageKind.ENTRY_REQUEST.value,
            "request_id": request_id,
            "reply_to": self.server.cluster.local_url,
            "target_node": peer_id,
            "key": CacheKeyMessage.from_global_cache_key(key).to_dict(),
        }
        sent = await self._send_payload(
            peer_id,
            payload,
            correlation_id=request_id,
            topic="mpreg.cache.sync.entry.request",
        )
        if not sent:
            future = self._pending_entries.pop(request_id, None)
            if future and not future.done():
                future.set_result(None)
            return None
        return await self._await_future(self._pending_entries, request_id, future)

    async def handle_message(
        self,
        message: UnifiedMessage,
        *,
        source_peer_url: str | None = None,
    ) -> None:
        payload = message.payload if isinstance(message.payload, dict) else {}
        kind = payload.get("kind")
        if not isinstance(kind, str):
            return

        if kind == CacheMessageKind.OPERATION.value:
            target_node = payload.get("target_node")
            if (
                isinstance(target_node, str)
                and target_node != self.server.cluster.local_url
            ):
                await self._forward_message(
                    message, target_node=target_node, source_peer_url=source_peer_url
                )
                return
            if not self._receiver:
                return
            from mpreg.fabric.cache_federation import CacheOperationMessage

            operation_payload = payload.get("operation")
            if not isinstance(operation_payload, dict):
                return
            operation = CacheOperationMessage.from_dict(operation_payload)
            await self._receiver.handle_cache_message(operation)
            return

        if kind in (
            CacheMessageKind.DIGEST_REQUEST.value,
            CacheMessageKind.ENTRY_REQUEST.value,
        ):
            target_node = payload.get("target_node")
            if (
                isinstance(target_node, str)
                and target_node != self.server.cluster.local_url
            ):
                await self._forward_message(
                    message, target_node=target_node, source_peer_url=source_peer_url
                )
                return

        if kind in (
            CacheMessageKind.DIGEST_RESPONSE.value,
            CacheMessageKind.ENTRY_RESPONSE.value,
        ):
            reply_to = payload.get("reply_to")
            if isinstance(reply_to, str) and reply_to != self.server.cluster.local_url:
                await self._forward_message(
                    message, target_node=reply_to, source_peer_url=source_peer_url
                )
                return

        if kind == CacheMessageKind.DIGEST_REQUEST.value:
            digest_payload = None
            if self._receiver:
                digest_payload = self._receiver.create_cache_digest().to_dict()
            response = {
                "kind": CacheMessageKind.DIGEST_RESPONSE.value,
                "request_id": payload.get("request_id", ""),
                "reply_to": payload.get("reply_to", ""),
                "responder": self.server.cluster.local_url,
                "digest": digest_payload,
            }
            reply_to = payload.get("reply_to", "")
            if reply_to:
                await self._send_payload(
                    reply_to,
                    response,
                    correlation_id=str(payload.get("request_id", "")),
                    topic="mpreg.cache.sync.digest.response",
                )
            return

        if kind == CacheMessageKind.DIGEST_RESPONSE.value:
            request_id = str(payload.get("request_id", ""))
            future = self._pending_digests.pop(request_id, None)
            if future and not future.done():
                digest_payload = payload.get("digest")
                if isinstance(digest_payload, dict):
                    from mpreg.fabric.cache_federation import CacheDigest

                    future.set_result(CacheDigest.from_dict(digest_payload))
                else:
                    future.set_result(None)
            return

        if kind == CacheMessageKind.ENTRY_REQUEST.value:
            entry_payload = None
            if self._receiver:
                key_payload = payload.get("key")
                if isinstance(key_payload, dict):
                    from mpreg.core.cache_protocol import (
                        CacheEntryMessage,
                        CacheKeyMessage,
                    )

                    key = CacheKeyMessage.from_dict(key_payload).to_global_cache_key()
                    entry = self._receiver.get_cache_entry(key)
                    if entry is not None:
                        entry_payload = CacheEntryMessage.from_global_cache_entry(
                            entry
                        ).to_dict()
            response = {
                "kind": CacheMessageKind.ENTRY_RESPONSE.value,
                "request_id": payload.get("request_id", ""),
                "reply_to": payload.get("reply_to", ""),
                "responder": self.server.cluster.local_url,
                "entry": entry_payload,
            }
            reply_to = payload.get("reply_to", "")
            if reply_to:
                await self._send_payload(
                    reply_to,
                    response,
                    correlation_id=str(payload.get("request_id", "")),
                    topic="mpreg.cache.sync.entry.response",
                )
            return

        if kind == CacheMessageKind.ENTRY_RESPONSE.value:
            request_id = str(payload.get("request_id", ""))
            future = self._pending_entries.pop(request_id, None)
            if future and not future.done():
                entry_payload = payload.get("entry")
                if isinstance(entry_payload, dict):
                    from mpreg.core.cache_protocol import CacheEntryMessage

                    future.set_result(
                        CacheEntryMessage.from_dict(
                            entry_payload
                        ).to_global_cache_entry()
                    )
                else:
                    future.set_result(None)
            return

        # --- STRONG barrier RR ---
        if kind in (
            CacheMessageKind.STRONG_PREPARE.value,
            CacheMessageKind.STRONG_COMMIT.value,
            CacheMessageKind.STRONG_ABORT.value,
        ):
            target_node = payload.get("target_node")
            if (
                isinstance(target_node, str)
                and target_node != self.server.cluster.local_url
            ):
                await self._forward_message(
                    message, target_node=target_node, source_peer_url=source_peer_url
                )
                return
            await self._handle_strong_request(kind, payload)
            return

        if kind in (
            CacheMessageKind.STRONG_PREPARE_ACK.value,
            CacheMessageKind.STRONG_COMMIT_ACK.value,
            CacheMessageKind.STRONG_ABORT_ACK.value,
        ):
            reply_to = payload.get("reply_to")
            if isinstance(reply_to, str) and reply_to != self.server.cluster.local_url:
                await self._forward_message(
                    message, target_node=reply_to, source_peer_url=source_peer_url
                )
                return
            request_id = str(payload.get("request_id", ""))
            future = self._pending_strong.pop(request_id, None)
            if future and not future.done():
                future.set_result(payload if isinstance(payload, dict) else {})
            return

    async def _handle_strong_request(self, kind: str, payload: dict) -> None:
        handler = self.strong_handler
        reply_to = str(payload.get("reply_to") or "")
        request_id = str(payload.get("request_id") or "")
        if handler is None:
            resp: dict = {
                "request_id": request_id,
                "reply_to": reply_to,
                "ok": False,
                "reason": "strong_handler_unbound",
                "node_id": self.server.cluster.local_url,
            }
        elif kind == CacheMessageKind.STRONG_PREPARE.value:
            resp = await handler.handle_prepare(payload)
            resp["kind"] = CacheMessageKind.STRONG_PREPARE_ACK.value
        elif kind == CacheMessageKind.STRONG_COMMIT.value:
            resp = await handler.handle_commit(payload)
            resp["kind"] = CacheMessageKind.STRONG_COMMIT_ACK.value
        else:
            resp = await handler.handle_abort(payload)
            resp["kind"] = CacheMessageKind.STRONG_ABORT_ACK.value
        resp.setdefault("request_id", request_id)
        resp.setdefault("reply_to", reply_to)
        if kind == CacheMessageKind.STRONG_PREPARE.value:
            resp.setdefault("kind", CacheMessageKind.STRONG_PREPARE_ACK.value)
        elif kind == CacheMessageKind.STRONG_COMMIT.value:
            resp.setdefault("kind", CacheMessageKind.STRONG_COMMIT_ACK.value)
        else:
            resp.setdefault("kind", CacheMessageKind.STRONG_ABORT_ACK.value)
        if reply_to:
            topic = {
                CacheMessageKind.STRONG_PREPARE.value: "mpreg.cache.strong.prepare.ack",
                CacheMessageKind.STRONG_COMMIT.value: "mpreg.cache.strong.commit.ack",
                CacheMessageKind.STRONG_ABORT.value: "mpreg.cache.strong.abort.ack",
            }.get(kind, "mpreg.cache.strong.ack")
            await self._send_payload(
                reply_to,
                resp,
                correlation_id=request_id,
                topic=topic,
            )

    async def strong_prepare(
        self,
        peer_id: NodeId,
        *,
        key: GlobalCacheKey,
        value: Any,
        metadata: Any,
        strong_version: Any,
        replica_set: tuple[str, ...],
        quorum: int,
        cluster_id: str,
        timeout: float,
    ) -> Any:
        from mpreg.core.cache_strong import PrepareAck
        from mpreg.core.cache_strong_handlers import (
            key_to_payload,
            prepare_ack_from_dict,
        )

        request_id = str(uuid.uuid4())
        future = self._create_future(self._pending_strong, request_id)
        if future is None:
            return PrepareAck(str(peer_id), False, "no_event_loop")
        meta_dict: dict = {}
        if metadata is not None:
            meta_dict = {
                "access_patterns": dict(
                    getattr(metadata, "access_patterns", None) or {}
                ),
                "created_by": str(getattr(metadata, "created_by", "") or ""),
                "ttl_seconds": getattr(metadata, "ttl_seconds", None),
            }
        sv = (
            strong_version.to_dict()
            if hasattr(strong_version, "to_dict")
            else dict(strong_version or {})
        )
        payload = {
            "kind": CacheMessageKind.STRONG_PREPARE.value,
            "request_id": request_id,
            "reply_to": self.server.cluster.local_url,
            "target_node": peer_id,
            "cluster_id": cluster_id,
            "key": key_to_payload(key),
            "value": value,
            "metadata": meta_dict,
            "strong_version": sv,
            "replica_set": list(replica_set),
            "quorum": quorum,
            "pending_ttl_s": timeout,
        }
        sent = await self._send_payload(
            peer_id,
            payload,
            correlation_id=request_id,
            topic="mpreg.cache.strong.prepare",
        )
        if not sent:
            fut = self._pending_strong.pop(request_id, None)
            if fut and not fut.done():
                fut.set_result(
                    {"ok": False, "reason": "send_failed", "node_id": peer_id}
                )
            return PrepareAck(str(peer_id), False, "send_failed")
        old_timeout = self.response_timeout
        self.response_timeout = timeout
        try:
            raw = await self._await_future(self._pending_strong, request_id, future)
        finally:
            self.response_timeout = old_timeout
        if not isinstance(raw, dict):
            return PrepareAck(str(peer_id), False, "timeout")
        return prepare_ack_from_dict(raw)

    async def strong_commit(
        self,
        peer_id: NodeId,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: Any,
        cluster_id: str,
        timeout: float,
    ) -> Any:
        from mpreg.core.cache_strong import CommitAck
        from mpreg.core.cache_strong_handlers import (
            commit_ack_from_dict,
            key_to_payload,
        )

        request_id = str(uuid.uuid4())
        future = self._create_future(self._pending_strong, request_id)
        if future is None:
            return CommitAck(str(peer_id), False, reason="no_event_loop")
        sv = (
            strong_version.to_dict()
            if hasattr(strong_version, "to_dict")
            else dict(strong_version or {})
        )
        payload = {
            "kind": CacheMessageKind.STRONG_COMMIT.value,
            "request_id": request_id,
            "reply_to": self.server.cluster.local_url,
            "target_node": peer_id,
            "cluster_id": cluster_id,
            "op_id": op_id,
            "key": key_to_payload(key),
            "strong_version": sv,
        }
        sent = await self._send_payload(
            peer_id,
            payload,
            correlation_id=request_id,
            topic="mpreg.cache.strong.commit",
        )
        if not sent:
            fut = self._pending_strong.pop(request_id, None)
            if fut and not fut.done():
                fut.set_result(
                    {"ok": False, "reason": "send_failed", "node_id": peer_id}
                )
            return CommitAck(str(peer_id), False, reason="send_failed")
        old_timeout = self.response_timeout
        self.response_timeout = timeout
        try:
            raw = await self._await_future(self._pending_strong, request_id, future)
        finally:
            self.response_timeout = old_timeout
        if not isinstance(raw, dict):
            return CommitAck(str(peer_id), False, reason="timeout")
        return commit_ack_from_dict(raw)

    async def strong_abort(
        self,
        peer_id: NodeId,
        *,
        op_id: str,
        key: GlobalCacheKey,
        strong_version: Any,
        cluster_id: str,
        timeout: float,
    ) -> bool:
        from mpreg.core.cache_strong_handlers import key_to_payload

        request_id = str(uuid.uuid4())
        future = self._create_future(self._pending_strong, request_id)
        if future is None:
            return False
        sv = (
            strong_version.to_dict()
            if hasattr(strong_version, "to_dict")
            else dict(strong_version or {})
        )
        payload = {
            "kind": CacheMessageKind.STRONG_ABORT.value,
            "request_id": request_id,
            "reply_to": self.server.cluster.local_url,
            "target_node": peer_id,
            "cluster_id": cluster_id,
            "op_id": op_id,
            "key": key_to_payload(key),
            "strong_version": sv,
        }
        sent = await self._send_payload(
            peer_id,
            payload,
            correlation_id=request_id,
            topic="mpreg.cache.strong.abort",
        )
        if not sent:
            fut = self._pending_strong.pop(request_id, None)
            if fut and not fut.done():
                fut.set_result({"ok": False})
            return False
        old_timeout = self.response_timeout
        self.response_timeout = timeout
        try:
            raw = await self._await_future(self._pending_strong, request_id, future)
        finally:
            self.response_timeout = old_timeout
        return bool(isinstance(raw, dict) and raw.get("ok"))

    def _active_connections(self) -> dict[str, Connection]:
        return {
            url: conn
            for url, conn in self.server._get_all_peer_connections().items()
            if conn.is_connected
        }

    async def _send_payload(
        self,
        target_node: NodeId,
        payload: JsonDict,
        *,
        correlation_id: str,
        topic: str,
        source_peer_url: str | None = None,
    ) -> bool:
        headers = MessageHeaders(
            correlation_id=correlation_id,
            source_cluster=self.server.settings.cluster_id,
            routing_path=(self.server.cluster.local_url,),
            federation_path=(self.server.settings.cluster_id,),
            hop_budget=self.server.settings.fabric_routing_max_hops,
        )
        resolved = self._resolve_target_peer(target_node, headers=headers)
        if resolved is None:
            return False
        message = UnifiedMessage(
            message_id=correlation_id,
            topic=topic,
            message_type=MessageType.CACHE,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=payload,
            headers=headers,
            timestamp=time.time(),
        )
        await self.server._send_fabric_message(
            message, target_nodes=(resolved,), source_peer_url=source_peer_url
        )
        return True

    async def _forward_message(
        self,
        message: UnifiedMessage,
        *,
        target_node: NodeId,
        source_peer_url: str | None = None,
    ) -> None:
        from mpreg.core.errors import MpregError, MpregErrorCode

        try:
            next_headers = self.server._next_fabric_headers(
                message.message_id,
                message.headers,
                max_hops=self.server.settings.fabric_routing_max_hops,
            )
        except MpregError as exc:
            if exc.code in (
                int(MpregErrorCode.HOP_BUDGET_EXCEEDED),
                int(MpregErrorCode.ROUTE_LOOP),
            ):
                return
            raise
        resolved = self._resolve_target_peer(target_node, headers=next_headers)
        if resolved is None:
            return
        forwarded = UnifiedMessage(
            message_id=message.message_id,
            topic=message.topic,
            message_type=message.message_type,
            delivery=message.delivery,
            payload=message.payload,
            headers=next_headers,
            timestamp=message.timestamp,
        )
        await self.server._send_fabric_message(
            forwarded,
            target_nodes=(resolved,),
            source_peer_url=source_peer_url,
        )

    def _resolve_target_peer(
        self, target_node: NodeId, *, headers: MessageHeaders
    ) -> NodeId | None:
        connections = self._active_connections()
        if target_node in connections:
            return target_node
        target_cluster = self.server.cluster.cluster_id_for_node_url(target_node)
        if target_cluster is None:
            return None
        if (
            self.allowed_clusters is not None
            and target_cluster not in self.allowed_clusters
        ):
            return None
        next_hop = self.server._fabric_next_hop_for_cluster(
            target_cluster, headers=headers
        )
        return next_hop

    def _create_future(
        self,
        pending: dict[str, asyncio.Future],
        request_id: str,
    ) -> asyncio.Future | None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None
        future: asyncio.Future = loop.create_future()
        pending[request_id] = future
        return future

    async def _await_future(
        self,
        pending: dict[str, asyncio.Future],
        request_id: str,
        future: asyncio.Future,
    ):
        try:
            return await asyncio.wait_for(future, timeout=self.response_timeout)
        except TimeoutError:
            if not future.done():
                future.cancel()
            return None
        finally:
            pending.pop(request_id, None)

"""Transport abstraction for fabric cache federation."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

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
        next_headers = self.server._next_fabric_headers(
            message.message_id,
            message.headers,
            max_hops=self.server.settings.fabric_routing_max_hops,
        )
        if next_headers is None:
            return
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

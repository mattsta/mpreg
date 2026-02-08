"""Fabric-backed transport for ProductionRaft RPCs."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

from loguru import logger

from mpreg.datastructures.raft_codec import (
    deserialize_append_entries,
    deserialize_append_entries_response,
    deserialize_install_snapshot,
    deserialize_install_snapshot_response,
    deserialize_request_vote,
    deserialize_request_vote_response,
    serialize_append_entries,
    serialize_append_entries_response,
    serialize_install_snapshot,
    serialize_install_snapshot_response,
    serialize_request_vote,
    serialize_request_vote_response,
)
from mpreg.datastructures.type_aliases import (
    ClusterId,
    HopCount,
    JsonDict,
    NodeId,
    RequestId,
)
from mpreg.fabric.message import (
    DeliveryGuarantee,
    MessageHeaders,
    MessageType,
    UnifiedMessage,
)

from ..datastructures.production_raft import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from .raft_messages import (
    RAFT_RPC_REQUEST_KIND,
    RAFT_RPC_RESPONSE_KIND,
    RAFT_RPC_TOPIC,
    FabricRaftRpcRequest,
    FabricRaftRpcResponse,
    RaftRpcKind,
)

raft_log = logger


class RaftNodeProtocol(Protocol):
    node_id: NodeId

    async def handle_request_vote(
        self, request: RequestVoteRequest
    ) -> RequestVoteResponse: ...

    async def handle_append_entries(
        self, request: AppendEntriesRequest
    ) -> AppendEntriesResponse: ...

    async def handle_install_snapshot(
        self, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse: ...


@dataclass(frozen=True, slots=True)
class FabricRaftTransportConfig:
    node_id: NodeId
    cluster_id: ClusterId
    request_timeout_seconds: float = 1.0
    max_hops: HopCount | None = None


@dataclass(frozen=True, slots=True)
class FabricRaftTransportHooks:
    send_direct: Callable[[NodeId, UnifiedMessage], Awaitable[bool]]
    send_to_cluster: Callable[
        [ClusterId, UnifiedMessage, NodeId | None], Awaitable[bool]
    ]
    resolve_cluster: Callable[[NodeId], ClusterId | None]


@dataclass(slots=True)
class FabricRaftTransport:
    config: FabricRaftTransportConfig
    hooks: FabricRaftTransportHooks
    _nodes: dict[NodeId, RaftNodeProtocol] = field(default_factory=dict)
    _pending: dict[RequestId, asyncio.Future[FabricRaftRpcResponse]] = field(
        default_factory=dict
    )
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def register_node(self, node: RaftNodeProtocol) -> None:
        if node.node_id != self.config.node_id:
            raise ValueError(
                f"Raft node id {node.node_id} does not match transport node id {self.config.node_id}"
            )
        self._nodes[node.node_id] = node

    async def send_request_vote(
        self, target: NodeId, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        response = await self._send_rpc(
            target=target,
            rpc_kind=RaftRpcKind.REQUEST_VOTE,
            payload=serialize_request_vote(request),
        )
        if response is None:
            return None
        return deserialize_request_vote_response(response.payload)

    async def send_append_entries(
        self, target: NodeId, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        response = await self._send_rpc(
            target=target,
            rpc_kind=RaftRpcKind.APPEND_ENTRIES,
            payload=serialize_append_entries(request),
        )
        if response is None:
            return None
        return deserialize_append_entries_response(response.payload)

    async def send_install_snapshot(
        self, target: NodeId, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse | None:
        response = await self._send_rpc(
            target=target,
            rpc_kind=RaftRpcKind.INSTALL_SNAPSHOT,
            payload=serialize_install_snapshot(request),
        )
        if response is None:
            return None
        return deserialize_install_snapshot_response(response.payload)

    async def handle_message(
        self, message: UnifiedMessage, *, source_peer_url: NodeId | None
    ) -> bool:
        if message.message_type is not MessageType.CONTROL:
            return False
        if not isinstance(message.payload, dict):
            return False
        kind = message.payload.get("kind")
        if kind == RAFT_RPC_REQUEST_KIND:
            try:
                request = FabricRaftRpcRequest.from_dict(message.payload)
            except Exception as exc:
                raft_log.warning("Invalid raft request payload: {}", exc)
                return False
            await self._handle_request(request, message, source_peer_url)
            return True
        if kind == RAFT_RPC_RESPONSE_KIND:
            try:
                response = FabricRaftRpcResponse.from_dict(message.payload)
            except Exception as exc:
                raft_log.warning("Invalid raft response payload: {}", exc)
                return False
            await self._handle_response(response, message, source_peer_url)
            return True
        return False

    async def _send_rpc(
        self, *, target: NodeId, rpc_kind: RaftRpcKind, payload: JsonDict
    ) -> FabricRaftRpcResponse | None:
        request_id = self._create_request_id()
        request = FabricRaftRpcRequest(
            request_id=request_id,
            rpc_kind=rpc_kind,
            sender_id=self.config.node_id,
            target_id=target,
            payload=dict(payload),
        )
        headers = self._next_headers(
            correlation_id=request_id,
            headers=None,
            target_cluster=self._resolve_target_cluster(target),
        )
        if headers is None:
            return None

        message = UnifiedMessage(
            message_id=request_id,
            topic=RAFT_RPC_TOPIC,
            message_type=MessageType.CONTROL,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=request.to_dict(),
            headers=headers,
        )

        async with self._lock:
            future: asyncio.Future[FabricRaftRpcResponse] = (
                asyncio.get_running_loop().create_future()
            )
            self._pending[request_id] = future

        sent = await self._send_message(target, message, source_peer_url=None)
        if not sent:
            async with self._lock:
                pending = self._pending.pop(request_id, None)
                if pending and not pending.done():
                    pending.cancel()
            return None

        try:
            response = await asyncio.wait_for(
                future, timeout=self.config.request_timeout_seconds
            )
        except TimeoutError:
            async with self._lock:
                pending = self._pending.pop(request_id, None)
                if pending and not pending.done():
                    pending.cancel()
            return None
        return response

    async def _handle_request(
        self,
        request: FabricRaftRpcRequest,
        message: UnifiedMessage,
        source_peer_url: NodeId | None,
    ) -> None:
        if request.target_id != self.config.node_id:
            await self._forward_message(
                message,
                target_id=request.target_id,
                source_peer_url=source_peer_url,
            )
            return

        node = self._nodes.get(request.target_id)
        if not node:
            raft_log.warning(
                "Raft request for missing node: target_id={}",
                request.target_id,
            )
            return

        response_payload: JsonDict
        if request.rpc_kind is RaftRpcKind.REQUEST_VOTE:
            vote_response = await node.handle_request_vote(
                deserialize_request_vote(request.payload)
            )
            response_payload = serialize_request_vote_response(vote_response)
        elif request.rpc_kind is RaftRpcKind.APPEND_ENTRIES:
            append_response = await node.handle_append_entries(
                deserialize_append_entries(request.payload)
            )
            response_payload = serialize_append_entries_response(append_response)
        elif request.rpc_kind is RaftRpcKind.INSTALL_SNAPSHOT:
            snapshot_response = await node.handle_install_snapshot(
                deserialize_install_snapshot(request.payload)
            )
            response_payload = serialize_install_snapshot_response(snapshot_response)
        else:
            return

        response = FabricRaftRpcResponse(
            request_id=request.request_id,
            rpc_kind=request.rpc_kind,
            sender_id=self.config.node_id,
            target_id=request.sender_id,
            payload=response_payload,
        )
        headers = self._next_headers(
            correlation_id=request.request_id,
            headers=None,
            target_cluster=message.headers.source_cluster,
        )
        if headers is None:
            return
        response_message = UnifiedMessage(
            message_id=request.request_id,
            topic=RAFT_RPC_TOPIC,
            message_type=MessageType.CONTROL,
            delivery=DeliveryGuarantee.AT_LEAST_ONCE,
            payload=response.to_dict(),
            headers=headers,
        )
        await self._send_message(
            request.sender_id, response_message, source_peer_url=source_peer_url
        )

    async def _handle_response(
        self,
        response: FabricRaftRpcResponse,
        message: UnifiedMessage,
        source_peer_url: NodeId | None,
    ) -> None:
        if response.target_id != self.config.node_id:
            await self._forward_message(
                message,
                target_id=response.target_id,
                source_peer_url=source_peer_url,
            )
            return
        async with self._lock:
            future = self._pending.pop(response.request_id, None)
        if future and not future.done():
            future.set_result(response)

    async def _forward_message(
        self,
        message: UnifiedMessage,
        *,
        target_id: NodeId,
        source_peer_url: NodeId | None,
    ) -> None:
        target_cluster = self._resolve_target_cluster(target_id)
        headers = self._next_headers(
            correlation_id=message.headers.correlation_id,
            headers=message.headers,
            target_cluster=target_cluster,
        )
        if headers is None:
            return
        forwarded = UnifiedMessage(
            message_id=message.message_id,
            topic=message.topic,
            message_type=message.message_type,
            delivery=message.delivery,
            payload=message.payload,
            headers=headers,
            timestamp=message.timestamp,
        )
        await self._send_message(target_id, forwarded, source_peer_url=source_peer_url)

    async def _send_message(
        self,
        target_id: NodeId,
        message: UnifiedMessage,
        *,
        source_peer_url: NodeId | None,
    ) -> bool:
        target_cluster = self._resolve_target_cluster(target_id)
        if target_cluster == self.config.cluster_id:
            if await self.hooks.send_direct(target_id, message):
                return True
        return await self.hooks.send_to_cluster(
            target_cluster, message, source_peer_url
        )

    def _resolve_target_cluster(self, target_id: NodeId) -> ClusterId:
        return self.hooks.resolve_cluster(target_id) or self.config.cluster_id

    def _next_headers(
        self,
        correlation_id: RequestId,
        headers: MessageHeaders | None,
        *,
        target_cluster: ClusterId | None,
    ) -> MessageHeaders | None:
        if headers is None:
            return MessageHeaders(
                correlation_id=correlation_id,
                source_cluster=self.config.cluster_id,
                target_cluster=target_cluster,
                routing_path=(self.config.node_id,),
                federation_path=(self.config.cluster_id,),
                hop_budget=self.config.max_hops,
            )
        if self.config.node_id in headers.routing_path:
            return None

        hop_budget = headers.hop_budget
        if hop_budget is None:
            hop_budget = self.config.max_hops
        elif self.config.max_hops is not None:
            hop_budget = min(hop_budget, self.config.max_hops)

        routing_path = headers.routing_path
        if not routing_path or routing_path[-1] != self.config.node_id:
            routing_path = (*routing_path, self.config.node_id)

        federation_path = headers.federation_path
        if not federation_path or federation_path[-1] != self.config.cluster_id:
            federation_path = (*federation_path, self.config.cluster_id)

        hop_count = max(0, len(routing_path) - 1)
        if hop_budget is not None and hop_count > hop_budget:
            return None

        return MessageHeaders(
            correlation_id=headers.correlation_id or correlation_id,
            source_cluster=headers.source_cluster or self.config.cluster_id,
            target_cluster=target_cluster or headers.target_cluster,
            routing_path=routing_path,
            federation_path=federation_path,
            hop_budget=hop_budget,
            priority=headers.priority,
            metadata=dict(headers.metadata),
        )

    @staticmethod
    def _create_request_id() -> RequestId:
        import uuid

        return f"raft_{uuid.uuid4().hex[:16]}"

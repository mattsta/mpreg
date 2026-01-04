"""Typed payloads for fabric Raft control messages."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from mpreg.datastructures.type_aliases import NodeId, RequestId

RAFT_RPC_REQUEST_KIND = "raft-rpc-request"
RAFT_RPC_RESPONSE_KIND = "raft-rpc-response"
RAFT_RPC_TOPIC = "mpreg.fabric.raft.rpc"


def _as_payload_dict(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key): val for key, val in value.items()}
    return {}


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


class RaftRpcKind(Enum):
    REQUEST_VOTE = "request_vote"
    APPEND_ENTRIES = "append_entries"
    INSTALL_SNAPSHOT = "install_snapshot"


@dataclass(frozen=True, slots=True)
class FabricRaftRpcRequest:
    request_id: RequestId
    rpc_kind: RaftRpcKind
    sender_id: NodeId
    target_id: NodeId
    payload: dict[str, Any]
    term: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": RAFT_RPC_REQUEST_KIND,
            "request_id": self.request_id,
            "rpc_kind": self.rpc_kind.value,
            "sender_id": self.sender_id,
            "target_id": self.target_id,
            "payload": dict(self.payload),
            "term": self.term,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> FabricRaftRpcRequest:
        return cls(
            request_id=str(payload.get("request_id", "")),
            rpc_kind=RaftRpcKind(str(payload.get("rpc_kind", ""))),
            sender_id=str(payload.get("sender_id", "")),
            target_id=str(payload.get("target_id", "")),
            payload=_as_payload_dict(payload.get("payload", {})),
            term=_as_optional_int(payload.get("term")),
        )


@dataclass(frozen=True, slots=True)
class FabricRaftRpcResponse:
    request_id: RequestId
    rpc_kind: RaftRpcKind
    sender_id: NodeId
    target_id: NodeId
    payload: dict[str, Any]
    term: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": RAFT_RPC_RESPONSE_KIND,
            "request_id": self.request_id,
            "rpc_kind": self.rpc_kind.value,
            "sender_id": self.sender_id,
            "target_id": self.target_id,
            "payload": dict(self.payload),
            "term": self.term,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> FabricRaftRpcResponse:
        return cls(
            request_id=str(payload.get("request_id", "")),
            rpc_kind=RaftRpcKind(str(payload.get("rpc_kind", ""))),
            sender_id=str(payload.get("sender_id", "")),
            target_id=str(payload.get("target_id", "")),
            payload=_as_payload_dict(payload.get("payload", {})),
            term=_as_optional_int(payload.get("term")),
        )
